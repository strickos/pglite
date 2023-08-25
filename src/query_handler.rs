use std::{sync::Arc, time::Duration};
use crossbeam_channel::RecvTimeoutError;
use async_trait::async_trait;
use futures::stream;
use futures_util::StreamExt;
use pgwire::{api::{query::{SimpleQueryHandler, ExtendedQueryHandler, StatementOrPortal}, results::{Response, DescribeResponse, DataRowEncoder, QueryResponse, FieldInfo}, ClientInfo, portal::Portal, store::MemPortalStore, stmt::NoopQueryParser, Type}, error::{PgWireResult, ErrorInfo, PgWireError}, messages::data::DataRow};
use rusqlite::types::Value;
pub use rusqlite::Column;

use crate::backend::{PgLiteDBMessage, BackendConnection, Record, Field, PgLiteDBResponse, PgLiteDBParam};

pub struct PgQueryProcessor {
    db:BackendConnection,
    portal_store: Arc<MemPortalStore<String>>,
    query_parser: Arc<NoopQueryParser>,
}

#[async_trait]
impl SimpleQueryHandler for PgQueryProcessor {
    async fn do_query<'a, 'b:'a, C>(&'b self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where C: ClientInfo + Unpin + Send + Sync {
        trace!("Processing Simple Query: {:?}", query);

        let (resp, waiter) = crossbeam_channel::bounded(1);
        let msg = PgLiteDBMessage::from_query(String::from(query), resp);
        let _ = self.db.sender.send(msg);
        let result = match waiter.recv_timeout(Duration::from_secs(10)) {   // todo make this configurable - currently hard coded to 10s
            Ok(msg) => msg,
            Err(RecvTimeoutError::Timeout) => {
                // Timeout waiting for response - return an error
                return PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Timeout waiting for response from the database".to_owned()).into())); 
            }, 
            Err(RecvTimeoutError::Disconnected) => {
                // Connection to the DB was lost for some reason, so exit...
                return PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Was disconnected from the database backend".to_owned()).into())); 
            }
        };

        self.translate_dbresponse_to_pgwire(result).map(|r| vec![r])
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgQueryProcessor {
    type Statement = String;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NoopQueryParser;
    
    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b:'a, C>(&'b self, _client: &mut C,portal: &'a Portal<Self::Statement>, _max_rows: usize) -> PgWireResult<Response<'a>>
    where C: ClientInfo + Unpin + Send + Sync {
        trace!("Processing Extended Query: {:?}", portal);
        let query = portal.statement().statement();
        let params = self.parse_params(portal);

        let (resp, waiter) = crossbeam_channel::bounded(1);
        let msg = PgLiteDBMessage::from_query_with_params(query.to_string(), params, resp);
        let _ = self.db.sender.send(msg);
        let result = match waiter.recv_timeout(Duration::from_secs(10)) {
            Ok(msg) => msg,
            Err(RecvTimeoutError::Timeout) => {
                // Timeout waiting for response - return an error
                return PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Timeout waiting for response from the database".to_owned()).into())); 
            }, 
            Err(RecvTimeoutError::Disconnected) => {
                // Connection to the DB was lost for some reason, so exit...
                return PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Was disconnected from the database backend".to_owned()).into())); 
            }
        };
        self.translate_dbresponse_to_pgwire(result)
    }

    async fn do_describe<C>(&self, _client: &mut C, target: StatementOrPortal<'_, Self::Statement>) -> PgWireResult<DescribeResponse>
    where C: ClientInfo + Unpin + Send + Sync {
        trace!("Processing Describe: {:?}", target);
        let query = match target {
            StatementOrPortal::Statement(statement) => statement.statement(),
            StatementOrPortal::Portal(portal) => portal.statement().statement()
        };

        let (resp, waiter) = crossbeam_channel::bounded(1);
        let msg = PgLiteDBMessage::from_describe(query.to_string(), resp);
        let _ = self.db.sender.send(msg);
        let result = match waiter.recv_timeout(Duration::from_secs(10)) { // todo make this configurable - currently hard coded to 10s
            Ok(msg) => msg,
            Err(RecvTimeoutError::Timeout) => {
                // Timeout waiting for response - return an error
                return PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Timeout waiting for response from the database".to_owned()).into()));
            }, 
            Err(RecvTimeoutError::Disconnected) => {
                // Connection to the DB was lost for some reason, so exit...
                return PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Was disconnected from the database backend".to_owned()).into())); 
            }
        };
        
        if let Some(schema) = result.result_schema {
            let fields = schema.iter().map(|field| field.into() ).collect();
            Ok(DescribeResponse::new(None, fields))
        } else {
            return PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Was unable to process the query schema".to_owned()).into())); 
        }
    }
}

impl PgQueryProcessor {
    pub fn create(db:BackendConnection, portal_store:Arc<MemPortalStore<String>>, query_parser:Arc<NoopQueryParser>) -> Self {
        Self { db, query_parser, portal_store, }
    }

    fn translate_dbresponse_to_pgwire(&self, result:PgLiteDBResponse) -> PgWireResult<Response<'_>> {
        if let Some(res) = result.result {
            let schema = Arc::new(self.translate_schema_to_pgwire(result.result_schema.unwrap()));
            let schema2 = schema.clone();
            match self.translate_records_to_pgwire(schema, res) {
                Ok(records) => {
                    let record_stream = stream::iter(records.into_iter()).boxed();
                    let response = Response::Query(QueryResponse::new( schema2, record_stream));
                    PgWireResult::Ok(response)
                },
                Err(err) => PgWireResult::Err(err),
            }
        } else if let Some(err) = result.error {
            PgWireResult::Err(err)
        } else {
            PgWireResult::Err(PgWireError::UserError(ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), "Unexpected Failure".to_owned()).into()))
        }
    }
    fn translate_records_to_pgwire(&self, record_schema:Arc<Vec<FieldInfo>>, records:Vec<Record>) -> PgWireResult<Vec<PgWireResult<DataRow>>> {
        let mut results = Vec::new();
        let num_cols = record_schema.len();
        for record in records {
            let mut encoder = DataRowEncoder::new(record_schema.clone());
            for col in 0..num_cols {
                let data = record.values.get(col).unwrap();
                match data {
                    Value::Null => encoder.encode_field(&None::<i8>).unwrap(),
                    Value::Integer(i) => { encoder.encode_field(&i).unwrap(); }
                    Value::Real(f) => { encoder.encode_field(&f).unwrap(); }
                    Value::Text(t) => { encoder.encode_field(t).unwrap(); }
                    Value::Blob(b) => { encoder.encode_field(&b).unwrap(); }
                }
            }
            results.push(encoder.finish());
        }
        Ok(results)
    }

    fn translate_schema_to_pgwire(&self, record_schema:Vec<Field>) -> Vec<FieldInfo> {
        record_schema.iter().map( | f | f.into()).collect::<Vec<FieldInfo>>()
    }

    fn parse_params(&self, portal: &Portal<String>) -> Vec<PgLiteDBParam> {
        let mut params = Vec::with_capacity(portal.parameter_len());
        for idx in 0..portal.parameter_len() {
            let param = if let Some(param_type) = portal.statement().parameter_types().get(idx) {
                match param_type {
                    &Type::BOOL => {
                        let value = portal.parameter::<bool>(idx, param_type).unwrap().map_or(Value::Null, |v| { if v { Value::Integer(1) } else { Value::Integer(0) } }); 
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value}
                    },
                    &Type::INT2 => {
                        let value = portal.parameter::<i16>(idx, param_type).unwrap().map_or(Value::Null, |v| Value::Integer(v.into())); 
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value}
                    },
                    &Type::INT4 => {
                        let value = portal.parameter::<i32>(idx, param_type).unwrap().map_or(Value::Null, |v| Value::Integer(v.into())); 
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value}
                    },
                    &Type::INT8 => {
                        let value = portal.parameter::<i64>(idx, param_type).unwrap().map_or(Value::Null, |v| Value::Integer(v.into())); 
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value}
                    },
                    &Type::TEXT | &Type::VARCHAR => {
                        let value = portal.parameter::<String>(idx, param_type).unwrap().map_or(Value::Null, |v| Value::Text(v));
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value }
                    },
                    &Type::FLOAT4  => {
                        let value = portal.parameter::<f32>(idx, param_type).unwrap().map_or(Value::Null, |v| Value::Real(v.into())); 
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value}
                    },
                    &Type::FLOAT8  => {
                        let value = portal.parameter::<f64>(idx, param_type).unwrap().map_or(Value::Null, |v| Value::Real(v.into())); 
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value}
                    },
                    &Type::BYTEA  => {
                        let value = portal.parameter::<Vec<u8>>(idx, param_type).unwrap().map_or(Value::Null, |v| Value::Blob(v.into()));
                        PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value}
                    },
                    _ => {
                        unimplemented!("This parameter type is not currently supported")
                    }
                }
            } else {
                PgLiteDBParam{ name:None, ordinal:Some(idx), param_type:None, value:Value::Null  }
            };
            params.push(param);
        }
        params
    }
}