use std::{path::PathBuf, sync::{Arc, RwLock}, collections::HashMap, time::Duration};

use crossbeam_channel::RecvTimeoutError;
use pgwire::error::{PgWireResult, PgWireError, ErrorInfo};
use rusqlite::{Connection, Error, Rows, types::{Value, Type}, Statement, ToSql};
use tokio::task::spawn_blocking;

use crate::{config::PgLiteConfig, backend::{PgLiteDBResponse, MessageType}};
use super::{PgLitebackendFactory, PgLiteDBBackend, PgLiteDBMessage, BackendConnection, Field, Record, PgLiteDBParam};

pub struct SimplePgLiteDBBackend {
    con:Connection
}

type BackendMap = HashMap<String, BackendConnection>;
pub struct SimplePgLiteDBBackendFactory { 
    db_root:PathBuf,
    db_idle_timeout:Duration,
    db_cache: Arc<RwLock<BackendMap>>
}

impl SimplePgLiteDBBackendFactory {
    pub fn new(config:&PgLiteConfig) -> Self {
        Self { 
            db_root: PathBuf::from(config.db_root.clone()), 
            db_idle_timeout:Duration::from_secs(config.db_idle_timeout), 
            db_cache: Arc::new(RwLock::new(HashMap::with_capacity(100))) 
        }
    }

    fn spawn_backend_connection(&self, db_path:PathBuf) -> BackendConnection  {
        let (tx, rx) = crossbeam_channel::unbounded::<PgLiteDBMessage>();
        let backend_conn: BackendConnection = BackendConnection{ sender:tx };
        let db_path_string = db_path.to_string_lossy().to_string();

        // Add the DB Connection (aka. the channel for sending messages to the backend) to the cache - for later use...
        {
            let cref = self.db_cache.write();
            if let Ok(mut cache) = cref {
                cache.insert(db_path_string.clone(), backend_conn.clone());
            } else {
                error!("Failed to acquire the cache lock for DB at: {}", &db_path_string);
            }
        }

        // Spawn a thread to handle queries into this DB
        let cache_ref = self.db_cache.clone();
        let idle_timeout = self.db_idle_timeout.clone();
        spawn_blocking(move || {
            let backend: SimplePgLiteDBBackend = SimplePgLiteDBBackend::open(db_path).unwrap();
            trace!("[{}] Opened new DB Handle", &db_path_string);

            // Loop + handle messages endlessly until the the IDLE timeout has passed (or the sending stream is closed, which shouldn't happen :p)...
            loop {
                let message = match rx.recv_timeout(idle_timeout) {
                    Ok(msg) => msg,
                    Err(RecvTimeoutError::Timeout) => { break; /* DB hasn't been used for the IDLE timeout period, so exit */ }, 
                    Err(RecvTimeoutError::Disconnected) => { break; /* Connection to the DB was lost for some reason?! So exit */ }
                };

                trace!("[{}] Handling {:#?} Message with query: {:#?}", &db_path_string, &message.message_type, &message.query);
                let result = match message.message_type {
                    MessageType::SimpleQuery => backend.query(message.query.as_str()), 
                    MessageType::QueryWithParams => backend.query_with_params(&message.query.as_str(), message.params.unwrap_or_default()),
                    MessageType::Describe => { backend.describe_query(message.query.as_str()) }, 
                };
                
                match result {
                    Ok(res) => {
                        if message.respond.send(res).is_err() {
                            trace!("[{}] Unable to send response to client - it's been disconnected...", &db_path_string);
                        }
                    }, 
                    Err(err) => {
                        if message.respond.send(PgLiteDBResponse{ result_schema:None, result:None, error:Some(err) }).is_err() {
                            trace!("[{}] Unable to send an error response to client - it's been disconnected...", &db_path_string);
                        }
                    }
                }
            }

            // Remove the database from the cache
            debug!("[{}] Closing the database handle - it hasn't been used for the IDLE timeout period", &db_path_string);
            cache_ref.write().unwrap().remove(&db_path_string);

            // Finally, close the handle to the database
            if let Err(err) = backend.close() {
                error!("[{}] Encountered an error closing the DB Handle, Error: {}", &db_path_string, err);
            }
        });

        backend_conn
    }
}

impl PgLitebackendFactory for SimplePgLiteDBBackendFactory {
    fn create_backend(&self, metadata:&HashMap<String, String>) -> Result<BackendConnection, PgWireError> {
        // The DB Path is extracted from the connection metadata
        let db_path = self.db_root.join(metadata.get("dbpath").unwrap_or(&String::from("blackhole")).to_owned());

        // Check if we already have a handle to this database in the cache - and return it if we do
        {
            let cache_lock_res = self.db_cache.read();
            if let Ok(cache_lock) = cache_lock_res {
                if let Some(cached_backend) = cache_lock.get(&db_path.to_string_lossy().to_string())  {
                    trace!("[{}] Using Cached DB Handle", db_path.to_string_lossy());
                    return Ok(cached_backend.clone());
                }
            }
        }

        // Not in cache, so spawn a new thread to handle this DB path
        let conn = self.spawn_backend_connection(db_path);
        return Ok(conn);
    }
}

impl SimplePgLiteDBBackend {
    pub fn open(db_path:PathBuf) -> Result<Self, Error> {
        let con = Connection::open(db_path)?;   // todo: Check the open flags we should use...
        Ok(Self { con })
    }

    fn get_sqlite_type_for_type(&self, name: &str) -> PgWireResult<Type> {
        // Ignore the additional specifiers like the field length (which aren't important for sqlite)
        let type_str = name
                .to_uppercase()
                .chars()
                .take_while(|&ch| ch != ' ' && ch != '(')
                .collect::<String>();
    
        // Match the Postgres type + return the sqlite equivalent type
        match type_str.as_ref() {
            "INT" => Ok(Type::Integer),
            "VARCHAR" => Ok(Type::Text),
            "DATE" => Ok(Type::Real),
            "TIME" => Ok(Type::Real),
            "TIMESTAMP" => Ok(Type::Real),
            "TEXT" => Ok(Type::Text),
            "BINARY" => Ok(Type::Blob),
            "FLOAT" => Ok(Type::Real),
            "SERIAL" => Ok(Type::Integer), // todo: Handle SERIAL properly ... 
            _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42846".to_owned(),
                format!("Unsupported data type: {name}"),
            )))),
        }
    }

    fn build_record_schema_from_statement(&self, stmt: &Statement) -> Vec<Field> {
        stmt.columns()
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                Field { 
                    field_type:self.get_sqlite_type_for_type(col.decl_type().unwrap()).unwrap(), 
                    name:col.name().to_owned(), 
                    ordinal:idx
                }
            })
            .collect()
    }

    fn build_records(&self, mut row_data: Rows, num_fields: usize) -> Vec<Record> {
        let mut records = Vec::new();   // todo: consider whether we can stream records back as we go through the recordset?! 
        while let Ok(Some(row)) = row_data.next() {
            let mut record = Record{ values:Vec::with_capacity(num_fields) };
            for field_num in 0..num_fields {
                let data = row.get_unwrap(field_num);
                record.values.push(data);
            }
            records.push(record);
        }
        records
    }
    
}

impl PgLiteDBBackend for SimplePgLiteDBBackend {
    fn close(&self) -> Result<(), PgWireError> {
        // We'll rely on the drop functionality - as we cannot call close() on self.con as this method will attempt to take ownership of self :p
        Ok(())
    }
    fn query(&self, query:&str) -> PgWireResult<PgLiteDBResponse> {
        let result = match query.to_uppercase().starts_with("SELECT") {
            true => {
                let mut statement = self.con
                    .prepare(query)
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                let fields = self.build_record_schema_from_statement(&statement);
                let num_fields = fields.len();
                statement.query(())
                    .map(|row_data| {
                        (fields, self.build_records(row_data, num_fields))
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))
            },
            false => {
                self.con
                    .execute(query, ())
                    .map(|affected_rows| {
                        let fields = vec![Field{ name:String::from("OK"), field_type:Type::Integer, ordinal:0 }];
                        let record = Record{ values:vec![ Value::Integer(affected_rows as i64) ] };
                        (fields, vec![record])
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))
            }
        };

        match result {
            Ok( (record_schema, records)) => PgWireResult::Ok(PgLiteDBResponse { result_schema:Some(record_schema), result: Some(records), error: None  }),
            Err(err) => Err(err)
        }
    }

    fn query_with_params(&self, query:&str, params:Vec<PgLiteDBParam>) -> PgWireResult<PgLiteDBResponse> {
        // Prepare the statement or get from cache
        let mut statement = self.con
                .prepare_cached(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // Prepare the params for the statement
        let sql_params: Vec<Box<dyn ToSql>> = params.iter().map(|p| { Box::new(p.value.clone()) as Box<dyn ToSql> }).collect();
        let sql_params_ref = sql_params.iter()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn rusqlite::ToSql>>();

        // Execute the Statement / Query
        let result = match query.to_uppercase().starts_with("SELECT") {
                true => {
                    let fields = self.build_record_schema_from_statement(&statement);
                    let num_fields = fields.len();
                    statement.query::<&[&dyn rusqlite::ToSql]>(sql_params_ref.as_ref())
                    .map(|row_data| {
                        (fields, self.build_records(row_data, num_fields))
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))
                }, 
                false => {
                    statement.execute::<&[&dyn rusqlite::ToSql]>(sql_params_ref.as_ref())
                    .map(|affected_rows| {
                        let fields = vec![Field{ name:String::from("OK"), field_type:Type::Integer, ordinal:0 }];
                        let record = Record{ values:vec![ Value::Integer(affected_rows as i64) ] };
                        (fields, vec![record])
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))
                }
            };
        match result {
            Ok( (record_schema, records)) => PgWireResult::Ok(PgLiteDBResponse { result_schema:Some(record_schema), result: Some(records), error: None  }),
            Err(err) => Err(err)
        }
    }

    fn describe_query(&self, query:&str) -> PgWireResult<PgLiteDBResponse> {
        // Simply prepare the statement and get the schema
        let statement = self.con
                .prepare_cached(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let fields = self.build_record_schema_from_statement(&statement);
        PgWireResult::Ok(PgLiteDBResponse { result_schema:Some(fields), result: None, error: None  })
    }
}