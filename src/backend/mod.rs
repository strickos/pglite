
mod simple_backend;
use std::collections::HashMap;
use crossbeam_channel::Sender;
use pgwire::api::results::FieldFormat;
use pgwire::api::results::FieldInfo;
use pgwire::error::PgWireError;
use pgwire::error::PgWireResult;
use rusqlite::types::Type;
use rusqlite::types::Value;
pub use simple_backend::SimplePgLiteDBBackend;
pub use simple_backend::SimplePgLiteDBBackendFactory;

use crate::config::PgLiteConfig;

pub trait PgLiteDBBackend { 
    fn close(&self) -> Result<(), PgWireError>;
    fn query(&self, query:&str) -> PgWireResult<PgLiteDBResponse>;
    fn query_with_params(&self, query:&str, params:Vec<PgLiteDBParam>) -> PgWireResult<PgLiteDBResponse>;
    fn describe_query(&self, query:&str) -> PgWireResult<PgLiteDBResponse>;
}

pub trait PgLitebackendFactory {
    fn create_backend(&self, metadata:&HashMap<String, String>) -> Result<BackendConnection, PgWireError>;
}


#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum PgLiteBackendType {
    #[clap(alias = "simple")]
    SimplePgLiteDBBackend,
}


pub fn load_backend_factory(config:&PgLiteConfig) -> impl PgLitebackendFactory {
    match config.backend {
        PgLiteBackendType::SimplePgLiteDBBackend => SimplePgLiteDBBackendFactory::new(config),
        // todo: add additional backends...
    }
}


/* Follows is the types used to communicate between the pglite "connection" and the "backend" */

#[derive(Debug, Clone)]
pub struct Field {
    pub ordinal: usize,
    pub name: String,
    pub field_type: Type,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub values: Vec<Value>
}

pub struct PgLiteDBResponse {
    pub result_schema: Option<Vec<Field>>,
    pub result:Option<Vec<Record>>,
    pub error:Option<PgWireError>
}

#[derive(Debug, Clone)]
pub struct PgLiteDBParam {
    pub name:Option<String>,    // Name based params are not currently supported, so this is here for future use
    pub ordinal:Option<usize>, 
    pub param_type:Option<Type>, 
    pub value:Value
}

#[derive(Debug, Clone)]
pub enum MessageType {
    SimpleQuery, 
    QueryWithParams, 
    Describe
}

#[derive(Debug, Clone)]
pub struct PgLiteDBMessage {
    pub message_type:MessageType,
    pub query:String,
    pub params:Option<Vec<PgLiteDBParam>>,
    pub respond: Sender<PgLiteDBResponse>
}

impl PgLiteDBMessage {
    pub fn from_query(query:String, respond: Sender<PgLiteDBResponse>) -> Self {
        Self { message_type:MessageType::SimpleQuery, query, respond, params:None }
    }
    pub fn from_query_with_params(query:String, params:Vec<PgLiteDBParam>, respond: Sender<PgLiteDBResponse>) -> Self {
        Self { message_type:MessageType::QueryWithParams, query, respond, params:Some(params) }
    }
    pub fn from_describe(query:String, respond: Sender<PgLiteDBResponse>) -> Self {
        Self { message_type:MessageType::Describe, query, respond, params:None }
    }
}

#[derive(Debug, Clone)]
pub struct BackendConnection {
    pub sender:Sender<PgLiteDBMessage>
}

impl Into<FieldInfo> for &Field {
    fn into(self) -> FieldInfo {
        FieldInfo::new(
            self.name.clone(),
            None,
            None,
            get_pgwiretype_for_type(&self.field_type),
            match self.field_type {  Type::Blob => FieldFormat::Binary, _ => FieldFormat::Text }
        )
    }
}

fn get_pgwiretype_for_type(field_type:&Type) -> pgwire::api::Type {
    match field_type {  
        Type::Integer => pgwire::api::Type::INT8,
        Type::Real => pgwire::api::Type::FLOAT8,
        Type::Text => pgwire::api::Type::TEXT,
        Type::Blob => pgwire::api::Type::BYTEA,
        _ => pgwire::api::Type::VARCHAR
    }
}