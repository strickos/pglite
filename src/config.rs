use std::{path::PathBuf, net::SocketAddr};
use clap::Parser;
use log::LevelFilter;

use crate::{backend::PgLiteBackendType, auth::PgLiteAuthType};

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum PgLiteLogLevel {
    #[clap(alias = "off")]
    OFF,
    #[clap(alias = "error")]
    ERROR,
    #[clap(alias = "warn")]
    WARN,
    #[clap(alias = "info")]
    INFO,
    #[clap(alias = "debug")]
    DEBUG,
    #[clap(alias = "trace")]
    TRACE,
}

impl Into<LevelFilter> for PgLiteLogLevel {
    fn into(self) -> LevelFilter {
        match self {
            PgLiteLogLevel::OFF => LevelFilter::Off,
            PgLiteLogLevel::ERROR => LevelFilter::Error,
            PgLiteLogLevel::WARN => LevelFilter::Warn,
            PgLiteLogLevel::INFO => LevelFilter::Info,
            PgLiteLogLevel::DEBUG => LevelFilter::Debug,
            PgLiteLogLevel::TRACE => LevelFilter::Trace
        }
    }
}


#[derive(Debug, Parser)]
#[command(name = "pglite")]
#[command(about = "SQLite over Postgres", long_about = "This process will provide access to SQLite databases over a Postgres connnection.")]
pub struct PgLiteConfig {
    /// The address on which the process will listen on
    #[clap(
        long = "listen-address", 
        short = 'a', 
        env = "PGLITE_LISTEN_ADDR", 
        default_value = "0.0.0.0:5432"
    )]
    pub listen_addr: SocketAddr,
    
    /// The Database backend to use
    #[clap(
        long = "backend",
        short = 'b',
        value_enum,
        default_value = "simple",
        env = "PGLITE_BACKEND"
    )]
    pub backend: PgLiteBackendType,

    /// The Authenticator to use
    #[clap(
        long = "auth",
        short = 'x',
        value_enum,
        default_value = "basic",
        env = "PGLITE_AUTHENTICATOR"
    )]
    pub authenticator: PgLiteAuthType,

    /// The Configuration data for the authenticator
    #[clap(
        long = "auth-config",
        short = 'y',
        env = "PGLITE_AUTH_CONFIG"
    )]
    pub auth_config: Option<String>,

    /// The Log level to use for the console Log
    #[clap(
        long = "consolelog-level",
        short = 'c',
        value_enum,
        default_value = "info",
        env = "PGLITE_CONSOLELOG_LEVEL"
    )]
    pub consolelog_level: PgLiteLogLevel,

    /// The Log level to use for the File Log
    #[clap(
        long = "filelog-level",
        short = 'l',
        value_enum,
        default_value = "off",
        env = "PGLITE_FILELOG_LEVEL"
    )]
    pub filelog_level: PgLiteLogLevel,

    /// The path to where the file log will be written (if enabled)
    #[clap(
        long = "filelog-path", 
        short = 'f', 
        default_value = "/var/log/pglite", 
        env = "PGLITE_FILELOG_PATH"
    )]
    pub filelog_path: PathBuf,

    /// The path to the root directory under which the SQLite databases will be read (if required by the backend)
    #[clap(
        long = "db-root", 
        short = 'p', 
        default_value = "./local-data", 
        env = "PGLITE_DB_ROOT"
    )]
    pub db_root: PathBuf,

    // The number of idle seconds after which the handle to the database file will be released (if supported by the backend)
    #[clap(
        long = "db-idle-timeout", 
        short = 't', 
        default_value = "600", 
        env = "PGLITE_DB_IDLE_TIMEOUT"
    )]
    pub db_idle_timeout: u64,
}
