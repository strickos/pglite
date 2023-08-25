use std::borrow::BorrowMut;
use clap::Parser;

#[macro_use] 
extern crate log;
extern crate simplelog;
pub use simplelog::*;
use std::fs::File;

mod config;
mod auth;
mod backend;
mod server;
mod connection;
mod query_handler;

use config::{PgLiteConfig, PgLiteLogLevel};
use backend::load_backend_factory;
use auth::load_authenticator;
use server::PgLiteServer;

#[tokio::main]
async fn main() {
    // Build the Config
    let config = PgLiteConfig::parse();

    // Configure the Logger
    let mut loggers: Vec<Box<dyn SharedLogger>> = vec![ TermLogger::new(config.consolelog_level.clone().into(), Config::default(), TerminalMode::Mixed, ColorChoice::Auto) ];
    if config.filelog_level != PgLiteLogLevel::OFF {
        loggers.push(WriteLogger::new(config.filelog_level.clone().into(), Config::default(), File::create(config.filelog_path.clone()).unwrap()));
    }
    CombinedLogger::init(loggers).unwrap();

    // Load the DB Backend
    let backend = load_backend_factory(&config);

    // Load the Authenticator
    let authenticator = load_authenticator(&config);

    // Start the server
    let mut server_handle = PgLiteServer::start(config, backend, authenticator);
    server_handle.borrow_mut().await.unwrap();
}

