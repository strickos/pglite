use std::{sync::{Arc, Mutex}, collections::HashMap};
use pgwire::api::{auth::ServerParameterProvider, ClientInfo};
use tokio::{net::TcpListener, task::JoinHandle};

use crate::{config::PgLiteConfig, backend::PgLitebackendFactory, auth::PgLiteAuthenticator, connection::PgLiteConnection};

pub struct PgLiteServerParameterProvider;

impl ServerParameterProvider for PgLiteServerParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(4);
        params.insert("server_version".to_owned(), env!("CARGO_PKG_VERSION").to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO YMD".to_owned());
        Some(params)
    }
}


pub struct PgLiteServer<F,A> {
    config:PgLiteConfig, 
    backend_factory:Arc<Mutex<F>>,
    authenticator:Arc<A>,
 }

impl <F,A> PgLiteServer<F,A>
where   F : PgLitebackendFactory + Send + Sync + 'static,
        A : PgLiteAuthenticator + Send + 'static { 

    pub fn start(config:PgLiteConfig, backend_factory:F, authenticator:A) -> JoinHandle<()> {
        let server = Self { config, backend_factory:Arc::new(Mutex::new(backend_factory)), authenticator:Arc::new(authenticator) };
        let handle = tokio::spawn( async move {  server.run().await } );
        handle
    }

    async fn run(&self) {
        // Bind to the server address and process every new connection
        let listen_addr = self.config.listen_addr;
        let listener: TcpListener = TcpListener::bind(listen_addr).await.unwrap();
        info!("PGLite is up and running! Listening at: {}", listen_addr);

        loop {
            trace!("Ready for next connection...");
            let (stream, addr) = listener.accept().await.unwrap();

            let backend_factory = self.backend_factory.clone();
            let authenticator = self.authenticator.clone();
            tokio::spawn(async move {
                let mut conn = PgLiteConnection::create(backend_factory, authenticator);
                debug!("Processing new connection, ID: {}, Address: {}", &conn.connection_id, addr);
                if let Err(err) = conn.handle(stream, addr).await {
                    error!("[{}] Unhandled error in connection processor: {:#?}", &conn.connection_id, err);
                }
                debug!("[{} ]Connection Closed", &conn.connection_id);
            });
        }
    }
}