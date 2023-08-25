use std::{fmt::Debug, collections::HashMap};
use async_trait::async_trait;
use pgwire::{error::{PgWireError, ErrorInfo}, api::auth::StartupHandler, messages::startup::{Authentication, PasswordMessageFamily}};

mod basic_authenticator;
use basic_authenticator::BasicPasswordAuthenticatorFactory;

use crate::config::PgLiteConfig;

#[async_trait]
pub trait PgLiteAuthenticator : StartupHandler + Send + Sync { 
    fn pg_auth_type(&self) -> Authentication;
    async fn verify_identity(&self, credential_data:PasswordMessageFamily, username:String, database: String) -> Result<HashMap<String, String>, ErrorInfo>;
}

#[macro_export]
macro_rules! implement_startup_handler {
    ($t:ty) => (
        #[async_trait]
        impl pgwire::api::auth::StartupHandler for $t {
            async fn on_startup<C>(&self, client: &mut C, message: pgwire::messages::PgWireFrontendMessage) -> pgwire::error::PgWireResult<()>
            where
                C: pgwire::api::ClientInfo + futures_sink::Sink<pgwire::messages::PgWireBackendMessage> + Unpin + Send,
                C::Error: std::fmt::Debug,
                PgWireError: From<<C as futures_sink::Sink<pgwire::messages::PgWireBackendMessage>>::Error> {
                    match message {
                        pgwire::messages::PgWireFrontendMessage::Startup(sm) => {
                            // Save startup parameters to the metadata
                            pgwire::api::auth::save_startup_parameters_to_metadata(client, &sm);
                            // Set the state to Auth in progress
                            client.set_state(pgwire::api::PgWireConnectionState::AuthenticationInProgress);
                            // Request the authentication data from the client
                            client.send(pgwire::messages::PgWireBackendMessage::Authentication(self.pg_auth_type())).await?;
                            return Ok(());
                        },
                        pgwire::messages::PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                            // Extract the name of the database that the client wishes to connect to
                            let database = client.metadata().get(pgwire::api::METADATA_DATABASE).unwrap_or(&String::from("unknown")).clone();
                            let username = client.metadata().get(pgwire::api::METADATA_USER).unwrap_or(&String::from("unknown")).clone();
                            // Verify the identity of the client and save the metadata to the client
                            match self.verify_identity(pwd, username, database).await {
                                Ok(metadata) => {
                                    // Copy the metadata from the auth provider into the client
                                    let client_meta = client.metadata_mut();
                                    metadata.into_iter().for_each(|(k,v)| { client_meta.insert(k, v); } );
                                    pgwire::api::auth::finish_authentication(client, &crate::server::PgLiteServerParameterProvider).await;
                                    Ok(())
                                },
                                Err(error_info) => {
                                    // Identity Verification failed - return an auth error
                                    client.feed(pgwire::messages::PgWireBackendMessage::ErrorResponse(error_info.into())).await?;
                                    client.close().await?;
                                    Ok(())
                                }
                            }
                        },
                        _ => Ok(())
                    }
            }
        }
    )
}

pub trait PgLiteAuthenticatorFactory<T>
where T: PgLiteAuthenticator {
    fn create_authenticator(&mut self, config:&PgLiteConfig) -> Result<T, PgWireError>;
}


#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum PgLiteAuthType {
    #[clap(alias = "basic")]
    BasicPasswordAuthenticator,
}

pub fn load_authenticator(config:&PgLiteConfig) -> impl PgLiteAuthenticator {
    match config.authenticator {
        PgLiteAuthType::BasicPasswordAuthenticator => BasicPasswordAuthenticatorFactory::load_and_create_authenticator(config).unwrap(),
        // todo: add other auth handlers...
    }
}