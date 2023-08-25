use std::{collections::HashMap, path::PathBuf};
use pgwire::{error::{ErrorInfo, PgWireError}, messages::startup::{Authentication, PasswordMessageFamily}};
use async_trait::async_trait;
use futures::SinkExt;

use crate::implement_startup_handler;
use super::{PgLiteAuthenticator, PgLiteAuthenticatorFactory};

pub struct BasicPasswordAuthenticator { 
    expected_password:String
}
implement_startup_handler!(BasicPasswordAuthenticator);

pub struct BasicPasswordAuthenticatorFactory {}
impl PgLiteAuthenticatorFactory<BasicPasswordAuthenticator> for BasicPasswordAuthenticatorFactory {
    fn create_authenticator(&mut self, config:&crate::config::PgLiteConfig) -> Result<BasicPasswordAuthenticator, PgWireError> {
        let expected_password = config.auth_config.to_owned().unwrap_or(String::from("123"));
        Ok(BasicPasswordAuthenticator{  expected_password })
    }
}
impl BasicPasswordAuthenticatorFactory {
    pub fn load_and_create_authenticator(config:&crate::config::PgLiteConfig) -> Result<BasicPasswordAuthenticator, PgWireError> {
        let mut factory = BasicPasswordAuthenticatorFactory{};
        factory.create_authenticator(config)
    }
}

#[async_trait]
impl PgLiteAuthenticator for BasicPasswordAuthenticator {
    fn pg_auth_type(&self) -> Authentication {
        Authentication::CleartextPassword
    }

    async fn verify_identity(&self, credential_data:PasswordMessageFamily, username:String, database: String) -> Result<HashMap<String, String>, ErrorInfo> {
        let Ok(psw_data) = credential_data.into_password() else { return Err(ErrorInfo::new( "FATAL".to_owned(),"28P01".to_owned(),
            "Authentication was not successful, please check you have provided all the credentials required for this database.".to_owned(),
        ))};
        let password = psw_data.password();
        
        // TODO: do something real here :p
        if self.expected_password.eq(password) {
            // Correct Password, save data to connection + move on
            let mut result = HashMap::new();
            result.insert(String::from("user"), username.clone());
            result.insert(String::from("database"), database.clone());
            result.insert(String::from("dbpath"), PathBuf::from(&username).join(&database).to_string_lossy().to_string());
            Ok(result)
        } else {
            // Incorrect Passwowrd
            Err(ErrorInfo::new(
                "FATAL".to_owned(),
                "28P01".to_owned(),
                "Authentication was not successful, please check you have provided the correct credentials for this database.".to_owned(),
            ))
        }
    }
}