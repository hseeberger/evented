use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use sqlx::postgres::{PgConnectOptions, PgSslMode};

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub dbname: String,
    #[serde_as(as = "DisplayFromStr")]
    pub sslmode: PgSslMode,
}

impl From<PgConfig> for PgConnectOptions {
    fn from(config: PgConfig) -> PgConnectOptions {
        PgConnectOptions::new()
            .host(&config.host)
            .username(&config.user)
            .password(config.password.expose_secret())
            .database(&config.dbname)
            .port(config.port)
            .ssl_mode(config.sslmode)
    }
}
