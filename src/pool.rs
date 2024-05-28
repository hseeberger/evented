//! Connection pool for sqlx with Postgres.

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    PgPool,
};
use std::ops::Deref;
use tracing::debug;

/// New type for `PgPool`, allowing for some custom extensions as well as security.
///
/// To use as `&PgPool` in `Query::execute`, use its `Deref` implementation: `&*pool` or
/// `pool.deref()`. If an owned `PgPool` is needed, use `Into::into`.
#[derive(Debug, Clone)]
pub struct Pool(PgPool);

impl Pool {
    /// Try to create a new [Pool] with the given [Config].
    pub async fn new(config: Config) -> Result<Self, sqlx::Error> {
        let pg_pool = PgPoolOptions::new().connect_with(config.into()).await?;
        let pool = Pool(pg_pool);
        debug!(?pool, "created pool");

        Ok(pool)
    }
}

impl Deref for Pool {
    type Target = PgPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Configuration for [Pool].
#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub dbname: String,
    #[serde_as(as = "DisplayFromStr")]
    pub sslmode: PgSslMode,
}

impl From<Config> for PgConnectOptions {
    fn from(config: Config) -> PgConnectOptions {
        PgConnectOptions::new()
            .host(&config.host)
            .username(&config.user)
            .password(config.password.expose_secret())
            .database(&config.dbname)
            .port(config.port)
            .ssl_mode(config.sslmode)
    }
}

#[cfg(test)]
mod tests {
    use crate::pool::{Config, Pool};
    use sqlx::postgres::PgSslMode;
    use std::{error::Error as StdError, ops::Deref};
    use testcontainers::{runners::AsyncRunner, RunnableImage};
    use testcontainers_modules::postgres::Postgres;

    type TestResult = Result<(), Box<dyn StdError>>;

    #[tokio::test]
    async fn test_pool() -> TestResult {
        let container = RunnableImage::from(Postgres::default())
            .with_tag("16-alpine")
            .start()
            .await?;
        let pg_port = container.get_host_port_ipv4(5432).await?;

        let config = Config {
            host: "localhost".to_string(),
            port: pg_port,
            user: "postgres".to_string(),
            password: "postgres".to_string().into(),
            dbname: "postgres".to_string(),
            sslmode: PgSslMode::Prefer,
        };

        let pool = Pool::new(config).await;
        assert!(pool.is_ok());
        let pool = pool.unwrap();

        let result = sqlx::query("CREATE TABLE test (id integer PRIMARY KEY)")
            .execute(pool.deref())
            .await;
        assert!(result.is_ok());

        Ok(())
    }
}
