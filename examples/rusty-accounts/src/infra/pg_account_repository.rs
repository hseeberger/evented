use crate::domain::{self, AccountRepository};
use futures::{Stream, TryStreamExt};
use sqlx::{prelude::FromRow, PgPool};
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PgAccountRepository {
    pool: PgPool,
}

impl PgAccountRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl AccountRepository for PgAccountRepository {
    type Error = sqlx::Error;

    #[instrument(skip(self))]
    async fn accounts(
        &self,
    ) -> Result<impl Stream<Item = Result<domain::Account, Self::Error>> + Send, Self::Error> {
        let accounts = sqlx::query_as::<_, Account>("SELECT * FROM account")
            .fetch(&self.pool)
            .map_ok(domain::Account::from);
        Ok(accounts)
    }
}

#[derive(Debug, FromRow)]
struct Account {
    id: Uuid,
    balance: i64,
}

impl From<Account> for domain::Account {
    fn from(Account { id, balance }: Account) -> Self {
        let balance = balance as u64;
        domain::Account { id, balance }
    }
}
