mod pg_account_event_handler;
mod pg_account_repository;

pub use pg_account_event_handler::*;
pub use pg_account_repository::*;

#[cfg(test)]
mod tests {
    use crate::{
        domain::{Account, AccountEvent, AccountRepository},
        infra::{PgAccountEventHandler, PgAccountRepository},
    };
    use error_ext::BoxError;
    use evented::projection::EventHandler;
    use futures::TryStreamExt;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use testcontainers::{runners::AsyncRunner, RunnableImage};
    use testcontainers_modules::postgres::Postgres as TCPostgres;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_account_repo_event_handler() -> Result<(), BoxError> {
        let container =
            AsyncRunner::start(RunnableImage::from(TCPostgres::default()).with_tag("16-alpine"))
                .await?;
        let port = container.get_host_port_ipv4(5432).await?;

        let cnn_url = format!("postgresql://postgres:postgres@localhost:{port}");
        let cnn_options = cnn_url.parse::<PgConnectOptions>()?;
        let pool = PgPoolOptions::new().connect_with(cnn_options).await?;

        sqlx::migrate!().run(&pool).await?;

        let account_repository = PgAccountRepository::new(pool.clone());

        let accounts = account_repository
            .accounts()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert!(accounts.is_empty());

        let id_1 = Uuid::now_v7();
        let id_2 = Uuid::now_v7();
        let mut tx = pool.begin().await?;
        PgAccountEventHandler
            .handle_event(AccountEvent::Created { id: id_1 }, &mut tx)
            .await?;
        tx.commit().await?;
        let mut tx = pool.begin().await?;
        PgAccountEventHandler
            .handle_event(AccountEvent::Created { id: id_2 }, &mut tx)
            .await?;
        tx.commit().await?;

        let accounts = account_repository
            .accounts()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            accounts,
            vec![
                Account {
                    id: id_1,
                    balance: 0
                },
                Account {
                    id: id_2,
                    balance: 0
                }
            ]
        );

        let mut tx = pool.begin().await?;
        PgAccountEventHandler
            .handle_event(
                AccountEvent::Deposited {
                    id: id_1,
                    amount: 10,
                    balance: 10,
                },
                &mut tx,
            )
            .await?;
        tx.commit().await?;

        let accounts = account_repository
            .accounts()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert!(accounts.contains(&Account {
            id: id_1,
            balance: 10,
        }));

        Ok(())
    }
}
