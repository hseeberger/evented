use crate::pool::Pool;
use error_ext::BoxError;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, Encode, Executor, Postgres, Row, Transaction, Type};
use std::{fmt::Debug, marker::PhantomData, num::NonZeroU64};
use thiserror::Error;
use tracing::instrument;

pub struct EventLog<I> {
    pool: Pool,
    _i: PhantomData<I>,
}

impl<I> EventLog<I>
where
    I: Debug + for<'q> Encode<'q, Postgres> + Type<Postgres> + Sync,
{
    pub async fn new(pool: Pool, ddl: Option<&str>) -> Result<Self, Error> {
        if let Some(ddl) = ddl {
            (&*pool).execute(ddl).await.map_err(|error| {
                Error::Sqlx("cannot create tables for event log".to_string(), error)
            })?;
        }

        Ok(Self {
            pool,
            _i: PhantomData,
        })
    }

    #[instrument(skip(self, events, listener))]
    pub async fn persist<E, L>(
        &mut self,
        id: &I,
        last_seq_no: Option<NonZeroU64>,
        type_name: &'static str,
        events: &[E],
        listener: &mut Option<L>,
    ) -> Result<NonZeroU64, Error>
    where
        E: Serialize + Sync,
        L: EventListener,
    {
        assert!(!events.is_empty());

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| Error::Sqlx("cannot begin transaction".to_string(), error))?;

        let seq_no = sqlx::query("SELECT MAX(seq_no) FROM events WHERE id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| Error::Sqlx("cannot select max seq_no".to_string(), error))
            .and_then(into_seq_no)?;

        if seq_no != last_seq_no {
            return Err(Error::UnexpectedSeqNo(seq_no, last_seq_no));
        }

        let mut seq_no = last_seq_no.map(|n| n.get() as i64).unwrap_or_default();
        for event in events.iter() {
            seq_no += 1;
            let bytes = serde_json::to_vec(event).map_err(Error::Ser)?;
            sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4)")
                .bind(id)
                .bind(seq_no)
                .bind(type_name)
                .bind(&bytes)
                .execute(&mut *tx)
                .await
                .map_err(|error| Error::Sqlx("cannot execute statement".to_string(), error))?;

            if let Some(listener) = listener {
                listener
                    .listen(&event, &mut tx)
                    .await
                    .map_err(Error::Listener)?;
            }
        }

        tx.commit()
            .await
            .map_err(|error| Error::Sqlx("cannot commit transaction".to_string(), error))?;

        (seq_no as u64).try_into().map_err(|_| Error::ZeroSeqNo)
    }

    #[instrument(skip(self))]
    pub async fn current_events_by_id<'i, E>(
        &self,
        id: &'i I,
    ) -> impl Stream<Item = Result<(NonZeroU64, E), Error>> + Send + 'i
    where
        E: for<'de> Deserialize<'de> + Send,
    {
        sqlx::query("SELECT seq_no, event FROM events WHERE id = $1")
            .bind(id)
            .fetch(&*self.pool)
            .map_err(|error| Error::Sqlx("cannot get next event".to_string(), error))
            .map(|row| {
                row.and_then(|row| {
                    let seq_no = (row.get::<i64, _>(0) as u64)
                        .try_into()
                        .map_err(|_| Error::ZeroSeqNo)?;
                    let bytes = row.get::<&[u8], _>(1);
                    let event = serde_json::from_slice::<E>(bytes).map_err(Error::De)?;
                    Ok((seq_no, event))
                })
            })
    }
}

/// Invoked for each event during the `persist` transaction.
#[trait_variant::make(Send)]
pub trait EventListener {
    async fn listen<E>(
        &mut self,
        event: &E,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), BoxError>
    where
        E: Sync;
}

pub struct NoEventListener;

impl EventListener for NoEventListener {
    async fn listen<E>(
        &mut self,
        _event: &E,
        _tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), BoxError>
    where
        E: Sync,
    {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Sqlx(String, #[source] sqlx::Error),

    #[error("cannot serialize event")]
    Ser(#[source] serde_json::Error),

    #[error("cannot deserialize event")]
    De(#[source] serde_json::Error),

    #[error("sequence number must not be zero")]
    ZeroSeqNo,

    #[error("expected sequence number {0:?}, but was {1:?}")]
    UnexpectedSeqNo(Option<NonZeroU64>, Option<NonZeroU64>),

    #[error("listener error")]
    Listener(#[source] BoxError),
}

fn into_seq_no(row: PgRow) -> Result<Option<NonZeroU64>, Error> {
    // If there is no seq_no there is one row with a NULL column, hence use `try_get`.
    row.try_get::<i64, _>(0)
        .ok()
        .map(|seq_no| (seq_no as u64).try_into().map_err(|_| Error::ZeroSeqNo))
        .transpose()
}

#[cfg(test)]
mod tests {
    use crate::{
        event_log::EventLog,
        pool::{Config, Pool},
    };
    use futures::{future::ok, StreamExt, TryStreamExt};
    use serde::{Deserialize, Serialize};
    use sqlx::postgres::PgSslMode;
    use testcontainers::{runners::AsyncRunner, RunnableImage};
    use testcontainers_modules::postgres::Postgres;

    #[derive(Debug, Serialize, Deserialize)]
    struct Event {
        n: u64,
    }

    #[tokio::test]
    async fn test_current_events_by_id() {
        let container = RunnableImage::from(Postgres::default())
            .with_tag("16-alpine")
            .start()
            .await;
        let pg_port = container.get_host_port_ipv4(5432).await;

        let config = Config {
            host: "localhost".to_string(),
            port: pg_port,
            user: "postgres".to_string(),
            password: "postgres".to_string().into(),
            dbname: "postgres".to_string(),
            sslmode: PgSslMode::Prefer,
        };

        let pool = Pool::new(config).await.expect("pool can be created");
        let ddl = include_str!("create_event_log_text.sql");
        let event_log = EventLog::<String>::new(pool.clone(), Some(ddl))
            .await
            .unwrap();

        let id = "unknown".to_string();
        let events = event_log.current_events_by_id::<Event>(&id).await;
        // events.for_each(|event| ready(println!("{event:?}"))).await;
        let n = events.count().await;
        assert_eq!(n, 0);

        sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(1_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event { n: 1 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(2_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event { n: 2 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();
        let events = event_log.current_events_by_id::<Event>(&id).await;
        let events = events
            .try_fold(vec![], |mut acc, (seq_no, Event { n })| {
                acc.push((seq_no.get(), n));
                ok(acc)
            })
            .await
            .unwrap();
        assert_eq!(events, vec![(1, 1), (2, 2)])
    }
}
