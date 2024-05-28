pub mod pool;

use crate::pool::Pool;
use error_ext::BoxError;
use futures::{future::ok, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, Encode, Postgres, Row, Transaction, Type};
use std::{
    fmt::{Debug, Display},
    num::NonZeroU64,
};
use thiserror::Error;
use tracing::instrument;

/// A command for an [EventSourcedEntity].
#[trait_variant::make(Send)]
pub trait Command {
    // The type of entity for this command.
    type Entity: Entity;

    /// The type for rejecting this command.
    type Rejection: Debug;

    /// The command handler, returning either to be persisted and applied events or a rejection.
    async fn handle(
        self,
        id: &<Self::Entity as Entity>::Id,
        entity: &Self::Entity,
    ) -> Result<Vec<<Self::Entity as Entity>::Event>, Self::Rejection>;
}

/// State and event handling for an [EventSourcedEntity].
pub trait Entity {
    /// The type of IDs.
    type Id: Debug
        + Display
        + for<'q> Encode<'q, Postgres>
        + Type<Postgres>
        + Serialize
        + for<'de> Deserialize<'de>
        + Sync;

    /// The type of events.
    type Event: Debug + Serialize + for<'de> Deserialize<'de> + Send + Sync;

    /// The type name.
    const TYPE_NAME: &'static str;

    /// The event handler, updating the state (self) for the given event.
    fn handle_event(&mut self, event: Self::Event);
}

/// Extension methods for [Entity] implementations.
#[allow(async_fn_in_trait)]
pub trait EntityExt
where
    Self: Entity + Sized,
{
    /// Turn an [Entity] implementation into a builder for an [EventSourcedEntity], thereby setting
    /// the [NoOpEventListener].
    fn entity(self) -> EventSourcedEntityBuilder<Self, NoOpEventListener> {
        EventSourcedEntityBuilder {
            entity: self,
            listener: None,
        }
    }
}

impl<E> EntityExt for E where E: Entity {}

/// Builder for an [EventSourcedEntity].
pub struct EventSourcedEntityBuilder<E, L> {
    entity: E,
    listener: Option<L>,
}

impl<E, L> EventSourcedEntityBuilder<E, L>
where
    E: Entity,
{
    /// Set the given [EventListener].
    pub fn with_listener<T>(self, listener: T) -> EventSourcedEntityBuilder<E, T> {
        EventSourcedEntityBuilder {
            entity: self.entity,
            listener: Some(listener),
        }
    }

    /// Build the [EventSourcedEntity] by loading and applying the current events.
    pub async fn build(self, id: E::Id, pool: Pool) -> Result<EventSourcedEntity<E, L>, Error> {
        let events = current_events_by_id::<E>(&id, &pool).await;
        let entity = events
            .try_fold(self.entity, |mut state, (_, event)| {
                state.handle_event(event);
                ok(state)
            })
            .await?;

        Ok(EventSourcedEntity {
            entity,
            id,
            last_seq_no: None,
            pool,
            listener: self.listener,
        })
    }
}

/// An event-sourced entity, ready to handle commands.
pub struct EventSourcedEntity<E, L>
where
    E: Entity,
{
    entity: E,
    listener: Option<L>,
    id: E::Id,
    pool: Pool,
    last_seq_no: Option<NonZeroU64>,
}

impl<E, L> EventSourcedEntity<E, L>
where
    E: Entity,
    L: EventListener<E::Event>,
{
    /// Handle the given command and transactionally persist all events returned from the command
    /// handler if not rejected.
    pub async fn handle_command<C>(&mut self, command: C) -> Result<Result<&E, C::Rejection>, Error>
    where
        C: Command<Entity = E>,
    {
        match command.handle(&self.id, &self.entity).await {
            Ok(events) => {
                if !events.is_empty() {
                    let seq_no = persist::<E, _>(
                        &self.id,
                        self.last_seq_no,
                        &events,
                        &self.pool,
                        &mut self.listener,
                    )
                    .await?;
                    self.last_seq_no = Some(seq_no);

                    for event in events {
                        self.entity.handle_event(event);
                    }
                }

                Ok(Ok(&self.entity))
            }

            Err(rejection) => Ok(Err(rejection)),
        }
    }
}

/// Invoked for each event during the transaction persisting the events returned from the command
/// handler.
#[trait_variant::make(Send)]
pub trait EventListener<E> {
    async fn listen(
        &mut self,
        event: &E,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), BoxError>
    where
        E: Sync;
}

/// A no-op [EventListener].
pub struct NoOpEventListener;

impl<E> EventListener<E> for NoOpEventListener {
    async fn listen(
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

/// Errors due to persistence, (de)serialization, etc.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Sqlx(String, #[source] sqlx::Error),

    #[error("cannot serialize event")]
    Ser(#[source] serde_json::Error),

    #[error("cannot deserialize event")]
    De(#[source] serde_json::Error),

    #[error("expected sequence number {0:?}, but was {1:?}")]
    UnexpectedSeqNo(Option<NonZeroU64>, Option<NonZeroU64>),

    #[error("listener error")]
    Listener(#[source] BoxError),
}

#[instrument(skip(pool))]
async fn current_events_by_id<'i, E>(
    id: &'i E::Id,
    pool: &Pool,
) -> impl Stream<Item = Result<(NonZeroU64, E::Event), Error>> + Send + 'i
where
    E: Entity,
{
    sqlx::query("SELECT seq_no, event FROM event WHERE id = $1")
        .bind(id)
        .fetch(&**pool)
        .map_err(|error| Error::Sqlx("cannot get next event".to_string(), error))
        .map(|row| {
            row.and_then(|row| {
                let seq_no = (row.get::<i64, _>(0) as u64)
                    .try_into()
                    .expect("sequence number greater zero");
                let bytes = row.get::<&[u8], _>(1);
                let event = serde_json::from_slice::<E::Event>(bytes).map_err(Error::De)?;
                Ok((seq_no, event))
            })
        })
}

#[instrument(skip(events, listener))]
async fn persist<E, L>(
    id: &E::Id,
    last_seq_no: Option<NonZeroU64>,
    events: &[E::Event],
    pool: &Pool,
    listener: &mut Option<L>,
) -> Result<NonZeroU64, Error>
where
    E: Entity,
    L: EventListener<E::Event>,
{
    assert!(!events.is_empty());

    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::Sqlx("cannot begin transaction".to_string(), error))?;

    let seq_no = sqlx::query("SELECT MAX(seq_no) FROM event WHERE id = $1")
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| Error::Sqlx("cannot select max seq_no".to_string(), error))
        .map(into_seq_no)?;

    if seq_no != last_seq_no {
        return Err(Error::UnexpectedSeqNo(seq_no, last_seq_no));
    }

    let mut seq_no = last_seq_no.map(|n| n.get() as i64).unwrap_or_default();
    for event in events.iter() {
        seq_no += 1;
        let bytes = serde_json::to_vec(event).map_err(Error::Ser)?;
        sqlx::query("INSERT INTO event VALUES ($1, $2, $3, $4)")
            .bind(id)
            .bind(seq_no)
            .bind(E::TYPE_NAME)
            .bind(&bytes)
            .execute(&mut *tx)
            .await
            .map_err(|error| Error::Sqlx("cannot execute statement".to_string(), error))?;

        if let Some(listener) = listener {
            listener
                .listen(event, &mut tx)
                .await
                .map_err(Error::Listener)?;
        }
    }

    tx.commit()
        .await
        .map_err(|error| Error::Sqlx("cannot commit transaction".to_string(), error))?;

    let seq_no = (seq_no as u64)
        .try_into()
        .expect("sequence number greater zero");
    Ok(seq_no)
}

fn into_seq_no(row: PgRow) -> Option<NonZeroU64> {
    // If there is no seq_no there is one row with a NULL column, hence use `try_get`.
    row.try_get::<i64, _>(0).ok().map(|seq_no| {
        (seq_no as u64)
            .try_into()
            .expect("sequence number greater zero")
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        pool::{Config, Pool},
        Command, Entity, EntityExt, EventListener,
    };
    use error_ext::BoxError;
    use serde::{Deserialize, Serialize};
    use sqlx::{postgres::PgSslMode, Executor, Row, Transaction};
    use std::error::Error as StdError;
    use testcontainers::{runners::AsyncRunner, RunnableImage};
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    type TestResult = Result<(), Box<dyn StdError>>;

    #[derive(Debug, Default, PartialEq, Eq)]
    pub struct Counter(u64);

    impl Entity for Counter {
        type Id = Uuid;
        type Event = Event;

        const TYPE_NAME: &'static str = "counter";

        fn handle_event(&mut self, event: Self::Event) {
            match event {
                Event::Increased { inc, .. } => self.0 += inc,
                Event::Decreased { dec, .. } => self.0 -= dec,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum Event {
        Increased { id: Uuid, inc: u64 },
        Decreased { id: Uuid, dec: u64 },
    }

    #[derive(Debug)]
    pub struct Increase(u64);

    impl Command for Increase {
        type Entity = Counter;
        type Rejection = Overflow;

        async fn handle(
            self,
            id: &<Self::Entity as Entity>::Id,
            entity: &Self::Entity,
        ) -> Result<Vec<<Self::Entity as Entity>::Event>, Self::Rejection> {
            let Increase(inc) = self;
            if entity.0 > u64::MAX - inc {
                Err(Overflow)
            } else {
                Ok(vec![Event::Increased { id: *id, inc }])
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct Overflow;

    #[derive(Debug)]
    pub struct Decrease(u64);

    impl Command for Decrease {
        type Entity = Counter;
        type Rejection = Underflow;

        async fn handle(
            self,
            id: &<Self::Entity as Entity>::Id,
            entity: &Self::Entity,
        ) -> Result<Vec<<Self::Entity as Entity>::Event>, Self::Rejection> {
            let Decrease(dec) = self;
            if entity.0 < dec {
                Err(Underflow)
            } else {
                Ok(vec![Event::Decreased { id: *id, dec }])
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct Underflow;

    struct Listener;

    impl EventListener<Event> for Listener {
        async fn listen(
            &mut self,
            event: &Event,
            tx: &mut Transaction<'_, sqlx::Postgres>,
        ) -> Result<(), BoxError> {
            match event {
                Event::Increased { id, inc } => {
                    let value = sqlx::query("SELECT value FROM counters WHERE id = $1")
                        .bind(id)
                        .fetch_optional(&mut **tx)
                        .await
                        .map_err(Box::new)?
                        .map(|row| row.try_get::<i64, _>(0))
                        .transpose()?;
                    match value {
                        Some(value) => {
                            sqlx::query("UPDATE counters SET value = $1 WHERE id = $2")
                                .bind(value + *inc as i64)
                                .bind(id)
                                .execute(&mut **tx)
                                .await
                                .map_err(Box::new)?;
                        }

                        None => {
                            sqlx::query("INSERT INTO counters VALUES ($1, $2)")
                                .bind(id)
                                .bind(*inc as i64)
                                .execute(&mut **tx)
                                .await
                                .map_err(Box::new)?;
                        }
                    }
                    Ok(())
                }

                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn test_load() -> TestResult {
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

        let pool = Pool::new(config).await.expect("pool can be created");
        let ddl = include_str!("../sql/create_event_log_uuid.sql");
        (&*pool).execute(ddl).await.unwrap();

        let id = Uuid::from_u128(0);
        sqlx::query("INSERT INTO event VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(1_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event::Increased { id, inc: 40 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO event VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(2_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event::Decreased { id, dec: 20 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO event VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(3_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event::Increased { id, inc: 22 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();

        let counter = Counter::default().entity().build(id, pool).await.unwrap();
        assert_eq!(counter.entity.0, 42);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_command() -> TestResult {
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

        let pool = Pool::new(config).await.expect("pool can be created");
        let ddl = include_str!("../sql/create_event_log_uuid.sql");
        (&*pool).execute(ddl).await.unwrap();

        let id = Uuid::from_u128(0);

        let mut counter = Counter::default().entity().build(id, pool).await.unwrap();
        assert_eq!(counter.entity, Counter(0));

        let result = counter.handle_command(Decrease(1)).await.unwrap();
        assert_eq!(result, Err(Underflow));

        let result = counter.handle_command(Increase(40)).await.unwrap();
        assert_eq!(result, Ok(&Counter(40)));
        let result = counter.handle_command(Decrease(20)).await.unwrap();
        assert_eq!(result, Ok(&Counter(20)));
        let result = counter.handle_command(Increase(22)).await.unwrap();
        assert_eq!(result, Ok(&Counter(42)));

        Ok(())
    }

    #[tokio::test]
    async fn test_event_listener() -> TestResult {
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

        let pool = Pool::new(config).await.expect("pool can be created");
        let ddl = include_str!("../sql/create_event_log_uuid.sql");
        (&*pool).execute(ddl).await.unwrap();

        let ddl = "CREATE TABLE IF NOT EXISTS counters (id uuid, value bigint, PRIMARY KEY (id));";
        (&*pool).execute(ddl).await.unwrap();

        let id_0 = Uuid::from_u128(0);
        let id_1 = Uuid::from_u128(1);
        let id_2 = Uuid::from_u128(2);

        let _ = Counter::default()
            .entity()
            .with_listener(Listener)
            .build(id_0, pool.clone())
            .await
            .unwrap();
        let mut counter_1 = Counter::default()
            .entity()
            .with_listener(Listener)
            .build(id_1, pool.clone())
            .await
            .unwrap();
        let mut counter_2 = Counter::default()
            .entity()
            .with_listener(Listener)
            .build(id_2, pool.clone())
            .await
            .unwrap();

        let _ = counter_1.handle_command(Increase(1)).await.unwrap();
        let _ = counter_2.handle_command(Increase(1)).await.unwrap();
        let _ = counter_2.handle_command(Increase(1)).await.unwrap();

        let value = sqlx::query("SELECT value FROM counters WHERE id = $1")
            .bind(id_0)
            .fetch_optional(&*pool)
            .await
            .unwrap()
            .map(|row| row.get::<i64, _>(0));
        assert!(value.is_none());

        let value = sqlx::query("SELECT value FROM counters WHERE id = $1")
            .bind(id_1)
            .fetch_optional(&*pool)
            .await
            .unwrap()
            .map(|row| row.get::<i64, _>(0));
        assert_eq!(value, Some(1));

        let value = sqlx::query("SELECT value FROM counters WHERE id = $1")
            .bind(id_2)
            .fetch_optional(&*pool)
            .await
            .unwrap()
            .map(|row| row.get::<i64, _>(0));
        assert_eq!(value, Some(2));

        Ok(())
    }
}
