use crate::pool::Pool;
use error_ext::BoxError;
use futures::{future::ok, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

    /// The command handler, returning either to be persisted and applied events and metadata or a
    /// rejection.
    async fn handle(
        self,
        id: &<Self::Entity as Entity>::Id,
        entity: &Self::Entity,
    ) -> Result<
        Vec<
            impl Into<
                EventWithMetadata<
                    <Self::Entity as Entity>::Event,
                    <Self::Entity as Entity>::Metadata,
                >,
            >,
        >,
        Self::Rejection,
    >;
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
    type Event: Debug + Serialize + for<'de> Deserialize<'de> + Sync;

    /// Tye type of event metadata.
    type Metadata: Debug + Serialize + Sync;

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

/// An event with metadata, as returned by the command handler.
#[derive(Debug)]
pub struct EventWithMetadata<E, M> {
    pub event: E,
    pub metadata: M,
}

impl<E> From<E> for EventWithMetadata<E, ()> {
    fn from(event: E) -> Self {
        EventWithMetadata {
            event,
            metadata: (),
        }
    }
}

/// Extension methods for events.
pub trait EventExt
where
    Self: Sized,
{
    /// Convert a plain event into an [EventWithMetadata].
    fn with_metadata<M>(self, metadata: M) -> EventWithMetadata<Self, M> {
        EventWithMetadata {
            event: self,
            metadata,
        }
    }
}

impl<E> EventExt for E {}

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
        let (last_version, entity) = events
            .try_fold((None, self.entity), |(_, mut state), (version, event)| {
                state.handle_event(event);
                ok((Some(version), state))
            })
            .await?;

        Ok(EventSourcedEntity {
            entity,
            id,
            last_version,
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
    last_version: Option<NonZeroU64>,
}

impl<E, L> EventSourcedEntity<E, L>
where
    E: Entity,
    L: EventListener<E::Event, E::Metadata>,
{
    /// Handle the given command and transactionally persist all events returned from the command
    /// handler if not rejected.
    pub async fn handle_command<C>(&mut self, command: C) -> Result<Result<&E, C::Rejection>, Error>
    where
        C: Command<Entity = E>,
    {
        let result = command.handle(&self.id, &self.entity).await.map(|es| {
            es.into_iter()
                .map(|into_ewm| into_ewm.into())
                .collect::<Vec<_>>()
        });
        match result {
            Ok(ewms) => {
                if !ewms.is_empty() {
                    let version = persist::<E, _>(
                        &self.id,
                        self.last_version,
                        &ewms,
                        &self.pool,
                        &mut self.listener,
                    )
                    .await?;
                    self.last_version = Some(version);

                    for EventWithMetadata { event, .. } in ewms {
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
pub trait EventListener<E, M> {
    async fn listen(
        &mut self,
        ewm: &EventWithMetadata<E, M>,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), BoxError>
    where
        E: Sync,
        M: Sync;
}

/// A no-op [EventListener].
pub struct NoOpEventListener;

impl<E, M> EventListener<E, M> for NoOpEventListener {
    async fn listen(
        &mut self,
        _ewm: &EventWithMetadata<E, M>,
        _tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), BoxError>
    where
        E: Sync,
        M: Sync,
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

    #[error("expected version {0:?}, but was {1:?}")]
    UnexpectedVersion(Option<NonZeroU64>, Option<NonZeroU64>),

    #[error("listener error")]
    Listener(#[source] BoxError),
}

#[instrument(skip(pool))]
async fn current_events_by_id<'a, E>(
    id: &'a E::Id,
    pool: &'a Pool,
) -> impl Stream<Item = Result<(NonZeroU64, E::Event), Error>> + Send + 'a
where
    E: Entity,
{
    sqlx::query(
        "SELECT version, event
         FROM event
         WHERE entity_id = $1
         ORDER BY seq_no ASC",
    )
    .bind(id)
    .fetch(&**pool)
    .map_err(|error| Error::Sqlx("cannot get next event".to_string(), error))
    .map(|row| {
        row.and_then(|row| {
            let version = (row.get::<i64, _>(0) as u64)
                .try_into()
                .expect("version greater zero");
            let value = row.get::<Value, _>(1);
            let event = serde_json::from_value::<E::Event>(value).map_err(Error::De)?;
            Ok((version, event))
        })
    })
}

#[instrument(skip(ewms, listener))]
async fn persist<E, L>(
    id: &E::Id,
    last_version: Option<NonZeroU64>,
    ewms: &[EventWithMetadata<E::Event, E::Metadata>],
    pool: &Pool,
    listener: &mut Option<L>,
) -> Result<NonZeroU64, Error>
where
    E: Entity,
    L: EventListener<E::Event, E::Metadata>,
{
    assert!(!ewms.is_empty());

    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::Sqlx("cannot begin transaction".to_string(), error))?;

    let version = sqlx::query(
        "SELECT MAX(version)
         FROM event
         WHERE entity_id = $1",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| Error::Sqlx("cannot select max version".to_string(), error))
    .map(into_version)?;

    if version != last_version {
        return Err(Error::UnexpectedVersion(version, last_version));
    }

    let mut version = last_version.map(|n| n.get() as i64).unwrap_or_default();
    for ewm @ EventWithMetadata { event, metadata } in ewms.iter() {
        version += 1;
        let bytes = serde_json::to_value(event).map_err(Error::Ser)?;
        let metadata = serde_json::to_value(metadata).map_err(Error::Ser)?;
        sqlx::query(
            "INSERT INTO event (entity_id, version, type_name, event, metadata)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(version)
        .bind(E::TYPE_NAME)
        .bind(&bytes)
        .bind(metadata)
        .execute(&mut *tx)
        .await
        .map_err(|error| Error::Sqlx("cannot execute statement".to_string(), error))?;

        if let Some(listener) = listener {
            listener
                .listen(ewm, &mut tx)
                .await
                .map_err(Error::Listener)?;
        }
    }

    tx.commit()
        .await
        .map_err(|error| Error::Sqlx("cannot commit transaction".to_string(), error))?;

    let version = (version as u64).try_into().expect("version greater zero");
    Ok(version)
}

fn into_version(row: PgRow) -> Option<NonZeroU64> {
    // If there is no version there is one row with a NULL column, hence use `try_get`.
    row.try_get::<i64, _>(0)
        .ok()
        .map(|version| (version as u64).try_into().expect("version greater zero"))
}

#[cfg(test)]
mod tests {
    use crate::{
        entity::{Command, Entity, EntityExt, EventExt, EventListener, EventWithMetadata},
        pool::{Config, Pool},
    };
    use error_ext::BoxError;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use sqlx::{postgres::PgSslMode, Executor, Row, Transaction};
    use std::error::Error as StdError;
    use testcontainers::{runners::AsyncRunner, ContainerRequest, ImageExt};
    use testcontainers_modules::postgres::Postgres;
    use time::OffsetDateTime;
    use uuid::Uuid;

    type TestResult = Result<(), Box<dyn StdError>>;

    #[derive(Debug, Default, PartialEq, Eq)]
    pub struct Counter(u64);

    impl Entity for Counter {
        type Id = Uuid;
        type Event = Event;
        type Metadata = Metadata;

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
        ) -> Result<
            Vec<
                impl Into<
                    EventWithMetadata<
                        <Self::Entity as Entity>::Event,
                        <Self::Entity as Entity>::Metadata,
                    >,
                >,
            >,
            Self::Rejection,
        > {
            let Increase(inc) = self;
            if entity.0 > u64::MAX - inc {
                Err(Overflow)
            } else {
                let increased = Event::Increased { id: *id, inc };
                let metadata = Metadata {
                    timestamp: OffsetDateTime::now_utc(),
                };
                Ok(vec![increased.with_metadata(metadata)])
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
        ) -> Result<
            Vec<
                impl Into<
                    EventWithMetadata<
                        <Self::Entity as Entity>::Event,
                        <Self::Entity as Entity>::Metadata,
                    >,
                >,
            >,
            Self::Rejection,
        > {
            let Decrease(dec) = self;
            if entity.0 < dec {
                Err::<Vec<_>, Underflow>(Underflow)
            } else {
                let decreased = Event::Decreased { id: *id, dec };
                let metadata = Metadata {
                    timestamp: OffsetDateTime::now_utc(),
                };
                Ok(vec![decreased.with_metadata(metadata)])
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct Underflow;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Metadata {
        timestamp: OffsetDateTime,
    }

    struct Listener;

    impl EventListener<Event, Metadata> for Listener {
        async fn listen(
            &mut self,
            ewm: &EventWithMetadata<Event, Metadata>,
            tx: &mut Transaction<'_, sqlx::Postgres>,
        ) -> Result<(), BoxError> {
            match ewm {
                EventWithMetadata {
                    event: Event::Increased { id, inc },
                    ..
                } => {
                    let value = sqlx::query(
                        "SELECT value
                         FROM counters
                         WHERE id = $1",
                    )
                    .bind(id)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(Box::new)?
                    .map(|row| row.try_get::<i64, _>(0))
                    .transpose()?;
                    match value {
                        Some(value) => {
                            sqlx::query(
                                "UPDATE counters
                                         SET value = $1
                                         WHERE id = $2",
                            )
                            .bind(value + *inc as i64)
                            .bind(id)
                            .execute(&mut **tx)
                            .await
                            .map_err(Box::new)?;
                        }

                        None => {
                            sqlx::query(
                                "INSERT INTO counters
                                 VALUES ($1, $2)",
                            )
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
        let container = ContainerRequest::from(Postgres::default())
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

        let pool = Pool::new(config).await?;
        let ddl = include_str!("../sql/create_event_log_uuid.sql");
        (&*pool).execute(ddl).await?;

        let id = Uuid::from_u128(0);
        sqlx::query(
            "INSERT INTO event (entity_id, version, type_name, event, metadata)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&id)
        .bind(1_i64)
        .bind("test")
        .bind(serde_json::to_value(&Event::Increased { id, inc: 40 })?)
        .bind(Value::Null)
        .execute(&*pool)
        .await?;
        sqlx::query(
            "INSERT INTO event (entity_id, version, type_name, event, metadata)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&id)
        .bind(2_i64)
        .bind("test")
        .bind(serde_json::to_value(&Event::Decreased { id, dec: 20 })?)
        .bind(Value::Null)
        .execute(&*pool)
        .await?;
        sqlx::query(
            "INSERT INTO event (entity_id, version, type_name, event, metadata)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&id)
        .bind(3_i64)
        .bind("test")
        .bind(serde_json::to_value(&Event::Increased { id, inc: 22 })?)
        .bind(Value::Null)
        .execute(&*pool)
        .await?;

        let counter = Counter::default().entity().build(id, pool).await?;
        assert_eq!(counter.entity.0, 42);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_command() -> TestResult {
        let container = ContainerRequest::from(Postgres::default())
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
        (&*pool).execute(ddl).await?;

        let id = Uuid::from_u128(0);

        let mut counter = Counter::default().entity().build(id, pool.clone()).await?;
        assert_eq!(counter.entity, Counter(0));

        let result = counter.handle_command(Decrease(1)).await?;
        assert_eq!(result, Err(Underflow));

        let result = counter.handle_command(Increase(40)).await?;
        assert_eq!(result, Ok(&Counter(40)));

        let result = counter.handle_command(Decrease(20)).await?;
        assert_eq!(result, Ok(&Counter(20)));

        // reinstantiate the counter (to test that event recovery works)
        let mut counter = Counter::default().entity().build(id, pool).await?;
        let result = counter.handle_command(Increase(22)).await?;
        assert_eq!(result, Ok(&Counter(42)));

        Ok(())
    }

    #[tokio::test]
    async fn test_event_listener() -> TestResult {
        let container = ContainerRequest::from(Postgres::default())
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
        (&*pool).execute(ddl).await?;

        let ddl = "CREATE TABLE
                   IF NOT EXISTS
                   counters (id uuid, value bigint, PRIMARY KEY (id));";
        (&*pool).execute(ddl).await?;

        let id_0 = Uuid::from_u128(0);
        let id_1 = Uuid::from_u128(1);
        let id_2 = Uuid::from_u128(2);

        let _ = Counter::default()
            .entity()
            .with_listener(Listener)
            .build(id_0, pool.clone())
            .await?;
        let mut counter_1 = Counter::default()
            .entity()
            .with_listener(Listener)
            .build(id_1, pool.clone())
            .await?;
        let mut counter_2 = Counter::default()
            .entity()
            .with_listener(Listener)
            .build(id_2, pool.clone())
            .await?;

        let _ = counter_1.handle_command(Increase(1)).await?;
        let _ = counter_2.handle_command(Increase(1)).await?;
        let _ = counter_2.handle_command(Increase(1)).await?;

        let value = sqlx::query(
            "SELECT value
             FROM counters
             WHERE id = $1",
        )
        .bind(id_0)
        .fetch_optional(&*pool)
        .await?
        .map(|row| row.get::<i64, _>(0));
        assert!(value.is_none());

        let value = sqlx::query(
            "SELECT value
             FROM counters
             WHERE id = $1",
        )
        .bind(id_1)
        .fetch_optional(&*pool)
        .await?
        .map(|row| row.get::<i64, _>(0));
        assert_eq!(value, Some(1));

        let value = sqlx::query(
            "SELECT value
             FROM counters
             WHERE id = $1",
        )
        .bind(id_2)
        .fetch_optional(&*pool)
        .await?
        .map(|row| row.get::<i64, _>(0));
        assert_eq!(value, Some(2));

        Ok(())
    }
}
