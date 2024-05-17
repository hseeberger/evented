mod event_log;
mod pool;

use crate::event_log::{EventListener, EventLog, NoEventListener};
use futures::{future::ok, TryStreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{Encode, Postgres, Type};
use std::{fmt::Debug, num::NonZeroU64};

/// State and behavior – command and event handling – for an event-sourced entity.
pub trait Entity {
    /// The type of IDs.
    type Id: Debug + for<'q> Encode<'q, Postgres> + Type<Postgres> + Sync;

    /// The type of commands.
    type Command: Debug;

    /// The type of events.
    type Event: Debug + Serialize + for<'de> Deserialize<'de> + Send + Sync;

    /// The type of rejections.
    type Rejection: Debug;

    /// The type name.
    const TYPE_NAME: &'static str;

    /// The command handler, returning either to be persisted and applied events or a rejection.
    fn handle_command(
        &self,
        id: &Self::Id,
        command: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Rejection>;

    /// The event handler, updating self for the given event.
    fn handle_event(&mut self, event: Self::Event);
}

/// Builder for an event-sourced entity.
pub struct EventSourcedEntityBuilder<E, L> {
    entity: E,
    listener: Option<L>,
}

impl<E, L> EventSourcedEntityBuilder<E, L> {
    pub fn with_listener<T>(self, listener: T) -> EventSourcedEntityBuilder<E, T> {
        EventSourcedEntityBuilder {
            entity: self.entity,
            listener: Some(listener),
        }
    }

    /// Load the [EventSourcedEntity] for this [Entity].
    pub async fn build(
        self,
        id: E::Id,
        event_log: EventLog<E::Id>,
    ) -> Result<EventSourcedEntity<E, L>, event_log::Error>
    where
        E: Entity,
    {
        let events = event_log.current_events_by_id::<E::Event>(&id).await;
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
            event_log,
            listener: self.listener,
        })
    }
}

/// An event-sourced entity.
pub struct EventSourcedEntity<E, L>
where
    E: Entity,
{
    entity: E,
    listener: Option<L>,
    id: E::Id,
    event_log: EventLog<E::Id>,
    last_seq_no: Option<NonZeroU64>,
}

impl<E, L> EventSourcedEntity<E, L>
where
    E: Entity,
    L: EventListener,
{
    pub async fn handle_command(
        &mut self,
        command: E::Command,
    ) -> Result<Result<&E, E::Rejection>, event_log::Error> {
        match self.entity.handle_command(&self.id, command) {
            Ok(events) => {
                if !events.is_empty() {
                    let seq_no = self
                        .event_log
                        .persist(
                            &self.id,
                            self.last_seq_no,
                            E::TYPE_NAME,
                            &events,
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

/// Extension methods for [Entity] implementations.
#[allow(async_fn_in_trait)]
pub trait EventSourcedExt
where
    Self: Entity + Sized,
{
    /// Build an event-sourced [Entity].
    fn entity(self) -> EventSourcedEntityBuilder<Self, NoEventListener> {
        EventSourcedEntityBuilder {
            entity: self,
            listener: None,
        }
    }
}

impl<E> EventSourcedExt for E where E: Entity {}

#[cfg(test)]
mod tests {
    use crate::{
        event_log::EventLog,
        pool::{Config, Pool},
        Entity, EventSourcedExt,
    };
    use serde::{Deserialize, Serialize};
    use sqlx::postgres::PgSslMode;
    use testcontainers::{runners::AsyncRunner, RunnableImage};
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    #[derive(Debug, Default, PartialEq, Eq)]
    struct Counter(u64);

    impl Entity for Counter {
        type Id = Uuid;
        type Command = Command;
        type Event = Event;
        type Rejection = Rejection;

        const TYPE_NAME: &'static str = "counter";

        fn handle_command(
            &self,
            id: &Self::Id,
            command: Self::Command,
        ) -> Result<Vec<Self::Event>, Self::Rejection> {
            match command {
                Command::Increase(inc) if self.0 > u64::MAX - inc => Err(Rejection::Overflow),
                Command::Increase(inc) => Ok(vec![Event::Increased { id: *id, inc }]),

                Command::Decrease(dec) if self.0 < dec => Err(Rejection::Underflow),
                Command::Decrease(dec) => Ok(vec![Event::Decreased { id: *id, dec }]),
            }
        }

        fn handle_event(&mut self, event: Self::Event) {
            match event {
                Event::Increased { inc, .. } => self.0 += inc,
                Event::Decreased { dec, .. } => self.0 -= dec,
            }
        }
    }

    #[derive(Debug)]
    enum Command {
        Increase(u64),
        Decrease(u64),
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Rejection {
        Overflow,
        Underflow,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum Event {
        Increased { id: Uuid, inc: u64 },
        Decreased { id: Uuid, dec: u64 },
    }

    #[tokio::test]
    async fn test_load() {
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
        let ddl = include_str!("create_event_log_uuid.sql");
        let event_log = EventLog::<Uuid>::new(pool.clone(), Some(ddl))
            .await
            .unwrap();

        let id = Uuid::from_u128(0);
        sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(1_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event::Increased { id, inc: 40 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(2_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event::Decreased { id, dec: 20 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(3_i64)
            .bind("type")
            .bind(serde_json::to_vec(&Event::Increased { id, inc: 22 }).unwrap())
            .execute(&*pool)
            .await
            .unwrap();

        let counter = Counter::default()
            .entity()
            .build(id, event_log)
            .await
            .unwrap();
        assert_eq!(counter.entity.0, 42);
    }

    #[tokio::test]
    async fn test() {
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
        let ddl = include_str!("create_event_log_uuid.sql");
        let event_log = EventLog::<Uuid>::new(pool.clone(), Some(ddl))
            .await
            .unwrap();

        let id = Uuid::from_u128(0);

        let mut counter = Counter::default()
            .entity()
            .build(id, event_log)
            .await
            .unwrap();
        assert_eq!(counter.entity, Counter(0));

        let result = counter.handle_command(Command::Decrease(1)).await.unwrap();
        assert_eq!(result, Err(Rejection::Underflow));

        let result = counter.handle_command(Command::Increase(40)).await.unwrap();
        assert_eq!(result, Ok(&Counter(40)));
        let result = counter.handle_command(Command::Decrease(20)).await.unwrap();
        assert_eq!(result, Ok(&Counter(20)));
        let result = counter.handle_command(Command::Increase(22)).await.unwrap();
        assert_eq!(result, Ok(&Counter(42)));
    }
}
