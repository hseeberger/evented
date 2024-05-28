# evented

[![Crates.io][crates-badge]][crates-url]
[![license][license-badge]][license-url]

[crates-badge]: https://img.shields.io/crates/v/evented
[crates-url]: https://crates.io/crates/evented
[license-badge]: https://img.shields.io/github/license/hseeberger/evented
[license-url]: https://github.com/hseeberger/evented/blob/main/LICENSE

evented is a library for [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) where state changes are persisted as events. It originated from [eventsourced](https://github.com/hseeberger/eventsourced), but offers additional strong consistency features by tightly coupling to [PostgreSQL](https://www.postgresql.org/) as well as other features like event metadata.

The core abstraction of evented is an `EventSourcedEntity` which can be identified via an ID: an `Entity` implementation defines its state and event handling and associated `Command` implementations define its command handling.

When an event-sourced entity receives a command, the respective command handler is called, which either returns a sequence of to be persisted events plus metadata or a rejection. If events are returned, these are transactionally persisted, thereby also invoking an optional `EventListener`. Concurrency is handled by optimistic locking via per event sequence numbers.

When creating an event-sourced entity, its events are loaded and its state is conctructed by applying them to its event handler. This state is then used by the command handlers to decide whether a command should be accepted – resulting in events to be persisted and applied – or rejected.

The following shows a simple example from the tests which uses an event listener to build a synchronous and consistent view. Actually this view need not be consistent, but real-world use cases for consistent views include uniqueness checks, e.g. email addresses for user entities.

```rust
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
```

## License ##

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
