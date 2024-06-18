use crate::pool::Pool;
use async_stream::stream;
use error_ext::{BoxError, StdErrorExt};
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};
use std::{
    error::Error as StdError, fmt::Debug, num::NonZeroU64, pin::pin, sync::Arc, time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task,
    time::sleep,
};
use tracing::{debug, error, info};

/// A projection of events of an event sourced entity to a Postgres database.
#[derive(Debug, Clone)]
pub struct Projection {
    name: String,
    command_in: mpsc::Sender<(Command, oneshot::Sender<State>)>,
}

impl Projection {
    pub async fn by_type_name<E, H>(
        type_name: &'static str,
        name: String,
        event_handler: H,
        error_strategy: ErrorStrategy,
        pool: Pool,
    ) -> Self
    where
        E: for<'de> Deserialize<'de> + Send,
        H: EventHandler<E> + Clone + Sync + 'static,
    {
        let state = Arc::new(RwLock::new(State {
            seq_no: None,
            running: false,
            error: None,
        }));

        let (command_in, mut command_out) = mpsc::channel::<(Command, oneshot::Sender<State>)>(1);

        task::spawn({
            let name = name.clone();
            let state = state.clone();

            async move {
                while let Some((command, reply_in)) = command_out.recv().await {
                    match command {
                        Command::Run => {
                            // Do not remove braces, dead-lock is waiting for you!
                            let running = { state.read().await.running };
                            if running {
                                info!(type_name, name, "projection already running");
                            } else {
                                info!(type_name, name, "running projection");

                                // Do not remove braces, dead-lock is waiting for you!
                                {
                                    let mut state = state.write().await;
                                    state.running = true;
                                    state.error = None;
                                }

                                run_projection::<E, H>(
                                    type_name,
                                    name.clone(),
                                    state.clone(),
                                    event_handler.clone(),
                                    pool.clone(),
                                    error_strategy,
                                )
                                .await;
                            }

                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }

                        Command::Stop => {
                            // Do not remove braces, dead-lock is waiting for you!
                            let running = { state.read().await.running };
                            if running {
                                info!(type_name, name, "stopping projection");
                                {
                                    let mut state = state.write().await;
                                    state.running = false;
                                }
                            } else {
                                info!(type_name, name, "projection already stopped");
                            }

                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }

                        Command::GetState => {
                            if reply_in.send(state.read().await.clone()).is_err() {
                                error!(type_name, name, "cannot send state");
                            }
                        }
                    }
                }
            }
        });

        Projection { name, command_in }
    }

    pub async fn run(&self) -> Result<State, CommandError> {
        self.dispatch_command(Command::Run).await
    }

    pub async fn stop(&self) -> Result<State, CommandError> {
        self.dispatch_command(Command::Stop).await
    }

    pub async fn get_state(&self) -> Result<State, CommandError> {
        self.dispatch_command(Command::GetState).await
    }

    async fn dispatch_command(&self, command: Command) -> Result<State, CommandError> {
        let (reply_in, reply_out) = oneshot::channel();
        self.command_in
            .send((command, reply_in))
            .await
            .map_err(|_| CommandError::SendCommand(command, self.name.clone()))?;
        let state = reply_out
            .await
            .map_err(|_| CommandError::ReceiveResponse(command, self.name.clone()))?;
        Ok(state)
    }
}

#[trait_variant::make(Send)]
pub trait EventHandler<E> {
    type Error: StdError + Send + Sync + 'static;

    async fn handle_event(
        &self,
        event: E,
        tx: &mut Transaction<'static, Postgres>,
    ) -> Result<(), Self::Error>;
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum CommandError {
    /// The command cannot be sent to the spawned projection.
    #[error("cannot send command {0:?} to spawned projection {1}")]
    SendCommand(Command, String),

    /// A reply for the command cannot be received from the spawned projection.
    #[error("cannot receive reply for command {0:?} from spawned projection {1}")]
    ReceiveResponse(Command, String),
}

#[derive(Debug, Clone, Copy)]
pub enum ErrorStrategy {
    Retry(Duration),
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub seq_no: Option<NonZeroU64>,
    pub running: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Command {
    Run,
    Stop,
    GetState,
}

#[derive(Debug, Error)]
enum RunError {
    #[error("{0}")]
    Sqlx(String, #[source] sqlx::Error),

    #[error("cannot deserialize event")]
    De(#[source] serde_json::Error),

    #[error(transparent)]
    Handler(BoxError),
}

async fn run_projection<E, H>(
    type_name: &'static str,
    name: String,
    state: Arc<RwLock<State>>,
    event_handler: H,
    pool: Pool,
    error_strategy: ErrorStrategy,
) where
    E: for<'de> Deserialize<'de> + Send,
    H: EventHandler<E> + Sync + 'static,
{
    task::spawn({
        async move {
            loop {
                let result =
                    start_projection::<E, H>(type_name, &name, &event_handler, &pool, &state).await;
                match result {
                    Ok(_) => {
                        info!(type_name, name, "projection stopped");
                        {
                            let mut state = state.write().await;
                            state.running = false;
                        }
                        break;
                    }

                    Err(error) => {
                        error!(
                            error = error.as_chain(),
                            type_name, name, "projection error"
                        );

                        match error_strategy {
                            ErrorStrategy::Retry(delay) => {
                                info!(type_name, name, ?delay, "projection retrying after error");
                                {
                                    let mut state = state.write().await;
                                    state.error = Some(error.to_string());
                                }
                                sleep(delay).await
                            }

                            ErrorStrategy::Stop => {
                                info!(type_name, name, "projection stopped after error");
                                {
                                    let mut state = state.write().await;
                                    state.running = false;
                                    state.error = Some(error.to_string());
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    });
}

async fn start_projection<E, H>(
    type_name: &'static str,
    name: &str,
    handler: &H,
    pool: &Pool,
    state: &Arc<RwLock<State>>,
) -> Result<(), RunError>
where
    E: for<'de> Deserialize<'de> + Send,
    H: EventHandler<E>,
{
    debug!(type_name, name, "starting projection");

    // We add 1 here, so we can later query events starting with this seq_no.
    let seq_no = load_seq_no(name, pool)
        .await?
        .map(|n| n.saturating_add(1))
        .unwrap_or(1);
    let events = events_by_type::<E>(type_name, seq_no, Duration::from_secs(2), pool).await?;
    let mut events = pin!(events);

    while let Some(event) = events.next().await {
        if !state.read().await.running {
            break;
        };

        let (seq_no, event) = event?;

        let mut tx = pool
            .begin()
            .await
            .map_err(|error| RunError::Sqlx("cannot begin transaction".to_string(), error))?;
        handler
            .handle_event(event, &mut tx)
            .await
            .map_err(|error| RunError::Handler(error.into()))?;
        debug!(type_name, name, seq_no, "projection handled event");
        save_seq_no(seq_no, name, &mut tx).await?;
        tx.commit()
            .await
            .map_err(|error| RunError::Sqlx("cannot commit transaction".to_string(), error))?;

        state.write().await.seq_no = Some(seq_no);
    }

    Ok(())
}

async fn load_seq_no(name: &str, pool: &Pool) -> Result<Option<i64>, RunError> {
    let seq_no = sqlx::query(
        "SELECT seq_no
         FROM projection
         WHERE name=$1",
    )
    .bind(name)
    .fetch_optional(&**pool)
    .await
    .map_err(|error| RunError::Sqlx("cannot load seq_no".to_string(), error))?
    .map(|row| {
        row.try_get::<i64, _>("seq_no")
            .map_err(|error| RunError::Sqlx("cannot get seq_no from row".to_string(), error))
    })
    .transpose()?;
    Ok(seq_no)
}

async fn events_by_type<'a, E>(
    type_name: &'static str,
    seq_no: i64,
    poll_interval: Duration,
    pool: &'a Pool,
) -> Result<impl Stream<Item = Result<(NonZeroU64, E), RunError>> + Send + 'a, RunError>
where
    E: for<'de> Deserialize<'de> + Send + 'a,
{
    let last_seq_no = sqlx::query(
        "SELECT MAX(seq_no)
         FROM event
         WHERE type_name = $1",
    )
    .bind(type_name)
    .fetch_one(&**pool)
    .await
    .map_err(|error| RunError::Sqlx("cannot select max version".to_string(), error))
    .map(into_seq_no)?;

    debug!(last_seq_no, seq_no, "selected last seq_no");

    let mut current_seq_no = seq_no;
    let events = stream! {
        'outer: loop {
            let events =
                current_events_by_type::<E>(type_name, current_seq_no, pool)
                .await;

            for await event in events {
                match event {
                    Ok(event @ (seq_no, _)) => {
                        current_seq_no = seq_no.get() as i64 + 1;
                        yield Ok(event);
                    }

                    Err(error) => {
                        yield Err(error);
                        break 'outer;
                    }
                }
            }

            // Only sleep if requesting future events.
            if current_seq_no >= last_seq_no {
                sleep(poll_interval).await;
            }
        }
    };

    Ok(events)
}

async fn current_events_by_type<E>(
    type_name: &'static str,
    seq_no: i64,
    pool: &Pool,
) -> impl Stream<Item = Result<(NonZeroU64, E), RunError>> + Send
where
    E: for<'de> Deserialize<'de>,
{
    sqlx::query(
        "SELECT seq_no, event
         FROM event
         WHERE type_name = $1 AND seq_no >= $2
         ORDER BY seq_no ASC",
    )
    .bind(type_name)
    .bind(seq_no)
    .fetch(&**pool)
    .map_err(|error| RunError::Sqlx("cannot get next event".to_string(), error))
    .map(|row| {
        row.and_then(|row| {
            let seq_no = (row.get::<i64, _>(0) as u64)
                .try_into()
                .expect("seq_no greater zero");
            let value = row.get::<Value, _>(1);
            let event = serde_json::from_value::<E>(value).map_err(RunError::De)?;
            Ok((seq_no, event))
        })
    })
}

async fn save_seq_no(
    seq_no: NonZeroU64,
    name: &str,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), RunError> {
    let query = "INSERT INTO projection (name, seq_no)
                 VALUES ($1, $2)
                 ON CONFLICT (name) DO
                 UPDATE SET seq_no = $2";
    sqlx::query(query)
        .bind(name)
        .bind(seq_no.get() as i64)
        .execute(&mut **tx)
        .await
        .map_err(|error| RunError::Sqlx("cannot save sequence number".to_string(), error))?;
    Ok(())
}

fn into_seq_no(row: PgRow) -> i64 {
    // If there is no seq_no there is one row with a NULL column, hence use `try_get`.
    row.try_get::<i64, _>(0).ok().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use crate::{
        pool::{Config, Pool},
        projection::{ErrorStrategy, EventHandler, Projection, State},
    };
    use assert_matches::assert_matches;
    use error_ext::BoxError;
    use serde_json::Value;
    use sqlx::{postgres::PgSslMode, Executor, Postgres, QueryBuilder, Row, Transaction};
    use std::{iter::once, time::Duration};
    use testcontainers::{runners::AsyncRunner, ContainerRequest, ImageExt};
    use testcontainers_modules::postgres::Postgres as TCPostgres;
    use tokio::time::sleep;
    use tracing_test::traced_test;
    use uuid::Uuid;

    #[derive(Clone)]
    struct TestHandler;

    impl EventHandler<i32> for TestHandler {
        type Error = sqlx::Error;

        async fn handle_event(
            &self,
            event: i32,
            tx: &mut Transaction<'static, Postgres>,
        ) -> Result<(), Self::Error> {
            QueryBuilder::new("INSERT INTO test (n) ")
                .push_values(once(event), |mut q, event| {
                    q.push_bind(event);
                })
                .build()
                .execute(&mut **tx)
                .await?;
            Ok(())
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test() -> Result<(), BoxError> {
        let container = ContainerRequest::from(TCPostgres::default())
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
        let ddl = include_str!("../sql/create_projection.sql");
        (&*pool).execute(ddl).await?;
        sqlx::query("CREATE TABLE test (n bigint);")
            .execute(&*pool)
            .await?;

        let values = (1..=100).map(|n| (Uuid::nil(), n, "test", n, Value::Null));
        QueryBuilder::new("INSERT INTO event (entity_id, version, type_name, event, metadata)")
            .push_values(
                values,
                |mut q, (id, version, type_name, event, metadata)| {
                    let event = serde_json::to_value(event).unwrap();
                    q.push_bind(id)
                        .push_bind(version)
                        .push_bind(type_name)
                        .push_bind(event)
                        .push_bind(metadata);
                },
            )
            .build()
            .execute(&*pool)
            .await?;

        let projection = Projection::by_type_name(
            "test",
            "test-projection".to_string(),
            TestHandler,
            ErrorStrategy::Stop,
            pool.clone(),
        )
        .await;

        QueryBuilder::new("INSERT INTO projection ")
            .push_values(once(("test-projection", 10)), |mut q, (name, seq_no)| {
                q.push_bind(name).push_bind(seq_no);
            })
            .build()
            .execute(&*pool)
            .await?;

        projection.run().await?;

        let mut state = projection.get_state().await?;
        let max = Some(100.try_into()?);
        while state.seq_no < max {
            sleep(Duration::from_millis(100)).await;
            state = projection.get_state().await?;
        }
        assert_matches!(
            state,
            State {
                seq_no,
                running,
                error
            } if seq_no == max && running && error.is_none()
        );

        projection.stop().await?;
        sleep(Duration::from_millis(100)).await;
        let state = projection.get_state().await?;
        sleep(Duration::from_millis(100)).await;
        let mut state_2 = projection.get_state().await?;
        while state_2 != state {
            sleep(Duration::from_millis(100)).await;
            state_2 = projection.get_state().await?;
        }
        assert_matches!(
            state,
            State {
                seq_no,
                running,
                error
            } if seq_no == Some(100.try_into().unwrap()) && !running && error.is_none()
        );

        let sum = sqlx::query("SELECT * FROM test;")
            .fetch_all(&*pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<i64, _>(0))
            .try_fold(0i64, |acc, n| n.map(|n| acc + n))?;
        assert_eq!(sum, 4_995); // sum(1..100) - sum(1..10)

        projection.run().await?;
        let mut state = projection.get_state().await?;
        while !state.running {
            sleep(Duration::from_millis(100)).await;
            state = projection.get_state().await?;
        }
        sleep(Duration::from_millis(100)).await;

        let values = (101..=200).map(|n| (Uuid::nil(), n, "test", n, Value::Null));
        QueryBuilder::new("INSERT INTO event (entity_id, version, type_name, event, metadata)")
            .push_values(
                values,
                |mut q, (id, version, type_name, event, metadata)| {
                    let event = serde_json::to_value(event).unwrap();
                    q.push_bind(id)
                        .push_bind(version)
                        .push_bind(type_name)
                        .push_bind(event)
                        .push_bind(metadata);
                },
            )
            .build()
            .execute(&*pool)
            .await?;

        let mut state = projection.get_state().await?;
        let max = Some(200.try_into()?);
        while state.seq_no < max {
            sleep(Duration::from_millis(100)).await;
            state = projection.get_state().await?;
        }
        assert_matches!(
            state,
            State {
                seq_no,
                running,
                error
            } if seq_no == max && running && error.is_none()
        );

        projection.stop().await?;
        sleep(Duration::from_millis(100)).await;
        let state = projection.get_state().await?;
        sleep(Duration::from_millis(100)).await;
        let mut state_2 = projection.get_state().await?;
        while state_2 != state {
            sleep(Duration::from_millis(100)).await;
            state_2 = projection.get_state().await?;
        }
        assert_matches!(
            state,
            State {
                seq_no,
                running,
                error
            } if seq_no == Some(200.try_into().unwrap()) && !running && error.is_none()
        );

        let sum = sqlx::query("SELECT * FROM test")
            .fetch_all(&*pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<i64, _>(0))
            .try_fold(0i64, |acc, n| n.map(|n| acc + n))?;
        assert_eq!(sum, 20_045); // sum(1..200) - sum(1..10)

        Ok(())
    }
}
