pub mod entity;
pub mod pool;
pub mod projection;

#[cfg(test)]
mod test {
    use testcontainers::{
        core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, Image, ImageExt,
        TestcontainersError,
    };

    pub async fn run_postgres() -> Result<ContainerAsync<impl Image>, TestcontainersError> {
        GenericImage::new("postgres", "16-alpine")
            // the existing wait condition in the predefined Postgres module is not good enough
            // see https://github.com/testcontainers/testcontainers-rs/issues/674
            // and https://github.com/hseeberger/evented/issues/42
            .with_wait_for(WaitFor::message_on_stderr("port 5432"))
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_DB", "postgres")
            .start()
            .await
    }
}
