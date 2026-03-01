#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use sayiir_core::context::WorkflowContext;
use sayiir_postgres::PostgresBackend;
use sayiir_runtime::serialization::JsonCodec;
use sqlx::PgPool;
use std::sync::Arc;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Spin up a Postgres container and return a connected backend.
///
/// Returns the container handle (must be kept alive), the backend, and the
/// connection URL.
pub async fn setup() -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresBackend<JsonCodec>,
    String,
) {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.unwrap();
    let backend = PostgresBackend::<JsonCodec>::connect_with(pool)
        .await
        .unwrap();
    (container, backend, url)
}

pub fn ctx() -> WorkflowContext<JsonCodec, ()> {
    WorkflowContext::new("test-wf", Arc::new(JsonCodec), Arc::new(()))
}
