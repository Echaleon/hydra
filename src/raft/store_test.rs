use std::future::Future;

use axum::async_trait;
use openraft::testing::StoreBuilder;

use crate::raft::store::{storage_read_error, StorageError, Store};
use crate::raft::TypeConfig;

struct Builder;

#[async_trait]
impl StoreBuilder<TypeConfig, Store> for Builder {
    async fn run_test<Fun, Ret, Res>(&self, t: Fun) -> Result<Ret, StorageError>
    where
        Res: Future<Output = Result<Ret, StorageError>> + Send,
        Fun: Fn(Store) -> Res + Sync + Send,
    {
        let config = kv::Config::new("test")
            .temporary(true)
            .use_compression(true);

        let kv = kv::Store::new(config).map_err(storage_read_error)?;

        let store = Store::new(kv).await;

        t(store).await
    }
}

#[test]
pub fn test_store() -> Result<(), StorageError> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(
        tracing_appender::rolling::daily("test-logs", "hydra"),
    );
    // Enable trace logging.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(non_blocking)
        .init();

    openraft::testing::Suite::test_all(Builder {})
}
