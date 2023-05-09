use std::sync::Arc;
use axum::Router;

use crate::raft;

pub mod cluster;
pub mod database;

// Build the external API router, which can be accessed over HTTP or gRPC.
pub fn api(raft: Arc<raft::Raft>, handle: axum_server::Handle) -> Router {
    Router::new()
        .nest("/cluster", cluster::api(raft.clone(), handle))
        .nest("/keys/", database::api(raft))
}
