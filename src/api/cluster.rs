use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::{
    extract,
    routing::{get, post},
    Json, Router,
};
use openraft::raft::ClientWriteResponse;
use openraft::RaftMetrics;

use crate::raft;
use crate::raft::common::{Node, NodeId, TypeConfig};
use crate::raft::Raft;

// Build the cluster management API router.
pub fn api(raft: Arc<raft::Raft>) -> Router {
    Router::new()
        .route("/initialize", post(initialize))
        .route("/add_learner", post(add_learner))
        .route("/change_membership", post(change_membership))
        .route("/metrics", get(get_metrics))
        .with_state(raft)
}

// Handlers for the cluster management API.
// Initializes the cluster
pub async fn initialize(
    State(raft): State<Arc<Raft>>,
    Json(nodes): extract::Json<Option<BTreeMap<NodeId, Node>>>,
) -> Result<(), StatusCode> {
    let mut nodes = nodes.unwrap_or_default();
    nodes.insert(raft.node.id, raft.node.clone());
    raft.raft
        .initialize(nodes)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// Adds a learner node to the cluster
async fn add_learner(
    State(raft): State<Arc<Raft>>,
    Json(node): Json<Node>,
) -> Result<Json<ClientWriteResponse<TypeConfig>>, StatusCode> {
    Ok(Json(
        raft.raft
            .add_learner(node.id, node, true)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

// Changes the membership of the cluster
async fn change_membership(
    State(raft): State<Arc<Raft>>,
    Json(nodes): Json<BTreeSet<NodeId>>,
) -> Result<Json<ClientWriteResponse<TypeConfig>>, StatusCode> {
    Ok(Json(
        raft.raft
            .change_membership(nodes, false)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

// Gets the metrics of the cluster
async fn get_metrics(State(raft): State<Arc<Raft>>) -> Json<RaftMetrics<NodeId, Node>> {
    Json(raft.raft.metrics().borrow().clone())
}
