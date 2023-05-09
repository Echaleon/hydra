use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::{
    routing::{get, post},
    Json, Router,
};
use openraft::raft::ClientWriteResponse;
use openraft::RaftMetrics;
use toy_rpc::macros::export_impl;

use crate::raft;
use crate::raft::{Node, NodeId, Raft, TypeConfig};

// Build the cluster management API router.
pub fn api(raft: Arc<raft::Raft>, handle: axum_server::Handle) -> Router {
    // Build the gRPC service
    let cluster_server = toy_rpc::Server::builder()
        .register(Arc::new(Cluster::new(raft.clone(), handle)))
        .build();

    Router::new()
        .route("/initialize", post(initialize))
        .route("/add_learner", post(add_learner))
        .route("/change_membership", post(change_membership))
        .route("/metrics", get(get_metrics))
        .with_state(raft)
        .nest_service("/rpc", cluster_server.handle_http())
}

// ------ Handlers for the cluster management API ------ //
// Initializes the cluster
pub async fn initialize(
    State(raft): State<Arc<Raft>>,
    Json(nodes): Json<Option<BTreeMap<NodeId, Node>>>,
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

// ------ gRPC service for the cluster API ------ //
#[derive(Clone)]
pub struct Cluster {
    raft: Arc<Raft>,
    handle: axum_server::Handle,
}

#[export_impl]
impl Cluster {
    fn new(raft: Arc<Raft>, handle: axum_server::Handle) -> Self {
        Self { raft, handle }
    }

    #[export_method]
    pub async fn initialize(
        &self,
        nodes: Option<BTreeMap<NodeId, Node>>,
    ) -> Result<(), toy_rpc::Error> {
        let mut nodes = nodes.unwrap_or_default();
        nodes.insert(self.raft.node.id, self.raft.node.clone());
        self.raft
            .raft
            .initialize(nodes)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn add_learner(
        &self,
        node: Node,
    ) -> Result<ClientWriteResponse<TypeConfig>, toy_rpc::Error> {
        self.raft
            .raft
            .add_learner(node.id, node, true)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn change_membership(
        &self,
        nodes: BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse<TypeConfig>, toy_rpc::Error> {
        self.raft
            .raft
            .change_membership(nodes, false)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn metrics(
        &self,
        _: Option<u8>,
    ) -> Result<RaftMetrics<NodeId, Node>, toy_rpc::Error> {
        Ok(self.raft.raft.metrics().borrow().clone())
    }

    #[export_method]
    pub async fn shutdown(&self, _: Option<u8>) -> Result<(), toy_rpc::Error> {
        tracing::info!("Shutting down...");
        let _ = self.raft.raft.shutdown().await.map_err(|e| {
            tracing::error!("Failed to shutdown the Raft node: {}", e);
        });
        self.handle
            .graceful_shutdown(Option::from(std::time::Duration::from_secs(5)));
        Ok(())
    }
}
