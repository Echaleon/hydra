use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{ErrorResponse, Redirect},
    routing::post,
    Json, Router,
};
use openraft::raft::ClientWriteResponse;
use serde::{Deserialize, Serialize};
use toy_rpc::macros::export_impl;

use crate::raft;
use crate::raft::{Node, Raft, Request, TypeConfig};

// Build the key value store API router.
pub fn api(raft: Arc<raft::Raft>) -> Router {
    // Build the gRPC service
    let key_server = toy_rpc::Server::builder()
        .register(Arc::new(KeyValueStore::new(raft.clone())))
        .build();

    Router::new()
        .route("/get", post(get_key))
        .route("/set", post(set_key))
        .route("/delete", post(delete_key))
        .with_state(raft)
        .nest_service("/rpc", key_server.handle_http())
}

// ------ Handlers for the key value store API ------ //
// Gets a key from the store
async fn get_key(
    State(raft): State<Arc<Raft>>,
    Json(key): Json<String>,
) -> axum::response::Result<Json<Option<String>>> {
    if let Err(e) = raft.raft.is_leader().await {
        if let Some(leader) = e.forward_to_leader() {
            if let Some(leader) = &leader.leader_node {
                tracing::debug!("Forwarding request to leader: {}", leader.id);
                Err(ErrorResponse::from(Redirect::temporary(&format!(
                    "http://{}/keys/get",
                    leader.addr
                ))))
            } else {
                tracing::error!("No leader found in cluster");
                Err(ErrorResponse::from(StatusCode::INTERNAL_SERVER_ERROR))
            }
        } else {
            tracing::error!("Error checking if this node is the leader: {:?}", e);
            Err(ErrorResponse::from(StatusCode::INTERNAL_SERVER_ERROR))
        }
    } else {
        Ok(Json(
            raft.store
                .sm
                .read()
                .await
                .get(&key)
                .map_err(|e| {
                    tracing::error!("Error reading from store: {:?}", e);
                    ErrorResponse::from(StatusCode::INTERNAL_SERVER_ERROR)
                })?
                .and_then(|v| if v.is_empty() { None } else { Some(v) }),
        ))
    }
}

// Sets a key in the store
async fn set_key(
    State(raft): State<Arc<Raft>>,
    Json((key, value)): Json<(String, String)>,
) -> Result<Json<ClientWriteResponse<TypeConfig>>, StatusCode> {
    Ok(Json(
        raft.raft
            .client_write(Request { key, value })
            .await
            .map_err(|e| {
                tracing::error!("Error writing to store: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?,
    ))
}

// Deletes a key from the store. In reality, this is a tombstone operation.
async fn delete_key(
    State(raft): State<Arc<Raft>>,
    Json(key): Json<String>,
) -> Result<Json<ClientWriteResponse<TypeConfig>>, StatusCode> {
    Ok(Json(
        raft.raft
            .client_write(Request {
                key,
                value: String::new(),
            })
            .await
            .map_err(|e| {
                tracing::error!("Error writing to store: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?,
    ))
}

// ------ gRPC API for the key value store API ------ //
#[derive(Clone)]
pub struct KeyValueStore {
    raft: Arc<raft::Raft>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetKeyResponse {
    Value(Option<String>),
    Redirect(Node),
}

#[export_impl]
impl KeyValueStore {
    fn new(raft: Arc<raft::Raft>) -> Self {
        Self { raft }
    }

    #[export_method]
    pub async fn get_key(&self, key: String) -> Result<GetKeyResponse, toy_rpc::Error> {
        if let Err(e) = self.raft.raft.is_leader().await {
            if let Some(leader) = e.forward_to_leader() {
                if let Some(leader) = &leader.leader_node {
                    tracing::debug!("Forwarding request to leader: {}", leader.id);
                    Ok(GetKeyResponse::Redirect(leader.clone()))
                } else {
                    tracing::error!("No leader found in cluster");
                    Err(toy_rpc::Error::Internal(Box::new(e)))
                }
            } else {
                tracing::error!("Error checking if this node is the leader: {:?}", e);
                Err(toy_rpc::Error::Internal(Box::new(e)))
            }
        } else {
            Ok(GetKeyResponse::Value(
                self.raft
                    .store
                    .sm
                    .read()
                    .await
                    .get(&key)
                    .map_err(|e| {
                        tracing::error!("Error reading from store: {:?}", e);
                        toy_rpc::Error::Internal(Box::new(e))
                    })?
                    .and_then(|v| if v.is_empty() { None } else { Some(v) }),
            ))
        }
    }

    #[export_method]
    pub async fn set_key(
        &self,
        request: Request,
    ) -> Result<ClientWriteResponse<TypeConfig>, toy_rpc::Error> {
        self.raft.raft.client_write(request).await.map_err(|e| {
            tracing::error!("Error writing to store: {:?}", e);
            toy_rpc::Error::Internal(Box::new(e))
        })
    }

    #[export_method]
    pub async fn delete_key(
        &self,
        key: String,
    ) -> Result<ClientWriteResponse<TypeConfig>, toy_rpc::Error> {
        self.raft
            .raft
            .client_write(Request {
                key,
                value: String::new(),
            })
            .await
            .map_err(|e| {
                tracing::error!("Error writing to store: {:?}", e);
                toy_rpc::Error::Internal(Box::new(e))
            })
    }
}
