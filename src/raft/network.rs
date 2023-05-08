use std::any::Any;
use std::fmt::Display;

use axum::async_trait;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AnyError, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use toy_rpc::pubsub::AckModeNone;
use toy_rpc::Client;

use crate::raft::common::{Node, NodeId, TypeConfig};
use crate::raft::RaftClientStub;

// ------ Network ------ //

#[derive(Clone, Debug)]
pub struct Network;

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        NetworkConnection {
            target: node.clone(),
            connection: Client::dial_http(&format!("ws://{}/raft", node.addr))
                .await
                .map_or_else(
                    |e| {
                        tracing::error!("Failed to connect to {}: {}", node.id, e);
                        None
                    },
                    Some,
                ),
        }
    }
}

// ------ NetworkConnection ------ //

pub struct NetworkConnection {
    target: Node,
    connection: Option<Client<AckModeNone>>,
}

impl NetworkConnection {
    // Connect to the node. If there is already a connection, return it. Otherwise, create a new connection, and return an error if it fails.
    async fn connect<E: std::error::Error + DeserializeOwned>(
        &mut self,
    ) -> Result<&Client<AckModeNone>, RPCError<NodeId, Node, E>> {
        if self.connection.is_none() {
            self.connection = Client::dial_http(&format!("ws://{}/raft", self.target.addr))
                .await
                .map_or_else(
                |e| {
                    tracing::error!("Failed to connect to {}: {}", self.target.id, e);
                    None
                },
                Some,
            );
        }

        self.connection.as_ref().ok_or_else(|| RPCError::Network(NetworkError::from(AnyError::default())))
    }
}

#[async_trait]
impl RaftNetwork<TypeConfig> for NetworkConnection {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn send_append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        tracing::debug!("Sending append entries to {}", self.target.id);
        let client = self.connect().await?;
        client.raft().append(rpc).await.map_err(|e| to_error(e, &self.target))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn send_install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        tracing::debug!("Sending install snapshot to {}", self.target.id);
        let client = self.connect().await?;
        client.raft().snapshot(rpc).await.map_err(|e| to_error(e, &self.target))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn send_vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        tracing::debug!("Sending vote to {}", self.target.id);
        let client = self.connect().await?;
        client.raft().vote(rpc).await.map_err(|e| to_error(e, &self.target))
    }
}

// ------ Error Handling ------ //

#[derive(Debug)]
struct BoxedError(Box<dyn std::error::Error>);

impl Display for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for BoxedError {}

fn to_error<E: std::error::Error + 'static + Clone>(
    e: toy_rpc::Error,
    target: &Node,
) -> RPCError<NodeId, Node, E> {
    match e {
        toy_rpc::Error::IoError(e) => RPCError::Network(NetworkError::new(&e)),
        toy_rpc::Error::ParseError(e) => RPCError::Network(NetworkError::new(&BoxedError(e))),
        toy_rpc::Error::Internal(e) => {
            let any: &dyn Any = &e;
            let error: &E = any.downcast_ref().unwrap();
            RPCError::RemoteError(RemoteError::new(target.id, error.clone()))
        }
        e @ (toy_rpc::Error::InvalidArgument
        | toy_rpc::Error::ServiceNotFound
        | toy_rpc::Error::MethodNotFound
        | toy_rpc::Error::ExecutionError(_)
        | toy_rpc::Error::Canceled(_)
        | toy_rpc::Error::Timeout(_)
        | toy_rpc::Error::MaxRetriesReached(_)) => RPCError::Network(NetworkError::new(&e)),
    }
}

