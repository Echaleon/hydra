use std::sync::Arc;

use openraft::error::Fatal;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use toy_rpc::macros::export_impl;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::raft::store::Store;

pub mod network;
pub mod store;

#[cfg(test)]
mod store_test;

openraft::declare_raft_types!(
    #[derive(Serialize, Deserialize)]
    pub TypeConfig: D = Request, R = Response, NodeId = NodeId, Node = Node
);

pub type NodeId = u64;
pub type SnapshotList = Cursor<Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub addr: std::net::SocketAddr,
}

// Just create a node with an id of 0 and an address of 0.0.0.0:0.
impl Default for Node {
    fn default() -> Self {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        Self {
            id: 0,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
        }
    }
}

// Data stored into the key-value store is a string and associated json data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub key: String,
    pub value: String,
}

// Data returned from the key-value store is the associated json data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub value: Option<String>,
}

#[derive(Clone)]
pub struct Raft {
    pub node: Node,
    pub store: Store,
    pub raft: openraft::Raft<TypeConfig, network::Network, Store>,
}

#[export_impl]
impl Raft {
    pub async fn new(
        id: NodeId,
        addr: std::net::SocketAddr,
        config: openraft::Config,
        store: Store,
    ) -> Result<Self, Fatal<NodeId>> {
        Ok(Self {
            node: Node { id, addr },
            store: store.clone(),
            raft: openraft::Raft::new(id, Arc::new(config), network::Network, store).await?,
        })
    }

    #[export_method]
    pub async fn vote(
        &self,
        req: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, toy_rpc::Error> {
        self.raft
            .vote(req)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn append(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, toy_rpc::Error> {
        self.raft
            .append_entries(req)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, toy_rpc::Error> {
        self.raft
            .install_snapshot(req)
            .await
            .map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }
}
