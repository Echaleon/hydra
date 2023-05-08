use std::sync::Arc;

use openraft::error::Fatal;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use toy_rpc::macros::export_impl;

use crate::raft::common::{Node, NodeId, TypeConfig};
use crate::raft::store::Store;

pub mod common;
pub mod network;
pub mod store;

#[cfg(test)]
mod store_test;

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
