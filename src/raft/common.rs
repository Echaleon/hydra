use std::io::Cursor;

use serde::{Deserialize, Serialize};

use crate::raft::store::{Request, Response};

pub type NodeId = u64;
pub type SnapshotList = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    pub TypeConfig: D = Request, R = Response, NodeId = NodeId, Node = Node
);

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
