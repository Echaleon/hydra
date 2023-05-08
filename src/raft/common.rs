use std::io::Cursor;

use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type SnapshotList = Cursor<Vec<u8>>;


openraft::declare_raft_types!(
    #[derive(Serialize, Deserialize)]
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

// ------ Storage Interface ------ //

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
