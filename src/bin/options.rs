use std::net::{IpAddr, SocketAddr};

use clap::Args;

#[derive(Debug, Args)]
pub struct ClusterOpts {
    /// Flag for verbose output. By default, errors, warnings, and info messages are printed. If
    /// specified once, debug messages are printed. If specified twice, trace messages are printed.
    /// Overrides the quiet flag. This gets passed to the server.
    #[clap(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Flag for quiet output. By default, errors, warnings, and info messages are printed. If
    /// specified once, only errors and warnings are printed. If specified twice, only errors are
    /// printed. Overridden by the verbose flag. This gets passed to the server.
    #[clap(short, long, action = clap::ArgAction::Count)]
    pub quiet: u8,

    /// Flag to set whether the database should be compressed or not.
    #[clap(short, long)]
    pub compress: bool,

    /// IP to listen on
    #[clap(short, long, default_value = "127.0.0.1")]
    pub ip: IpAddr,

    /// Ports to listen on
    #[clap(short, long, default_value = "9000")]
    pub ports: Vec<u16>,

    /// Path of where to store the database.
    #[clap(short, long, default_value = "db")]
    pub db_path: std::path::PathBuf,

    /// Log path to write to
    /// If not specified, logs will be printed to stdout.
    #[clap(short, long)]
    pub log_path: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct StartOpts {
    /// Flag for verbose output. By default, errors, warnings, and info messages are printed. If
    /// specified once, debug messages are printed. If specified twice, trace messages are printed.
    /// Overrides the quiet flag. This gets passed to the server.
    #[clap(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Flag for quiet output. By default, errors, warnings, and info messages are printed. If
    /// specified once, only errors and warnings are printed. If specified twice, only errors are
    /// printed. Overridden by the verbose flag. This gets passed to the server.
    #[clap(short, long, action = clap::ArgAction::Count)]
    pub quiet: u8,

    /// Flag to set whether the database should be compressed or not.
    #[clap(short, long)]
    pub compress: bool,

    /// Network address to bind to.
    #[clap(short, long, default_value = "127.0.0.1:9000")]
    pub bind: SocketAddr,

    /// Path of where to store the database.
    #[clap(short, long, default_value = "db")]
    pub db_path: std::path::PathBuf,

    /// Log path to write to
    /// If not specified, logs will be printed to stdout.
    #[clap(short, long)]
    pub log_path: Option<std::path::PathBuf>,

    /// Set the node id.
    pub node_id: u64,
}

#[derive(Debug, Args)]
pub struct InitOpts {
    /// Network nodes and addresses to add to the cluster. This should be a list of
    /// node IDs and addresses, e.g. "1,127.0.0.1:9000 2,127.0.0.1:9001 3,127.0.0.1:9002".
    #[arg(value_parser = parse_node)]
    #[arg(required = true)]
    pub nodes: Vec<Node>,
}

#[derive(Debug, Args)]
pub struct AddOpts {
    /// Address of a current node in the cluster.
    pub current_node: SocketAddr,

    /// ID of the node to add.
    pub node_id: u64,

    /// Network address of the node to add.
    pub address: SocketAddr,
}

#[derive(Debug, Args)]
pub struct ChangeOpts {
    /// Address of a current node in the cluster.
    pub current_node: SocketAddr,

    /// IDs of nodes that should be in the cluster.
    pub node_ids: Vec<u64>,
}

#[derive(Debug, Args)]
pub struct MetricsOpts {
    /// Address of a node in the cluster.
    pub node: SocketAddr,
}

#[derive(Debug, Args)]
pub struct GetOpts {
    /// Address of a node in the cluster.
    pub node: SocketAddr,

    /// Key to get.
    pub key: String,
}

#[derive(Debug, Args)]
pub struct SetOpts {
    /// Address of a node in the cluster.
    pub node: SocketAddr,

    /// Key to set.
    pub key: String,

    /// Value to set.
    pub value: String,
}

#[derive(Debug, Args)]
pub struct DeleteOpts {
    /// Address of a node in the cluster.
    pub node: SocketAddr,

    /// Key to delete.
    pub key: String,
}

// A small helper struct to hold node id and address pairs.
#[derive(Debug, Clone)]
pub struct Node {
    pub id: u64,
    pub address: SocketAddr,
}

// Parse a node from a string of the form "id,address".
fn parse_node(arg: &str) -> Result<Node, String> {
    let mut split = arg.split(',');
    let id = split.next().ok_or_else(|| "Missing node ID".to_string())?;
    let id = id
        .parse::<u64>()
        .map_err(|_| "Invalid node ID".to_string())?;
    let address = split
        .next()
        .ok_or_else(|| "Missing node address".to_string())?;
    let address = address
        .parse::<SocketAddr>()
        .map_err(|_| "Invalid node address".to_string())?;
    Ok(Node { id, address })
}
