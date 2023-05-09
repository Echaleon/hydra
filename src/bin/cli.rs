use std::collections::{BTreeMap, BTreeSet};
use std::iter::zip;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use toy_rpc::client::Client;

use hydra::api::cluster::ClusterClientStub;
use hydra::api::database::{GetKeyResponse, KeyValueStoreClientStub};
use hydra::raft::{Node, NodeId, Request};

use crate::options::{
    AddOpts, ChangeOpts, ClusterOpts, DeleteOpts, GetOpts, InitOpts, MetricsOpts, PingOpts,
    SetOpts, ShutdownOpts, StartOpts,
};

mod options;

#[derive(Debug, Parser)]
#[clap(version)]
struct Opts {
    /// Flag for verbose output. By default, errors, warnings, and info messages are printed. If
    /// specified once, debug messages are printed. If specified twice, trace messages are printed.
    /// Overrides the quiet flag.
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Flag for quiet output. By default, errors, warnings, and info messages are printed. If
    /// specified once, only errors and warnings are printed. If specified twice, only errors are
    /// printed. Overridden by the verbose flag.
    #[clap(short, long, action = clap::ArgAction::Count)]
    quiet: u8,

    /// Command to run
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a new cluster from scratch, with all the nodes running on the same machine.
    /// This is useful for testing.
    Cluster(ClusterOpts),

    /// Start a server that will be initialized later.
    /// This is useful for starting a server that will be added to an existing cluster, or for
    /// building a cluster that is running on multiple machines.
    Start(StartOpts),

    /// Initialize a cluster, by passing in existing node information.
    Init(InitOpts),

    /// Add a listening node to the cluster
    Add(AddOpts),

    /// Change the membership of the cluster. This can be used to remove nodes from the cluster.
    Change(ChangeOpts),

    /// Get the metrics of the cluster or a specific node.
    Metrics(MetricsOpts),

    /// Shutdown a node in the cluster.
    Shutdown(ShutdownOpts),

    /// Ping a node in the cluster.
    Ping(PingOpts),

    /// Get a key from the cluster.
    Get(GetOpts),

    /// Set a key in the cluster.
    Set(SetOpts),

    /// Delete a key from the cluster.
    Delete(DeleteOpts),
}

#[tokio::main]
async fn main() {
    let opts = Opts::parse();

    // Initialize the logger
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_max_level(match (opts.verbose, opts.quiet) {
            (1, _) => tracing::Level::DEBUG,
            (x, _) if (x >= 2) => tracing::Level::TRACE,
            (_, 1) => tracing::Level::WARN,
            (_, x) if (x >= 2) => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        })
        .with_writer(non_blocking)
        .init();

    if let Err(e) = match opts.cmd {
        Command::Cluster(opts) => cluster(opts).await,
        Command::Start(opts) => start(opts).await,
        Command::Init(opts) => init(opts).await,
        Command::Add(opts) => add(opts).await,
        Command::Change(opts) => change(opts).await,
        Command::Metrics(opts) => metrics(opts).await,
        Command::Shutdown(opts) => shutdown(opts).await,
        Command::Ping(opts) => ping(opts).await,
        Command::Get(opts) => get(opts).await,
        Command::Set(opts) => set(opts).await,
        Command::Delete(opts) => delete(opts).await,
    } {
        tracing::error!("{}", e);
    }
}

async fn cluster(opts: ClusterOpts) -> Result<()> {
    let path = opts.path.unwrap_or_else(|| PathBuf::from("hydra"));

    // Build a vector of the commands to run. For each port specified, start a hydra process.
    let mut commands = opts
        .ports
        .iter()
        .map(|_| std::process::Command::new(&path))
        .collect::<Vec<_>>();

    // Set up the actual commands to run
    for (port, command) in zip(&opts.ports, &mut commands) {
        for _ in 0..opts.verbose {
            command.arg("-v");
        }
        for _ in 0..opts.quiet {
            command.arg("-q");
        }
        if opts.compress {
            command.arg("-c");
        }
        if let Some(log_path) = opts.log_path.as_ref() {
            command
                .arg("-l")
                .arg(format!("{}-{}", log_path.display(), port));
        }
        command
            .arg("-b")
            .arg(format!("{}:{}", opts.ip, port))
            .arg("-d")
            .arg(format!("{}-{}", opts.db_path.display(), port))
            .arg(port.to_string());
    }

    // Spawn the servers
    let children = commands
        .into_iter()
        .map(|mut command| command.spawn())
        .collect::<Result<Vec<_>, _>>()?;

    // Loop through the servers and wait until all of them respond to a ping
    for port in &opts.ports {
        let client = Client::dial_http(&format!("ws://{}:{}/cluster/rpc/", opts.ip, port)).await?;
        loop {
            match client.cluster().ping(None).await {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
        client.close().await;
    }

    // Build the list of nodes to pass to the first server
    let nodes = opts
        .ports
        .iter()
        .map(|port| {
            (
                *port as NodeId,
                Node {
                    id: *port as NodeId,
                    addr: std::net::SocketAddr::new(opts.ip, *port),
                },
            )
        })
        .collect::<BTreeMap<NodeId, Node>>();

    // Initialize the first server
    let client =
        Client::dial_http(&format!("ws://{}:{}/cluster/rpc/", opts.ip, &opts.ports[0])).await?;
    client.cluster().initialize(Some(nodes)).await?;
    client.close().await;
    println!("Cluster initialized");

    // Wait for the servers to exit
    for mut child in children {
        child.wait()?;
    }

    Ok(())
}

async fn start(opts: StartOpts) -> Result<()> {
    // Begin building the command to run
    let mut command =
        std::process::Command::new(opts.path.unwrap_or_else(|| PathBuf::from("hydra")));
    for _ in 0..opts.verbose {
        command.arg("-v");
    }
    for _ in 0..opts.quiet {
        command.arg("-q");
    }
    if opts.compress {
        command.arg("-c");
    }
    if let Some(log_path) = opts.log_path {
        command.arg("-l").arg(log_path);
    }
    command
        .arg("-b")
        .arg(opts.bind.to_string())
        .arg("-d")
        .arg(opts.db_path)
        .arg(opts.node_id.to_string());

    // Spawn the hydra process
    let mut handle = command.spawn().map_err(anyhow::Error::from)?;

    // Wait for the hydra process to exit.
    // TODO: Make the hydra process daemonize itself. On Linux, this is fairly easy to do, but
    //       to do this on Windows it has to be installed as a service.
    handle.wait().map_err(anyhow::Error::from).map(|_| ())
}

async fn init(opts: InitOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/cluster/rpc/", opts.current_node)).await?;
    client
        .cluster()
        .initialize(if opts.nodes.is_empty() {
            None
        } else {
            Some(
                opts.nodes
                    .into_iter()
                    .map(|node| {
                        (
                            node.id,
                            Node {
                                id: node.id,
                                addr: node.address,
                            },
                        )
                    })
                    .collect::<BTreeMap<NodeId, Node>>(),
            )
        })
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("Cluster initialized");
    Ok(())
}

async fn add(opts: AddOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/cluster/rpc/", opts.current_node)).await?;
    let res = client
        .cluster()
        .add_learner(Node {
            id: opts.node_id,
            addr: opts.address,
        })
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("{:#?}", res);
    Ok(())
}

async fn change(opts: ChangeOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/cluster/rpc/", opts.current_node)).await?;
    let res = client
        .cluster()
        .change_membership(opts.node_ids.into_iter().collect::<BTreeSet<NodeId>>())
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("{:#?}", res);
    Ok(())
}

async fn metrics(opts: MetricsOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/cluster/rpc/", opts.node)).await?;
    let res = client
        .cluster()
        .metrics(None)
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("{:#?}", res);
    Ok(())
}

async fn shutdown(opts: ShutdownOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/cluster/rpc/", opts.node)).await?;
    client
        .cluster()
        .shutdown(None)
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("Shutdown successful");
    Ok(())
}

async fn ping(opts: PingOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/cluster/rpc/", opts.node)).await?;
    client
        .cluster()
        .ping(None)
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("Ping successful");
    Ok(())
}

async fn get(opts: GetOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/keys/rpc/", opts.node)).await?;
    let res = client
        .key_value_store()
        .get_key(opts.key.clone())
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;

    // Check if the key is actually present or if we got a redirect.
    match res {
        GetKeyResponse::Value(value) => {
            println!("{}", value.unwrap_or_else(|| "Key not found".to_string()));
            Ok(())
        }
        GetKeyResponse::Redirect(node) => {
            tracing::info!("Redirecting to {}", node.addr);
            let client = Client::dial_http(&format!("ws://{}/keys/rpc/", node.addr)).await?;
            let res = client
                .key_value_store()
                .get_key(opts.key)
                .await
                .map_err(anyhow::Error::from)?;
            client.close().await;

            if let GetKeyResponse::Value(value) = res {
                println!("{}", value.unwrap_or_else(|| "Key not found".to_string()));
            } else {
                println!("Client was redirected too many times. Please try again.");
            }

            Ok(())
        }
    }
}

async fn set(opts: SetOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/keys/rpc/", opts.node)).await?;
    let res = client
        .key_value_store()
        .set_key(Request {
            key: opts.key,
            value: opts.value,
        })
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("{:#?}", res);
    Ok(())
}

async fn delete(opts: DeleteOpts) -> Result<()> {
    let client = Client::dial_http(&format!("ws://{}/keys/rpc/", opts.node)).await?;
    let res = client
        .key_value_store()
        .delete_key(opts.key)
        .await
        .map_err(anyhow::Error::from)?;
    client.close().await;
    println!("{:#?}", res);
    Ok(())
}
