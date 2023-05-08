use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use clap::Parser;
use openraft::Config;
use toy_rpc::pubsub::AckModeNone;
use toy_rpc::Server;

use hydra::raft::store::Store;
use hydra::raft::Raft;
use hydra::{api, raft};

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

    /// Flag to set whether the database should be compressed or not.
    #[clap(short, long)]
    compress: bool,

    /// Network address to bind to.
    #[clap(short, long, default_value = "127.0.0.1:9000")]
    bind: SocketAddr,

    /// Path of where to store the database.
    #[clap(short, long, default_value = "db")]
    db_path: std::path::PathBuf,

    /// Path to the directory  of where to store the log file. If not specified, logs will be
    /// printed to stdout.
    #[clap(short, long)]
    log_path: Option<std::path::PathBuf>,

    /// Set the node id.
    node_id: u64,
}

#[tokio::main]
async fn main() {
    // Parse command line arguments.
    let opts = Opts::parse();

    // If the user specified a log path, create a file logger, otherwise just print to stdout.
    let (non_blocking, _guard) = if let Some(log_path) = &opts.log_path {
        tracing_appender::non_blocking(tracing_appender::rolling::daily(log_path, "hydra"))
    } else {
        tracing_appender::non_blocking(std::io::stdout())
    };

    // Set up the tracing subscriber.
    tracing_subscriber::fmt()
        .with_max_level(match (opts.verbose, opts.quiet) {
            (1, _) => tracing::Level::DEBUG,
            (x, _) if (x >= 2) => tracing::Level::TRACE,
            (_, 1) => tracing::Level::WARN,
            (_, x) if (x >= 2) => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        })
        .with_writer(non_blocking)
        .with_ansi(opts.log_path.is_none())
        .init();

    // Create the raft configuration.
    tracing::debug!("Creating configuration");
    let config = match Config::default().validate() {
        Ok(config) => config,
        Err(err) => {
            tracing::error!("Invalid configuration: {}", err);
            return;
        }
    };

    // Create the database.
    tracing::debug!("Creating database at {}", opts.db_path.display());
    let store = Store::new(
        match kv::Store::new(kv::Config::new(opts.db_path).use_compression(opts.compress)) {
            Ok(kv) => kv,
            Err(err) => {
                tracing::error!("Failed to create database: {}", err);
                return;
            }
        },
    )
    .await;

    // Create the node.
    tracing::debug!("Creating node");
    let raft = match Raft::new(opts.node_id, opts.bind, config, store).await {
        Ok(raft) => raft,
        Err(err) => {
            tracing::error!("Failed to create node: {}", err);
            return;
        }
    };

    // Create the RPC server and router.
    let raft = Arc::new(raft);
    let raft_server = toy_rpc::Server::builder().register(raft.clone()).build();
    let router = router(raft_server, raft);

    // Create the shutdown handler.
    let shutdown_handle = axum_server::Handle::new();
    tokio::spawn(shutdown(shutdown_handle.clone()));

    // Start the node and await for it to exit.
    tracing::info!("Starting node on {}", opts.bind);
    match axum_server::bind(opts.bind)
        .handle(shutdown_handle)
        .serve(router.into_make_service())
        .await
    {
        Ok(_) => {
            tracing::info!("Node exited");
        }
        Err(err) => {
            tracing::error!("Node exited with error: {}", err);
        }
    }
}

// Create the router.
fn router(server: Server<AckModeNone>, raft: Arc<raft::Raft>) -> Router {
    Router::new()
        .route_service("/raft", server.handle_http())
        .merge(api::api(raft))
}

// Shutdown handler
async fn shutdown(handle: axum_server::Handle) {
    // Check for CTRL+C
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Could not register CTRL-C signal handler");
    };

    // Check for the TERM signal on Linux
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Could not register TERM signal handler")
            .recv()
            .await;
    };

    // Check for the CTRL+BREAK signal on Windows
    #[cfg(windows)]
    let terminate = async {
        tokio::signal::windows::ctrl_break()
            .expect("Could not register CTRL-BREAK signal handler")
            .recv()
            .await;
    };

    // Wait for either signal and exit
    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("CTRL-C signal received, shutting down");
        },
        _ = terminate => {
            tracing::info!("TERM signal received, shutting down");
        }
    }

    // Shutdown the server
    handle.shutdown();
}
