use clap::Parser;

mod raft;

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
    bind: std::net::SocketAddr,

    /// Path of where to store the database.
    #[clap(short, long, default_value = "db")]
    db_path: std::path::PathBuf,

    /// Path to the directory  of where to store the log file. If not specified, logs will be
    /// printed to stdout.
    #[clap(short, long)]
    log_path: Option<std::path::PathBuf>,

    /// List of other nodes to connect to. If not specified, the node will start in standalone
    /// mode.
    #[clap(short, long)]
    nodes: Option<Vec<std::net::SocketAddr>>,

    /// Set the node id. If not specified, a random node id will be generated.
    #[clap(short = 'i', long)]
    node_id: Option<u64>,
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

    // Create the database. Output a tracing error if the database could not be created and exit.
    tracing::debug!("Creating database at {}", opts.db_path.display());
    let kv = match kv::Store::new(kv::Config::new(opts.db_path).use_compression(opts.compress)) {
        Ok(kv) => kv,
        Err(err) => {
            tracing::error!("Failed to create database: {}", err);
            return;
        }
    };
}
