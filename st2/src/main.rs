//! CLI to SnailTrail, containing subcommands for
//! running aggregate metrics,
//! visualizing SnailTrail's PAG, and
//! inspecting & benchmarking SnailTrail itself.
#![deny(missing_docs)]
#[macro_use] extern crate log;
use env_logger;

use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use st2::STError;

fn main() {
    env_logger::init();
    info!("running.");

    match run() {
        Ok(()) => (),
        Err(STError(e)) => eprintln!("Error: {}", e)
    }
}

fn run() -> Result<(), STError> {
    let args = clap::App::new("snailtrail")
        .about("Online and offline analysis of Timely & Differential dataflows")
        .arg(clap::Arg::with_name("interface")
             .short("i")
             .long("interface")
             .value_name("INTERFACE")
             .conflicts_with("from_file")
             .help("Interface (ip address) to listen on. Set if you want to run online.")
             .takes_value(true))
        .arg(clap::Arg::with_name("port")
             .short("p")
             .long("port")
             .value_name("PORT")
             .requires("interface")
             .help("Port to listen on when running online"))
        .arg(clap::Arg::with_name("from_file")
             .short("f")
             .long("from-file")
             .value_name("PATH")
             .help("File path from which to load *.dump files (without trailing /). Set if you want to run offline.")
             .takes_value(true))
        .arg(clap::Arg::with_name("source_peers")
             .short("s")
             .long("source-peers")
             .value_name("PEERS")
             .help("Number of workers in the source computation")
             .required(true))
        .arg(clap::Arg::with_name("snailtrail_workers")
             .short("w")
             .long("snailtrail-workers")
             .value_name("WORKERS")
             .help("Number of worker threads for SnailTrail")
             .default_value("1"))
        .subcommand(clap::SubCommand::with_name("viz")
            .about("Render a PAG visualization")
            .arg(clap::Arg::with_name("output_path")
                .short("o")
                .long("out")
                .value_name("PATH")
                .help("The output path for the generated html file (don't forget the .html extension)")
                .default_value("graph.html"))
        )
        .subcommand(
            clap::SubCommand::with_name("metrics")
                .about("Write dataflow metrics to file")
                .arg(clap::Arg::with_name("output_path")
                    .short("o")
                    .long("out")
                    .value_name("PATH")
                    .help("The output path for the generated CSV file (don't forget the .CSV extension)")
                    .default_value("metrics.csv"))
        )
        .subcommand(
            clap::SubCommand::with_name("inspect")
                .about("run SnailTrail inspector")
        )
        .get_matches();

    match args.subcommand() {
        (_, None) => Err(STError("Invalid subcommand".to_string()))?,
        _ => (),
    }

    // @TODO: support cluster mode
    let st_workers: usize = args.value_of("snailtrail_workers").expect("error parsing args")
        .parse().map_err(|e| STError(format!("Invalid --diag-workers: {}", e)))?;
    let timely_configuration = match st_workers {
        1 => timely::Configuration::Thread,
        n => timely::Configuration::Process(n),
    };


    match args.subcommand() {
        ("viz", Some(viz_args)) => {
            let output_path = std::path::Path::new(viz_args.value_of("output_path").expect("error parsing args"));

            let replay_source = make_replay_source(&args)?;
            println!("Connected!");

            st2::commands::viz::run(timely_configuration, replay_source, output_path)
        }
        ("metrics", Some(metrics_args)) => {
            let output_path = std::path::Path::new(metrics_args.value_of("output_path").expect("error parsing args"));

            let replay_source = make_replay_source(&args)?;
            println!("Connected!");

            st2::commands::metrics::run(timely_configuration, replay_source, output_path)
        }
        ("inspect", Some(_inspect_args)) => {
            let replay_source = make_replay_source(&args)?;
            println!("Connected!");

            st2::commands::inspect::run(timely_configuration, replay_source)
        }
        _ => panic!("Invalid subcommand"),
    }
}

/// creates one socket per worker in the computation we're examining
fn make_replay_source(args: &clap::ArgMatches) -> Result<ReplaySource, STError> {
    let source_peers: usize = args.value_of("source_peers").expect("error parsing args")
        .parse().map_err(|e| STError(format!("Invalid --source-peers: {}", e)))?;

    if let Some(path) = args.value_of("from_file") {
        let path: String = path.parse().map_err(|e| STError(format!("Invalid --from_file: {}", e)))?;

        println!("Reading from {} *.dump files", source_peers);

        let files = (0 .. source_peers)
            .map(|idx| format!("{}/{}.dump", path, idx))
            .map(|path| Some(PathBuf::from(path)))
            .collect::<Vec<_>>();

        Ok(ReplaySource::Files(Arc::new(Mutex::new(files))))
    } else {
        let ip_addr: std::net::IpAddr = args.value_of("interface").expect("error parsing args")
            .parse().map_err(|e| STError(format!("Invalid --interface: {}", e)))?;
        let port: u16 = args.value_of("port").expect("error parsing args")
            .parse().map_err(|e| STError(format!("Invalid --port: {}", e)))?;

        println!("Listening for {} connections on {}:{}", source_peers, ip_addr, port);

        let sockets = connect::open_sockets(ip_addr, port, source_peers)?;
        Ok(ReplaySource::Tcp(Arc::new(Mutex::new(sockets))))
    }
}
