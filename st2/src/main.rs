//! CLI to SnailTrail, containing subcommands for
//! running aggregate metrics,
//! visualizing SnailTrail's PAG, and
//! inspecting & benchmarking SnailTrail itself.
#![deny(missing_docs)]
#[macro_use] extern crate log;
use env_logger;

use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::sync::mpsc;
use std::convert::TryInto;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use st2::STError;
use st2::PagData;
use std::collections::HashMap;

use ws::Handshake;
use ws::Handler;
use ws::Sender;
use ws::Message;
use ws::listen;

use serde_json::json;

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
                .about("run ST2 inspector")
        )
        .subcommand(
            clap::SubCommand::with_name("algo")
                .about("run ST2 graph algorithms")
        )
        .subcommand(
            clap::SubCommand::with_name("dashboard")
                .about("run ST2 live dashboard")
                .arg(clap::Arg::with_name("epoch_max")
                    .short("e")
                    .long("epoch-max")
                    .value_name("MS")
                    .help("Temporal invariant: the maximum milliseconds an epoch is allowed to take"))
                .arg(clap::Arg::with_name("operator_max")
                    .short("o")
                    .long("operator-max")
                    .value_name("MS")
                    .help("Temporal invariant: the maximum milliseconds an operator is allowed to take"))
                .arg(clap::Arg::with_name("message_max")
                    .short("m")
                    .long("message-max")
                    .value_name("MS")
                    .help("Temporal invariant: the maximum milliseconds a control or data message is allowed to take"))
        )
        .subcommand(
            clap::SubCommand::with_name("invariants")
                .about("run invariants checker")
                .arg(clap::Arg::with_name("epoch_max")
                    .short("e")
                    .long("epoch-max")
                    .value_name("MS")
                    .help("Temporal invariant: the maximum milliseconds an epoch is allowed to take"))
                .arg(clap::Arg::with_name("operator_max")
                    .short("o")
                    .long("operator-max")
                    .value_name("MS")
                    .help("Temporal invariant: the maximum milliseconds an operator is allowed to take"))
                .arg(clap::Arg::with_name("message_max")
                    .short("m")
                    .long("message-max")
                    .value_name("MS")
                    .help("Temporal invariant: the maximum milliseconds a control or data message is allowed to take"))
                .arg(clap::Arg::with_name("progress_max")
                    .short("p")
                    .long("progress-max")
                    .value_name("MS")
                    .help("Progress invariant: the maximum milliseconds between two progress messages per worker"))
        )
        .get_matches();

    match args.subcommand() {
        (_, None) => Err(STError("Invalid subcommand".to_string()))?,
        _ => (),
    }

    // @TODO: support cluster mode
    let st_workers: usize = args.value_of("snailtrail_workers").expect("error parsing worker args")
        .parse().map_err(|e| STError(format!("Invalid --diag-workers: {}", e)))?;
    let timely_configuration = match st_workers {
        1 => timely::Configuration::Thread,
        n => timely::Configuration::Process(n),
    };

    match args.subcommand() {
        ("metrics", Some(metrics_args)) => {
            let output_path = std::path::Path::new(metrics_args.value_of("output_path").expect("error parsing metrics output args"));

            let replay_source = make_replay_source(&args)?;
            println!("Connected!");

            st2::commands::metrics::run(timely_configuration, replay_source, output_path)
        }
        ("inspect", Some(_inspect_args)) => {
            let replay_source = make_replay_source(&args)?;
            println!("Connected!");

            st2::commands::inspect::run(timely_configuration, replay_source)
        }
        ("algo", Some(_algo_args)) => {
            let replay_source = make_replay_source(&args)?;
            println!("Connected!");

            st2::commands::algo::run(timely_configuration, replay_source)
        }
        ("dashboard", Some(dashboard_args)) => {
            let epoch_max: Option<u64> = if let Some(t) = dashboard_args.value_of("epoch_max") {
                println!("epoch max given");
                Some(t.parse().map_err(|e| STError(format!("Invalid --epoch-max: {}", e)))?)
            } else {
                None
            };
            let operator_max: Option<u64> = if let Some(t) = dashboard_args.value_of("operator_max") {
                Some(t.parse().map_err(|e| STError(format!("Invalid --operator-max: {}", e)))?)
            } else {
                None
            };
            let message_max: Option<u64> = if let Some(t) = dashboard_args.value_of("message_max") {
                Some(t.parse().map_err(|e| STError(format!("Invalid --message-max: {}", e)))?)
            } else {
                None
            };

            println!("Waiting for source computation...");
            let replay_source = make_replay_source(&args)?;
            println!("Connected to source computation!");

            let (pag_send, pag_recv) = mpsc::channel();
            let pag_send = Arc::new(Mutex::new(pag_send));

            println!("Waiting for dashboard connection...");
            let listener = std::thread::spawn(move || {
                listen("127.0.0.1:3012", |out| { Server { out, pag_recv: &pag_recv, pag_recvd: HashMap::new() } } ).unwrap();
            });

            st2::commands::dashboard::run(timely_configuration, replay_source, pag_send, epoch_max, operator_max, message_max)?;

            listener.join().expect("couldn't join listener");
            Ok(())
        }
        ("invariants", Some(invariants_args)) => {
            let progress_max: Option<u64> = if let Some(t) = invariants_args.value_of("progress_max") {
                Some(t.parse().map_err(|e| STError(format!("Invalid --progress-max: {}", e)))?)
            } else {
                None
            };
            let epoch_max: Option<u64> = if let Some(t) = invariants_args.value_of("epoch_max") {
                println!("epoch max given");
                Some(t.parse().map_err(|e| STError(format!("Invalid --epoch-max: {}", e)))?)
            } else {
                None
            };
            let operator_max: Option<u64> = if let Some(t) = invariants_args.value_of("operator_max") {
                Some(t.parse().map_err(|e| STError(format!("Invalid --operator-max: {}", e)))?)
            } else {
                None
            };
            let message_max: Option<u64> = if let Some(t) = invariants_args.value_of("message_max") {
                Some(t.parse().map_err(|e| STError(format!("Invalid --message-max: {}", e)))?)
            } else {
                None
            };

            let replay_source = make_replay_source(&args)?;
            println!("Connected!");

            st2::commands::invariants::run(timely_configuration, replay_source, epoch_max, operator_max, message_max, progress_max)
        }
        _ => panic!("Invalid subcommand"),
    }?;

    Ok(())
}

/// creates one socket per worker in the computation we're examining
fn make_replay_source(args: &clap::ArgMatches) -> Result<ReplaySource, STError> {
    let source_peers: usize = args.value_of("source_peers").expect("error parsing source peers args")
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
        let ip_addr: std::net::IpAddr = args.value_of("interface").expect("error parsing ip addr args")
            .parse().map_err(|e| STError(format!("Invalid --interface: {}", e)))?;
        let port: u16 = args.value_of("port").expect("error parsing args")
            .parse().map_err(|e| STError(format!("Invalid --port: {}", e)))?;

        println!("Listening for {} connections on {}:{}", source_peers, ip_addr, port);

        let sockets = connect::open_sockets(ip_addr, port, source_peers)?;
        Ok(ReplaySource::Tcp(Arc::new(Mutex::new(sockets))))
    }
}


struct Server<'a> { out: Sender, pag_recv: &'a mpsc::Receiver<(u64, PagData)>, pag_recvd: HashMap<u64, Vec<PagData>> }
impl<'a> Handler for Server<'a> {
    fn on_open(&mut self, _: Handshake) -> ws::Result<()> {
        println!("Connected to dashboard!");
        Ok(())
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        // update internal state
        while let Ok((epoch, pag_data)) = self.pag_recv.try_recv() {
            self.pag_recvd.entry(epoch).or_insert(Vec::new()).push(pag_data);
        }

        let payload: serde_json::Value = match msg {
            Message::Text(msg) => serde_json::from_str(&msg).unwrap(),
            _ => unreachable!()
        };

        let payload_type = payload["type"].as_str().unwrap();

        match payload_type {
            "ALL" => {
                if let Some(events) = self.pag_recvd.get(&payload["epoch"].as_u64().unwrap()) {
                    let result: Vec<_> = events.iter().filter_map(|x| match x {
                        PagData::All(x) => Some(x),
                        _ => None
                    }).collect();
                    self.out.send(json!({"type": "ALL", "payload": result }).to_string())?;
                } else {
                    self.out.send(json!({"type": "ALL", "payload": Vec::<u64>::new() }).to_string())?;
                }
            },
            "AGG" => {
                if let Some(events) = self.pag_recvd.get(&payload["epoch"].as_u64().unwrap()) {
                    let result: Vec<_> = events.iter().filter_map(|x| match x {
                        PagData::Agg(x) => Some(x),
                        _ => None
                    }).collect();
                    self.out.send(json!({"type": "AGG", "payload": result }).to_string())?;
                } else {
                    self.out.send(json!({"type": "AGG", "payload": Vec::<u64>::new() }).to_string())?;
                }
            },
            "PAG" => {
                if let Some(events) = self.pag_recvd.get(&payload["epoch"].as_u64().unwrap()) {
                    let mut result: Vec<_> = events.iter()
                        .filter_map(|x| match x {
                            PagData::Pag(x) => {
                                let src_t: u64 = x.source.timestamp.as_nanos().try_into().unwrap();
                                let dst_t: u64 = x.destination.timestamp.as_nanos().try_into().unwrap();
                                Some(json!({
                                    "src": { "t": src_t,
                                              "w": x.source.worker_id },
                                    "dst": { "t": dst_t,
                                              "w": x.destination.worker_id },
                                    "type": x.edge_type,
                                    "o": x.operator_id.unwrap_or(0),
                                    "l": x.length.unwrap_or(0)
                                }))
                            },
                            _ => None
                        }).collect();
                    result.sort_by_key(|x| (x["src"]["t"]).as_u64());
                    self.out.send(json!({"type": "PAG", "payload": result }).to_string())?;
                } else {
                    self.out.send(json!({"type": "PAG", "payload": Vec::<u64>::new() }).to_string())?;
                }
            },
            "MET" => {
                if let Some(events) = self.pag_recvd.get(&payload["epoch"].as_u64().unwrap()) {
                    let result: Vec<_> = events.iter().filter_map(|x| match x {
                        PagData::Met(x) => Some(x),
                        _ => None
                    }).collect();
                    self.out.send(json!({"type": "MET", "payload": result }).to_string())?;
                } else {
                    self.out.send(json!({"type": "MET", "payload": Vec::<u64>::new() }).to_string())?;
                }
            }
            "INV" => {
                if let Some(events) = self.pag_recvd.remove(&0) {
                    let result: Vec<_> = events.iter().filter_map(|x| match x {
                        PagData::Inv(x) => Some(x),
                        _ => None
                    }).collect();
                    self.out.send(json!({"type": "INV", "payload": result }).to_string())?;
                } else {
                    self.out.send(json!({"type": "INV", "payload": Vec::<u64>::new() }).to_string())?;
                }
            }
            _ => unimplemented!()
        }


        Ok(())
    }
}
