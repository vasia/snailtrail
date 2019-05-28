use differential_dataflow::lattice::Lattice;

use timely::dataflow::Scope;
use timely::dataflow::operators::{capture::{event::Event, Capture, replay::Replay, extract::Extract}};
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::capture::EventReader;
use timely::communication::initialize::WorkerGuards;

use timely_snailtrail::{pag, Config};
use timely_snailtrail::pag::PagEdge;

use logformat::pair::Pair;

use std::io::Write;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::path::PathBuf;

static HTML: &str = include_str!("../html/index.html");

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

fn main() {
    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let worker_peers = args.next().expect("no worker peers").parse::<usize>().expect("invalid worker peers");
    let source_peers = args.next().expect("no source peers").parse::<usize>().expect("invalid source peers");
    let output_path = args.next().expect("missing output path");
    let from_file = if let Some(_) = args.next() {
        true
    } else {
        false
    };

    let config = Config {
        timely_args: vec!["-w".to_string(), worker_peers.to_string()],
        worker_peers,
        source_peers,
        from_file,
    };

    let (worker_handles, pag) = prep_pag(config);

    {
        use std::io;
        use std::io::prelude::*;

        let mut stdin = io::stdin();
        let mut stdout = io::stdout();

        // write!(stdout, "Press enter to generate graph (this will crash the source computation if it hasn't terminated).")
        //     .expect("failed to write to stdout");
        write!(stdout, "Wait until computation has finished, then press any key to generate graph.")
            .expect("failed to write to stdout");
        stdout.flush().unwrap();

        // Read a single byte and discard
        let _ = stdin.read(&mut [0u8]).expect("failed to read from stdin");
    }

    println!("rendering...");
    render(&output_path, worker_handles, pag);

    println!("Graph generated in file://{}", std::fs::canonicalize(output_path).expect("invalid path").to_string_lossy());
}

fn prep_pag(config: Config) -> (WorkerGuards<()>, mpsc::Receiver<Event<Pair<u64, Duration>, PagEdge>>) {
    // creates one socket per worker in the computation we're examining
    let replay_source = if config.from_file {
        let files = (0 .. config.source_peers)
            .map(|idx| format!("{}.dump", idx))
            .map(|path| Some(PathBuf::from(path)))
            .collect::<Vec<_>>();

        ReplaySource::Files(Arc::new(Mutex::new(files)))
    } else {
        let sockets = connect::open_sockets("127.0.0.1".parse().expect("couldn't parse IP"), 8000, config.source_peers).expect("couldn't open sockets");
        ReplaySource::Tcp(Arc::new(Mutex::new(sockets)))
    };

    let (pag_send, pag_recv) = mpsc::channel();
    let pag_send = Arc::new(Mutex::new(pag_send));

    let worker_handles = timely::execute_from_args(config.timely_args.clone().into_iter(), move |worker| {
        let pag_send: mpsc::Sender<_> = pag_send.lock().expect("cannot lock pag_send").clone();

        // read replayers from file (offline) or TCP stream (online)
        let readers: Vec<EventReader<_, timely_adapter::connect::CompEvent, _>> =
            connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            pag::create_pag(scope, readers, 0)
                .inner
                .map(|(x, _t, _diff)| x)
                .capture_into(pag_send);
        });
    })
    .expect("timely error");

    (worker_handles, pag_recv)
}

pub fn render(output_path: &String, worker_handles: WorkerGuards<()>, pag: mpsc::Receiver<Event<Pair<u64, Duration>, PagEdge>>) {
    worker_handles.join().into_iter().collect::<Result<Vec<_>, _>>().expect("Timely error");

    let mut file = std::fs::File::create(output_path).map_err(|e| panic!(format!("io error: {}", e))).expect("could not create file");

    expect_write(writeln!(file, "<script type=\"text/javascript\">"));
    expect_write(writeln!(file, "let pag = ["));

    for edge in pag.extract().into_iter().flat_map(|(_t, v)| v) {
        expect_write(writeln!(
            file,
            "{{ w: {}, t: [{}, {}] }},",
            edge.source.worker_id,
            edge.source.timestamp.as_nanos(),
            edge.destination.timestamp.as_nanos()
        ))
    }
    expect_write(writeln!(file, "];"));
    expect_write(writeln!(file, "</script>"));
    expect_write(writeln!(file, "{}", HTML));
}

fn expect_write(e: Result<(), std::io::Error>) {
    e.expect("write failed");
}
