use differential_dataflow::lattice::Lattice;

use timely::dataflow::Scope;
use timely::dataflow::operators::{capture::{event::Event, Capture, replay::Replay, extract::Extract}};
use timely::dataflow::operators::map::Map;
use timely::communication::initialize::WorkerGuards;

use timely_adapter::{connect::{make_replayers, open_sockets}};

use timely_snailtrail::{pag, Config};
use timely_snailtrail::pag::PagEdge;

use logformat::pair::Pair;

use std::io::Write;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

static HTML: &str = include_str!("../../html/index.html");

fn main() {
    let worker_peers = std::env::args().nth(2).expect("1st").parse::<usize>().expect("no num");
    let source_peers = std::env::args().nth(3).expect("2nd").parse::<usize>().expect("no num2");
    let output_path = std::env::args().nth(4).expect("path");
    let from_file = if let Some(_) = std::env::args().nth(5) {
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

        write!(stdout, "Press enter to generate graph (this will crash the source computation if it hasn't terminated).")
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
    let sockets = if !config.from_file {
        Some(open_sockets(config.source_peers))
    } else {
        None
    };

    let (pag_send, pag_recv) = mpsc::channel();
    let pag_send = Arc::new(Mutex::new(pag_send));

    let worker_handles = timely::execute_from_args(config.timely_args.clone().into_iter(), move |worker| {
        let pag_send: mpsc::Sender<_> = pag_send.lock().expect("cannot lock pag_send").clone();

        // read replayers from file (offline) or TCP stream (online)
        let replayers = make_replayers(
            worker.index(),
            config.worker_peers,
            config.source_peers,
            sockets.clone(),
        );

        worker.dataflow(|scope| {
            pag::create_pag(scope, replayers, 0)
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
