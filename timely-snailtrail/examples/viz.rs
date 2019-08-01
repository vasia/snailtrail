use timely::dataflow::operators::{capture::{event::Event, Capture, extract::Extract}};
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::capture::EventReader;
use timely::communication::initialize::WorkerGuards;
use timely::dataflow::operators::filter::Filter;

use timely_snailtrail::pag;
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
    let (output_path, worker_handles, pag) = prep_pag();

    {
        use std::io;
        use std::io::prelude::*;

        let mut stdin = io::stdin();
        let mut stdout = io::stdout();

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

fn prep_pag() -> (String, WorkerGuards<()>, mpsc::Receiver<Event<Pair<u64, Duration>, PagEdge>>) {
    let mut args = std::env::args();

    let _ = args.next(); // bin name
    let source_peers = args.next().unwrap().parse::<usize>().unwrap();
    let output_path = args.next().expect("missing output path");
    let online = if args.next().unwrap().parse::<bool>().unwrap() {
        Some((args.next().unwrap().parse::<String>().unwrap(), std::env::args().nth(5).unwrap().parse::<u16>().unwrap()))
    } else {
        None
    };
    let _ = args.next(); // --

    let args = args.collect::<Vec<_>>();

    // creates one socket per worker in the computation we're examining
    let replay_source = if let Some((ip, port)) = &online {
        let sockets = connect::open_sockets(ip.parse().expect("couldn't parse IP"), *port, source_peers).expect("couldn't open sockets");
        ReplaySource::Tcp(Arc::new(Mutex::new(sockets)))
    } else {
        let files = (0 .. source_peers)
            .map(|idx| format!("{}.dump", idx))
            .map(|path| Some(PathBuf::from(path)))
            .collect::<Vec<_>>();

        ReplaySource::Files(Arc::new(Mutex::new(files)))
    };

    let (pag_send, pag_recv) = mpsc::channel();
    let pag_send = Arc::new(Mutex::new(pag_send));


    let worker_handles = timely::execute_from_args(args.clone().into_iter(), move |worker| {
        let index = worker.index();

        let pag_send: mpsc::Sender<_> = pag_send.lock().expect("cannot lock pag_send").clone();

        // read replayers from file (offline) or TCP stream (online)
        let readers: Vec<EventReader<_, timely_adapter::connect::CompEvent, _>> =
            connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            pag::create_pag(scope, readers, index, 1)
                .map(|(x, _t, _diff)| x)
                .filter(|x| x.source.epoch == 2)
                .capture_into(pag_send);
        });
    })
    .expect("timely error");

    (output_path, worker_handles, pag_recv)
}

pub fn render(output_path: &String, worker_handles: WorkerGuards<()>, pag: mpsc::Receiver<Event<Pair<u64, Duration>, PagEdge>>) {
    worker_handles.join().into_iter().collect::<Result<Vec<_>, _>>().expect("Timely error");

    let mut file = std::fs::File::create(output_path).map_err(|e| panic!(format!("io error: {}", e))).expect("could not create file");

    expect_write(writeln!(file, "<script type=\"text/javascript\">"));
    expect_write(writeln!(file, "let pag = ["));

    for edge in pag.extract().into_iter().flat_map(|(_t, v)| v) {
        expect_write(writeln!(
            file,
            "{{ src: {{ t: {}, w: {}, e: {} }}, dst: {{ t: {}, w: {}, e: {} }}, type: \"{:?}\", o: \"{:?}\", tr: \"{:?}\" }},",
            edge.source.timestamp.as_nanos(),
            edge.source.worker_id,
            edge.source.epoch,
            edge.destination.timestamp.as_nanos(),
            edge.destination.worker_id,
            edge.destination.epoch,
            edge.edge_type,
            edge.operator_id,
            edge.traverse,
        ))
    }
    expect_write(writeln!(file, "];"));
    expect_write(writeln!(file, "</script>"));
    expect_write(writeln!(file, "{}", HTML));
}

fn expect_write(e: Result<(), std::io::Error>) {
    e.expect("write failed");
}
