use timely::dataflow::operators::{capture::{event::Event, Capture, extract::Extract}};
use timely::dataflow::operators::map::Map;
use timely::communication::initialize::WorkerGuards;

use crate::pag;
use crate::pag::PagEdge;
use crate::STError;

use st2_logformat::pair::Pair;

use std::io::Write;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;


/// Renders the PAG created from `replay_source` to file at `output_path`.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource,
    output_path: &std::path::Path) -> Result<(), STError> {

    let (worker_handles, pag) = prep_pag(timely_configuration, replay_source)?;

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
    render(output_path, worker_handles, pag);

    println!("Graph generated in file://{}", std::fs::canonicalize(output_path).expect("invalid path").to_string_lossy());

    Ok(())
}

/// Creates the PAG from `replay_source`.
fn prep_pag(timely_configuration: timely::Configuration,
            replay_source: ReplaySource) -> Result<(WorkerGuards<()>, mpsc::Receiver<Event<Pair<u64, Duration>, PagEdge>>), STError> {
    let (pag_send, pag_recv) = mpsc::channel();
    let pag_send = Arc::new(Mutex::new(pag_send));

    let worker_handles = timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        let pag_send: mpsc::Sender<_> = pag_send.lock().expect("cannot lock pag_send").clone();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            pag::create_pag(scope, readers, index, 1)
                .map(|(x, _t, _diff)| x)
                .capture_into(pag_send);
        });
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok((worker_handles, pag_recv))
}

/// Renders the provided PAG to file.
pub fn render(output_path: &std::path::Path, worker_handles: WorkerGuards<()>, pag: mpsc::Receiver<Event<Pair<u64, Duration>, PagEdge>>) {
    worker_handles.join().into_iter().collect::<Result<Vec<_>, _>>().expect("Timely error");

    let mut file = std::fs::File::create(output_path).map_err(|e| panic!(format!("io error: {}", e))).expect("could not create file");

    expect_write(writeln!(file, "let pag = ["));

    for edge in pag.extract().into_iter().flat_map(|(_t, v)| v) {
        expect_write(writeln!(
            file,
            "{{ src: {{ t: {}, w: {}, e: {} }}, dst: {{ t: {}, w: {}, e: {} }}, type: \"{:?}\", o: {}, tr: \"{:?}\", l: {} }},",
            edge.source.timestamp.as_nanos(),
            edge.source.worker_id,
            edge.source.epoch,
            edge.destination.timestamp.as_nanos(),
            edge.destination.worker_id,
            edge.destination.epoch,
            edge.edge_type,
            edge.operator_id.unwrap_or(0),
            edge.traverse,
            edge.length.unwrap_or(0)
        ))
    }
    expect_write(writeln!(file, "];"));
}

/// Unwraps a write.
fn expect_write(e: Result<(), std::io::Error>) {
    e.expect("write failed");
}
