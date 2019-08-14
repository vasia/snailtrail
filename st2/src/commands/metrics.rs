use crate::pag;
use crate::pag::PagEdge;

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::aggregation::aggregate::Aggregate;
use timely::dataflow::operators::delay::Delay;

use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write;

use st2_logformat::pair::Pair;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use crate::STError;


/// Computes aggregate metrics for the computation traces in `replay_source`.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource,
    output_path: &std::path::Path) -> Result<(), STError> {

    let throttle = 1;

    let file = Arc::new(Mutex::new(std::fs::File::create(output_path).map_err(|e| STError(format!("io error: {}", e)))?));

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        let probe: ProbeHandle<Pair<u64, Duration>> = worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, throttle);
            pag.activities(index, Arc::clone(&file)).probe()
        });

        while !probe.done() { worker.step_or_park(None); };

        info!("w{} done", index);
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// Benchmarks epoch duration & # of events passing through
trait Metrics<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Reports activity type & duration per epoch per worker
    fn activities(&self, index: usize, file: Arc<Mutex<std::fs::File>>) -> Stream<S, ()>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> Metrics<S> for Stream<S, (PagEdge, S::Timestamp, isize)> {
    fn activities(&self, index: usize, file: Arc<Mutex<std::fs::File>>) -> Stream<S, ()> {
        if index == 0 {
            expect_write(writeln!(*file.lock().unwrap(), "epoch,from_worker,to_worker,activity_type,#(activities),t(activities),#(records)"));
        }

        self
            .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
            .map(|(edge, _t, _diff)| ((edge.source.worker_id, edge.destination.worker_id, edge.edge_type), edge))
            .aggregate::<_,(u64, u128, u64),_,_,_>(
                |_key, edge, acc| {
                    *acc = (acc.0 + 1,
                            acc.1 + edge.duration(),
                            acc.2 + edge.length.unwrap_or(0) as u64);
                },
                |key, acc| (key.0, key.1, key.2, acc.0, acc.1, acc.2),
                |key| calculate_hash(key))
            .inspect_time(move |t,x| expect_write(writeln!(*file.lock().unwrap(), "{:?},{},{},{:?},{},{},{}", t.first - 1, x.0, x.1, x.2, x.3, x.4, x.5)))
            .map(|_| ())
    }
}

/// Unwraps a write.
fn expect_write(e: Result<(), std::io::Error>) {
    e.expect("write failed");
}
