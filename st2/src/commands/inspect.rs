use crate::pag;

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::capture::EventReader;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;

use std::time::Duration;
use std::time::Instant;

use logformat::pair::Pair;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use crate::STError;

/// Inspects a running SnailTrail computation, e.g. for benchmarking of SnailTrail itself.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource) -> Result<(), STError> {

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        // read replayers from file (offline) or TCP stream (online)
        let readers: Vec<EventReader<_, timely_adapter::connect::CompEvent, _>> =
            connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        let probe: ProbeHandle<Pair<u64, Duration>> = worker.dataflow(|scope| {
            // use timely::dataflow::operators::inspect::Inspect;
            // use timely_adapter::replay_throttled::ReplayThrottled;
            // use timely_adapter::ConstructLRs;
            // readers
            //     .replay_throttled_into(index, scope, None, throttle)
            //     .peel_ops(index)
            //     .inspect(|x| println!("{:?}", x))
            //     .probe()

            pag::create_pag(scope, readers, index, 1)
                // .bench(index)
                .probe()
        });

        while !probe.done() { worker.step_or_park(None); };

        info!("w{} done", index);

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}


/// Benchmarks epoch duration & # of events passing through
trait Benchmark<S: Scope<Timestamp = Pair<u64, Duration>>, D: Data> {
    /// Benchmark epoch duration & # of events passing through.
    fn bench(&self, index: usize) -> Stream<S, D>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>, D: Data> Benchmark<S, D> for Stream<S, D> {
    fn bench(&self, index: usize) -> Stream<S, D> {
        self.unary_frontier(Pipeline, "benchmark", move |_cap, _info| {
            let mut buffer = Vec::new();
            let mut t = Instant::now();

            move |input, output: &mut OutputHandle<_, D, _>| {
                let mut count = 0;

                input.for_each(|cap, data| {
                    data.swap(&mut buffer);
                    count += buffer.len();
                    output.session(&cap).give_vec(&mut buffer);
                });

                if let Some(f) = input.frontier.frontier().get(0) {
                    println!("{}|{}|{}|{}", index, f.first, t.elapsed().as_nanos(), count);
                    t = Instant::now();
                }
            }
        })
    }
}
