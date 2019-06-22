use logformat::pair::Pair;
use std::time::Duration;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::channels::pushers::{Counter as PushCounter, buffer::Buffer as PushBuffer};
use timely::dataflow::operators::generic::builder_raw::OperatorBuilder;
use timely::dataflow::operators::probe::Probe;

pub fn throttle<G>(
    scope: &G
) -> Stream<G, u64>
where
    G: Scope<Timestamp=Pair<u64, Duration>>,
{
    let mut builder = OperatorBuilder::new("ReplayThrottled".to_owned(), scope.clone());

    let address = builder.operator_info().address;
    let activator = scope.activator_for(&address[..]);

    let (targets, stream) = builder.new_output();
    let mut output = PushBuffer::new(PushCounter::new(targets));

    let mut input_round = 0;

    builder.build(
        move |_frontier| { },
        move |_consumed, internal, produced| {
            if input_round < 50000 {
                output.session(&Pair::new(input_round, Duration::new(0,0))).give_iterator(input_round * 10 .. input_round * 10 + 10);

                let vec = vec![(Pair::new(input_round + 1, Duration::new(0,0)), 1), (Pair::new(input_round, Duration::new(0,0)), -1)];
                internal[0].extend(vec.iter().cloned());

                input_round += 1;
            } else {
                let vec = vec![(Pair::new(input_round, Duration::new(0,0)), -1)];
                internal[0].extend(vec.iter().cloned());
            }

            activator.activate();

            output.cease();
            output.inner().produced().borrow_mut().drain_into(&mut produced[0]);

            false
        }
    );
    stream
}

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let index = worker.index();

        let probe = worker.dataflow(|scope| {
            throttle(scope)
                .inspect_batch(move |t, x| {
                    std::thread::sleep_ms(10);
                    println!("w{}@{:?}: {:?}", index, t, x);
                })
                .probe()
        });

        while !probe.done() {
            let frontier = probe.with_frontier(|frontier| frontier.to_vec());
            println!("{:?}", frontier);
            worker.step();
        }

        println!("w{} done", index);
    }).unwrap();
}
