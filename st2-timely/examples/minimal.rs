use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};

use st2_timely::connect::Adapter;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // (A) Create SnailTrail adapter at the beginning of the worker closure
        let adapter = Adapter::attach(worker);

        // Some computation
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope|
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("hello {}", x))
                 .probe()
        );

        for round in 0..100 {
            if worker.index() == 0 { input.send(round); }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) { worker.step(); }

            // (B) Communicate epoch completion
            adapter.tick_epoch();
        }
    }).unwrap();
}
