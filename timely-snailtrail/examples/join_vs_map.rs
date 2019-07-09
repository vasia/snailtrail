use logformat::pair::Pair;
use std::time::Duration;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::channels::pushers::{Counter as PushCounter, buffer::Buffer as PushBuffer};
use timely::dataflow::operators::generic::builder_raw::OperatorBuilder;
use timely::dataflow::operators::probe::Probe;
use differential_dataflow::input::Input;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let index = worker.index();

        let (mut a, mut b, probe) = worker.dataflow(|scope| {
            let (a, a_d) = scope.new_collection();
            let (b, b_d) = scope.new_collection();

            let a_d = a_d.map(|x| (x, x)).arrange_by_key();
            // let a_d = a_d2.arrange_by_key();
            let b_d = b_d.map(|x| (x, x)).arrange_by_key();

            // 2w map + inspect: 15.4 (~650K / s)
            // 2w map + arrange + inspect: ~35.4 (~280K / s)
            // 2w map + arrange + join + inspect: ~120 (~80K / s)
            // 2w map + join + inspect: ~120 (~80K / s)

            let probe = a_d
                // .join_core(&b_d, |_, &a, _| Some(a))
                .inspect_batch(move |t, x| println!("w{}@{:?}", index, t))
                .probe();

            (a, b, probe)
        });

        for i in 0 .. 10_000_000 {
            if index == 0 {
                a.insert(i);
            } else {
                b.insert(i + 1);
            }

            if i % 1_000 == 0 {
                a.advance_to(i/1000);
                b.advance_to(i/1000);
                a.flush();
                b.flush();
            }

            while probe.less_than(&a.time()) || probe.less_than(&b.time()) {
                let frontier = probe.with_frontier(|frontier| frontier.to_vec());
                println!("{:?}", frontier);
                worker.step();
            }

        }

        println!("w{} done", index);
    }).unwrap();
}
