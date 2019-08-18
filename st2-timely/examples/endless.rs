use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};

use differential_dataflow::operators::join::Join;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;

use st2_timely::connect::Adapter;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // (A) Create SnailTrail adapter at the beginning of the worker closure
        let adapter = Adapter::attach(worker);

        // Some computation
        let mut input = InputSession::new();
        let probe = worker.dataflow(|scope| {
            let input = input.to_collection(scope);

            let relations = input.arrange_by_key();

            // Stream of (curr_node, (root, root_distance))
            let start = input.map(|(manager, _managed)| (manager, (manager, 0)));

            start
                .iterate(|distances| {
                    let relations = relations.enter(&distances.scope());
                    distances
                        .join_core(&relations, |&curr_node, &(root, root_distance), &managed| Some((managed, (root, root_distance + 1))))
                        .concat(distances)
                        // infinite loop without distinct
                        .distinct()
                })
                .inspect(|((last, (root, root_distance)), t, diff)| println!("{}-[{}]->{} ({}, {})", root, root_distance, last, t, diff))
                .probe()

            // @TODO: MWEs for differential log messages requirement (cf. pag.rs TODO)
            // let arranged_input = input.arrange_by_key();
            // let arranged_managed = input.map(|(manager, managed)| (managed, manager)).arrange_by_key();
            // arranged_managed
                // .join_core(&arranged_input, |&key, &a, &b| Some((key, (a, b))))
                // .inspect(move |((managed, (manager, even_lower)), _t, _diff)| println!("hello {}, {}, {}", manager, managed, even_lower))
                // .probe()
            // input
                // .map(|(manager, managed)| (managed, manager))
                // .join(&input)
                // .inspect(move |((managed, (manager, even_lower)), _t, _diff)| println!("hello {}, {}, {}", manager, managed, even_lower))
                // .probe()
        });

        // infinite loop if starting from 0
        for round in 1..100 {
            if worker.index() == 0 { input.insert((round/2, round)); }

            input.advance_to(round + 1);
            input.flush();

            while probe.less_than(input.time()) { worker.step(); }

            // (B) Communicate epoch completion
            adapter.tick_epoch();
        }
    }).unwrap();
}
