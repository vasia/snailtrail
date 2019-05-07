use timely_adapter::{
    connect::{make_replayers, open_sockets},
    make_log_records,
};
use timely_snailtrail::{pag, Config};

use timely::dataflow::{
    operators::{capture::replay::Replay, probe::Probe},
    ProbeHandle,
};

use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::input::Input;

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let timer = std::time::Instant::now();

        let (mut blacklist, mut input, probe) = worker.dataflow(|scope| {
            let (blacklist_handle, blacklist) = scope.new_collection();
            let (input_handle, input) = scope.new_collection();

            let probe = input
                .antijoin(&blacklist)
                .consolidate()
                .probe();

            (blacklist_handle, input_handle, probe)
        });

        blacklist.advance_to(0);
        blacklist.flush();
        blacklist.insert(3);
        drop(blacklist);

        input.advance_to(2);
        input.flush();
        for i in 1 .. 10_00000 {
            input.insert((3, i));
        }

        // Inserting additional times doesn't affect
        // consolidate at all.
        // input.advance_to(3);
        // input.flush();

        for i in 10_00000 .. 11_00000 {
            input.insert((3, i));
        }

        for i in 11_00000 .. 12_00000 {
            input.insert((4, i));
        }

        // Inserting additional times doesn't affect
        // consolidate at all.

        // input.advance_to(4);
        // input.flush();

        for i in 12_00000 .. 13_00000 {
            input.insert((4, i));
        }

        input.advance_to(5);
        input.flush();

        for i in 0 .. 10 {
            input.insert((5, i));
        }

        // drop(blacklist);
        // drop(input);

        let mut curr_frontier = vec![];
        while probe.less_equal(&4) {
            probe.with_frontier(|f| {
                let f = f.to_vec();
                if f != curr_frontier {
                    println!("frontier: {:?}", f);
                    curr_frontier = f;
                }
            });
            worker.step();
        }

        // Consolidate reorders by value.

        println!("done: {}ms", timer.elapsed().as_millis());
    })
    .unwrap();
}
