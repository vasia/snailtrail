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

use std::cmp::Ordering;

#[macro_use]
extern crate abomonation_derive;

#[derive(Abomonation, PartialEq, Eq, Hash, Debug, Clone, PartialOrd, Ord)]
struct Dupel {
    first: u64,
    second: u64,
}

// impl Ord for Dupel {
//     fn cmp(&self, other: &Dupel) -> Ordering {
//         if self.first < other.first {
//             Ordering::Less
//         } else if self.first > other.first {
//             Ordering::Greater
//         } else {
//             if self.second < other.second {
//                 Ordering::Greater // !
//             } else if self.second > other.second {
//                 Ordering::Less
//             } else {
//                 Ordering::Equal
//             }
//         }
//     }
// }

// impl PartialOrd for Dupel {
//     fn partial_cmp(&self, other: &Dupel) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

#[test]
fn comparisons() {
    assert!(Dupel { first: 1, second: 10 } < Dupel { first: 1, second: 8 });
    assert!(Dupel { first: 1, second: 5 } > Dupel { first: 1, second: 8 });
    assert!(Dupel { first: 1, second: 5 } < Dupel { first: 2, second: 8 });
    assert!(Dupel { first: 2, second: 10 } == Dupel { first: 2, second: 10 });
}


fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let timer = std::time::Instant::now();

        let index = worker.index();

        let (mut blacklist, mut input, probe) = worker.dataflow(|scope| {
            let (blacklist_handle, blacklist) = scope.new_collection();
            let (input_handle, input) = scope.new_collection();

            use differential_dataflow::operators::reduce::Count;
            use timely::dataflow::operators::inspect::Inspect;
            use differential_dataflow::collection::AsCollection;

            let probe = input
                .antijoin(&blacklist)
                .map(|x: (u64, u64)| Dupel { first: x.0, second: x.1})
                .inner
                .inspect_time(|t, x| { if index == 0 {println!("a {}@{:?} : {:?}", index, t, x); }})
                .as_collection()
                .consolidate()
                .inner
                .inspect_time(|t, x| { if index == 0 {println!("b {}@{:?} : {:?}", index, t, x); }})
                .as_collection()
                .map(|_| 0)
                .count()
                .inspect(|x| println!("{:?}", x))
                .probe();

            (blacklist_handle, input_handle, probe)
        });

        blacklist.advance_to(Pair::new(0,0));
        blacklist.flush();
        blacklist.insert(3);
        drop(blacklist);

        input.advance_to(Pair::new(2, 2));
        input.flush();
        // for i in 1 .. 10_00000 {
        //     input.insert((3, i));
        // }

        // Inserting additional times doesn't affect
        // consolidate at all.
        // input.advance_to(3);
        // input.flush();

        // for i in 10_00000 .. 11_00000 {
        //     input.insert((3, i));
        // }

        // for i in 11_00000 .. 12_00000 {
        //     input.insert((4, i));
        // }

        // Inserting additional times doesn't affect
        // consolidate at all.

        // input.advance_to(4);
        // input.flush();

        for i in (12_00000 .. 13_00000).rev() {
            input.insert((4, i));
        }

        // input.advance_to(3);
        // input.flush();

        std::thread::sleep_ms(1000);

        input.advance_to(Pair::new(2, 3));
        input.flush();

        for i in (14_00000 .. 15_00000).rev() {
            input.insert((4, i));
        }

        // input.advance_to(4);
        // input.flush();

        std::thread::sleep_ms(1000);

        input.advance_to(Pair::new(2, 4));
        input.flush();

        for i in (13_00000 .. 14_00000).rev() {
            input.insert((4, i));
        }

        input.advance_to(Pair::new(5, 5));
        input.flush();
        // for i in 0 .. 10 {
        //     input.insert((5, i));
        // }

        // drop(blacklist);
        // drop(input);

        let mut curr_frontier = vec![];
        while probe.less_equal(&Pair::new(2, 4)) {
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
