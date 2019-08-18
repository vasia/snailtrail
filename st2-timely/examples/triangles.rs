//! This is an exemplary dataflow that includes instrumentation to be used by SnailTrail.
//!
//! For barebones logging of TimelyEvents, env var `TIMELY_WORKER_LOG_ADDR=<IP:Port>` can
//! be passed. This then logs every message handled by any worker.

#[macro_use]
extern crate log;

use differential_dataflow::input::Input;

use graph_map::GraphMMap;

use timely::dataflow::operators::probe::Handle;
use timely::dataflow::Scope;
use timely::dataflow::ProbeHandle;
use timely::communication::allocator::Generic;

use st2_timely::connect::Adapter;

use dogsdogsdogs::ProposeExtensionMethod;
use dogsdogsdogs::{altneu::AltNeu, CollectionIndex};

fn main() {
    env_logger::init();

    // snag a filename to use for the input graph.
    let mut args = std::env::args();

    info!("CLAs: {:?}", args);

    let _ = args.next(); // bin name
    // let filename = args.next().expect("file name");
    let filename = "livejournal.graph".to_string();
    let batch_size = args.next().expect("missing batch size").parse::<usize>().unwrap();
    let rounds = args.next().expect("missing rounds").parse::<usize>().unwrap();
    // let tuples_file = args.next().expect("missing tuples file").parse::<String>().unwrap();
    let _ = args.next(); // --

    let args = args.collect::<Vec<_>>();
    timely::execute_from_args(args.clone().into_iter(), move |worker| {
        // SnailTrail adapter
        let adapter = Adapter::attach(worker);

        info!("triangles with args: {:?}, ws{}", args, worker.peers());
        // comment in to track tuples
        // let mut file = File::create(format!("{}_{}.csv", tuples_file, worker.index())).expect("couldn't create file");

        // dataflow
        let mut probe = Handle::new();
        let mut input = triangles_query(worker, &mut probe);


        let graph = GraphMMap::new(&filename);
        let peers = worker.peers();
        let index = worker.index();
        // let mut count = 0;
        let mut timer = std::time::Instant::now();

        // Note: parallelization makes this computation more complex (as `n` times the
        // input is inserted). We're not interested in speeding up triangles, but measuring
        // how fast SnailTrail can process the generated log traces.
        // Similarly, a higher batch slows down the computation as well, as all edges for the
        // `curr_node` are mindlessly inserted `batch_size` times
        for i in 0..rounds {
            // for w0 in 4w comp: 0, 5, 10, ...
            let curr_node = i * peers + index;
            for _ in 0 .. batch_size {
                for &edge in graph.edges(curr_node) {
                    input.insert((curr_node, edge as usize));
                    // count += 1;
                }
            }
            // writeln!(file, "{}|{}|{}", index, i, count).expect("write failed");
            // count = 0;

            input.advance_to(i + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }

            if index == 0 {
                info!("w{} {:?}\tEpoch {} complete, close times before: {:?}", index, timer.elapsed(), i, input.time());
                timer = std::time::Instant::now();
            }

            adapter.tick_epoch();
        }
    })
    .unwrap();
}

fn triangles_query(worker: &mut timely::worker::Worker<Generic>, probe: &mut ProbeHandle<usize>) -> differential_dataflow::input::InputSession<usize, (usize, usize), isize> {
    worker.dataflow::<usize, _, _>(|scope| {
        let (edges_input, edges) = scope.new_collection();

        let forward = edges.clone();
        let reverse = edges.map(|(x, y)| (y, x));

        // Q(a,b,c) :=  E1(a,b),  E2(b,c),  E3(a,c)
        let triangles = scope.scoped::<AltNeu<usize>, _, _>("DeltaQuery (Triangles)", |inner| {
            let forward = forward.enter(inner);
            let reverse = reverse.enter(inner);

            let alt_forward = CollectionIndex::index(&forward);
            let alt_reverse = CollectionIndex::index(&reverse);
            let neu_forward = CollectionIndex::index(
                &forward.delay(|time| AltNeu::neu(time.time.clone())),
            );
            let neu_reverse = CollectionIndex::index(
                &reverse.delay(|time| AltNeu::neu(time.time.clone())),
            );

            let changes1 = forward
                .extend(&mut [
                    &mut neu_forward.extend_using(|(_a, b)| *b),
                    &mut neu_forward.extend_using(|(a, _b)| *a),
                ])
                .map(|((a, b), c)| (a, b, c));

            let changes2 = forward
                .extend(&mut [
                    &mut alt_reverse.extend_using(|(b, _c)| *b),
                    &mut neu_reverse.extend_using(|(_b, c)| *c),
                ])
                .map(|((b, c), a)| (a, b, c));

            let changes3 = forward
                .extend(&mut [
                    &mut alt_forward.extend_using(|(a, _c)| *a),
                    &mut alt_reverse.extend_using(|(_a, c)| *c),
                ])
                .map(|((a, c), b)| (a, b, c));

            changes1.concat(&changes2).concat(&changes3).leave()
        });

        triangles.probe_with(probe);

        edges_input
    })
}
