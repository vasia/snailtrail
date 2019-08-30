use crate::pag;
use crate::pag::PagEdge;
use crate::STError;
use crate::PagData;
use crate::commands::algo::{KHops, KHopsSummary};
use crate::{MetricsData, KHopSummaryData};
use crate::commands::metrics::Metrics;

use timely::dataflow::Stream;
use timely::dataflow::operators::inspect::Inspect;

use std::time::Duration;
use std::sync::mpsc;
use std::sync::{Mutex, Arc};
use std::convert::TryInto;

use st2_logformat::pair::Pair;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;


/// Creates an online dashboard for ST2.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource,
    pag_send: Arc<Mutex<mpsc::Sender<(u64, PagData)>>>) -> Result<(), STError> {

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        let pag_send1 = pag_send.lock().expect("cannot lock pag_send").clone();
        let pag_send2 = pag_send.lock().expect("cannot lock pag_send").clone();
        let pag_send3 = pag_send.lock().expect("cannot lock pag_send").clone();
        let pag_send4 = pag_send.lock().expect("cannot lock pag_send").clone();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, 1);

            // log PAG to socket
            pag.inspect(move |(x, t, _)| {
                pag_send3
                    .send((t.first, PagData::Pag(x.clone())))
                    .expect("couldn't send pagedge")
            });

            let khops = pag.khops(2);

            // log khops edges to socket
            khops.inspect(move |((x, _), t, _diff)| {
                pag_send1
                    .send((t.first - 1, PagData::All((x.source.timestamp.as_nanos().try_into().unwrap(), x.destination.timestamp.as_nanos().try_into().unwrap()))))
                    .expect("khops_edges")
            });

            let khops_summary = khops.khops_summary();

            // log khops summary to socket
            khops_summary.inspect(move |(((a, wf), (ac, wac)), t, diff)| {
                if *diff > 0 {
                    pag_send2
                        .send((t.first - 1, PagData::Agg(KHopSummaryData {a: *a, wf: *wf, ac: *ac, wac: *wac })))
                        .expect("khops_summary")
                }
            });


            let metrics = pag.metrics();

            // log metrics to socket
            metrics.inspect_time(move |t, x| {
                pag_send4
                    .send((t.first - 1, PagData::Met(MetricsData {
                        wf: x.0,
                        wt: x.1,
                        a: x.2,
                        ac: x.3,
                        at: x.4,
                        rc: x.5,
                    })))
                    .expect("metrics")
            });
        });
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}
