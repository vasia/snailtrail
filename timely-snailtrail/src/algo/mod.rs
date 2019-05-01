//! Algorithms to be run on the PAG

use std::{io::Read, time::Duration};

use differential_dataflow::{
    input::{Input, InputSession},
    operators::{arrange::ArrangeByKey, iterate::Iterate, join::JoinCore, reduce::Threshold},
};

use timely::{communication::Allocate, dataflow::ProbeHandle, worker::Worker};

use timely_adapter::connect::Replayer;

use crate::pag;
use crate::pag::{PagEdge, PagNode};


/// PAG Nodes from which a computation can start
pub type Whitelist = InputSession<Duration, (u64, PagNode), isize>;

/// PAG Edges which might be removed during a computation
pub type Blacklist = InputSession<Duration, (u64, PagEdge), isize>;

/// Counts path length in the PAG generated from `replayers`, starting from nodes in the `whitelist`.
/// To simulate differential behavior on a changing PAG (this hopefully
/// does not happen in a real-world setting), edges in the `blacklist`
/// are stripped from the PAG.
pub fn path_length<R: 'static + Read, A: Allocate>(
    worker: &mut Worker<A>,
    replayers: Vec<Replayer<R>>,
) -> (
    ProbeHandle<Duration>,
    Whitelist,
    Blacklist,
) {
    worker.dataflow(|scope| {
        let (blacklist_handle, blacklist) = scope.new_collection();
        let (whitelist_handle, whitelist) = scope.new_collection();

        let pag_by_source = pag::create_pag(scope, replayers)
            .filter(|(epoch, _x)| epoch == &4) // @TODO: remove
            .concat(&blacklist)
            .map(|(epoch, x)| ((epoch, x.source), x.destination))
            .arrange_by_key();

        let probe = whitelist
            .map(|(epoch, x)| ((epoch, x), 1))
            .iterate(|dists| {
                let pag_by_source = pag_by_source.enter(&dists.scope());

                dists
                    .join_core(&pag_by_source, |(epoch, _start), dist, dest| {
                        Some(((*epoch, *dest), dist + 1))
                    })
                    .concat(dists)
                    .distinct()
            })
            .inspect(|x| println!("{:?}", x))
            .probe();

        (probe, whitelist_handle, blacklist_handle)
    })
}
