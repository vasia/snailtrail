//! Data structure for a `LogRecord`, `Pair` two-dimensional time type.
//! A `LogRecord` constitutes the unified `struct` representation of
//! log messages from various stream processors.
//! It is the underlying structure from which the PAG construction starts.

#![deny(missing_docs)]

#[macro_use]
extern crate abomonation_derive;

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// The various types of activity that can happen in a dataflow.
/// `Unknown` et al. shouldn't be emitted by instrumentation. Instead,
/// they might be inserted as helpers during PAG construction.
#[derive(Abomonation, PartialEq, Debug, Clone, Copy, Hash, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum ActivityType {
    /// Operator scheduled. Used as temporary state for `LogRecord`s
    /// where it's still unclear whether they were only spinning or
    /// also did some work
    Scheduling = 0,
    /// Operator actually doing work
    Processing = 2,
    /// Operator scheduled, but not doing any work
    Spinning = 1,
    /// Data serialization
    Serialization = 3,
    /// Data deserialization
    Deserialization = 4,
    /// remote control messages, e.g. about progress
    ControlMessage = 5,
    /// remote data messages, e.g. moving tuples around
    DataMessage = 6,
    /// Waiting for unblocking.
    /// In particular, operator might wait for external input.
    /// (not emitted by profiling)
    Waiting = 8,
    /// Waiting where next activity is actively prepared,
    /// e.g. in-between a ScheduleEnd and consecutive ScheduleStart.
    /// In particular, operator doesn't depend on external input.
    /// (not emitted by profiling)
    Busy = 9,
}

/// What "side" of the event did we log? E.g., for
/// scheduling events, it might be the start or end of the event;
/// for messages, we might log the sender or receiver.
#[derive(Abomonation, PartialEq, Eq, PartialOrd, Ord, Debug, Hash, Clone, Copy)]
pub enum EventType {
    /// Start of an event (e.g. ScheduleStart, Sending a Message)
    Start = 1,
    /// End of an event (e.g. ScheduleEnd, Receiving a Message)
    End = 2,
    /// Sender end of an event (e.g. a data message)
    Sent = 3,
    /// Receiver end of an event (e.g. a data message)
    Received = 4,
}

/// A worker ID
pub type Worker = u64;
/// An event timestamp
pub type Timestamp = std::time::Duration;
/// Type used as identifiers for (mapping between) two event sides
pub type CorrelatorId = u64;
/// A worker-local operator ID
pub type OperatorId = u64;
/// A worker-local channel ID
pub type ChannelId = u64;


/// A `LogRecord` constitutes the unified `struct` representation of
/// log messages from various stream processors.
///
/// It is the underlying structure from which the PAG construction starts.
/// If necessary, it can also be serialized e.g. into a `msgpack` representation.
#[derive(Abomonation, PartialEq, Eq, Hash, Clone, Debug)]
pub struct LogRecord {
    /// worker-unique identifier of a message, given in order the events are logged
    /// in the computation.
    pub seq_no: u64,
    /// epoch of the computation this record belongs to
    pub epoch: u64,
    /// Event time in nanoseconds since the Epoch (midnight, January 1, 1970 UTC).
    pub timestamp: Timestamp,
    /// Context this event occured in; denotes which of the parallel timelines it belongs to.
    pub local_worker: Worker,
    /// Describes the instrumentation point which triggered this event.
    pub activity_type: ActivityType,
    /// Identifies which end of an edge this program event belongs to.
    pub event_type: EventType,
    /// Similar to `local_worker` but specifies the worker ID for the other end of a sent/received message.
    pub remote_worker: Option<Worker>,
    /// Unique id for the operator in the dataflow. This only applies for some event types, e.g. scheduling or processing.
    pub operator_id: Option<OperatorId>,
    /// Unique id for the channel in the dataflow. This only applies for some event types, e.g. data / control messages.
    pub channel_id: Option<ChannelId>,
    /// correlates remote events belonging together
    pub correlator_id: Option<u64>,
    /// Number of records to detect skew
    pub length: Option<usize>,
}

impl Ord for LogRecord {
    fn cmp(&self, other: &LogRecord) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for LogRecord {
    fn partial_cmp(&self, other: &LogRecord) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


/// This module contains a definition of a new timestamp time, a "pair" or product.
///
/// Note: Its partial order trait is modified so that it follows a lexicographical order;
/// It is not truly partially ordered (cf. the `compare_pairs` test)!
pub mod pair {
    use differential_dataflow::lattice::Lattice;

    use timely::{
        order::{PartialOrder, TotalOrder},
        progress::{timestamp::Refines, PathSummary, Timestamp}
    };

    use std::fmt::{Formatter, Error, Debug};

    /// A pair of timestamps, partially ordered by the product order.
    #[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Abomonation)]
    pub struct Pair<S, T> {
        /// first part of timestamp
        pub first: S,
        /// second part of timestampe
        pub second: T,
    }

    impl<S, T> Pair<S, T> {
        /// Create a new pair.
        pub fn new(first: S, second: T) -> Self {
            Pair { first, second }
        }
    }

    /// Implement timely dataflow's `PartialOrder` trait.
    /// Note: This is in fact a total order implementation!
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_than(&other.first) ||
               self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    impl<S: TotalOrder, T: TotalOrder> TotalOrder for Pair<S, T> {}

    #[test]
    fn compare_pairs() {
        assert!(Pair::new(0, 0).less_equal(&Pair::new(0,0)));
        assert!(Pair::new(0, 0).less_equal(&Pair::new(0,1)));
        assert!(Pair::new(0, 0).less_equal(&Pair::new(1,0)));
        assert!(Pair::new(0, 1).less_equal(&Pair::new(1,0)));
        assert!(Pair::new(1, 0).less_equal(&Pair::new(1,0)));
        assert!(Pair::new(1, 0).less_equal(&Pair::new(1,1)));
        assert!(!Pair::new(1, 0).less_equal(&Pair::new(0,1)));

        assert!(!Pair::new(1, 1).less_equal(&Pair::new(0,1000)));
        assert!(Pair::new(0, 1000).less_equal(&Pair::new(1, 1)));
    }

    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Default::default() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    /// Implement timely dataflow's `PathSummary` trait.
    /// This is preparation for the `Timestamp` implementation below.
    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> {
            Some(timestamp.clone())
        }
        fn followed_by(&self, other: &Self) -> Option<Self> {
            Some(other.clone())
        }
    }

    /// Implement timely dataflow's `Timestamp` trait.
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        type Summary = ();
    }

    /// Implement differential dataflow's `Lattice` trait.
    /// This extends the `PartialOrder` implementation with additional structure.
    impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
        fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() }}
        fn join(&self, other: &Self) -> Self {
            Pair {
                first: self.first.join(&other.first),
                second: self.second.join(&other.second),
            }
        }
        fn meet(&self, other: &Self) -> Self {
            Pair {
                first: self.first.meet(&other.first),
                second: self.second.meet(&other.second),
            }
        }
    }


    /// Debug implementation to avoid seeing fully qualified path names.
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }
}
