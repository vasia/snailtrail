//! Data structure for a `LogRecord`.
//! A `LogRecord` constitutes the unified `struct` representation of
//! log messages from various stream processors.
//!
//! It is the underlying structure from which the PAG construction starts.
//! If necessary, it can also be serialized e.g. into a `msgpack` representation.

#![deny(missing_docs)]

#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate abomonation_derive;

use std::io::{Write, Read};
use std::cmp::Ordering;
use num_traits::{FromPrimitive, ToPrimitive};

use msgpack::decode::ValueReadError;
use rmp as msgpack;


/// This module contains a definition of a new timestamp time, a "pair" or product.
///
/// This is a minimal self-contained implementation, in that it doesn't borrow anything
/// from the rest of the library other than the traits it needs to implement. With this
/// type and its implementations, you can use it as a timestamp type.
pub mod pair {

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

    // Implement timely dataflow's `PartialOrder` trait.
    use timely::order::PartialOrder;
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    use timely::progress::timestamp::Refines;
    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Default::default() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    // Implement timely dataflow's `PathSummary` trait.
    // This is preparation for the `Timestamp` implementation below.
    use timely::progress::PathSummary;

    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> {
            Some(timestamp.clone())
        }
        fn followed_by(&self, other: &Self) -> Option<Self> {
            Some(other.clone())
        }
    }

    // Implement timely dataflow's `Timestamp` trait.
    use timely::progress::Timestamp;
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        type Summary = ();
    }

    // Implement differential dataflow's `Lattice` trait.
    // This extends the `PartialOrder` implementation with additional structure.
    use differential_dataflow::lattice::Lattice;
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

    use std::fmt::{Formatter, Error, Debug};

    /// Debug implementation to avoid seeing fully qualified path names.
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }

}

/// The various types of activity that can happen in a dataflow.
/// `Unknown` et al. shouldn't be emitted by instrumentation. Instead,
/// they might be inserted as helpers during PAG construction.
#[derive(Primitive, Abomonation, PartialEq, Debug, Clone, Copy, Hash, Eq, PartialOrd, Ord)]
pub enum ActivityType {
    /// Input introduced to the dataflow
    Input = 1,
    /// Data buffered at an operator (?)
    Buffer = 2,
    /// Operator scheduled for work
    Scheduling = 3,
    /// Operator actually doing work
    Processing = 4,
    /// Barrier processing activities
    BarrierProcessing = 5,
    /// Data serialization
    Serialization = 6,
    /// Data deserialization
    Deserialization = 7,
    /// Fault tolerance activities
    FaultTolerance = 8,
    /// remote control messages, e.g. about progress
    ControlMessage = 9,
    /// remote data messages, e.g. moving tuples around
    DataMessage = 10,
    /// Unknown message, used during PAG construction
    /// (not emitted by profiling)
    Unknown = 11,
    /// Waiting for unblocking.
    /// In particular, operator might wait for external input.
    /// (not emitted by profiling)
    Waiting = 12,
    /// Waiting where next activity is actively prepared,
    /// e.g. in-between a ScheduleEnd and consecutive ScheduleStart.
    /// In particular, operator doesn't depend on external input.
    /// (not emitted by profiling)
    BusyWaiting = 13,
}

impl ActivityType {
    /// Mapps activity types to whether this activity is local
    /// to a worker
    pub fn is_worker_local(&self) -> bool {
        match *self {
            ActivityType::Input => true,
            ActivityType::Buffer => true,
            ActivityType::Scheduling => true,
            ActivityType::Processing => true,
            ActivityType::BarrierProcessing => true,
            ActivityType::Serialization => true,
            ActivityType::Deserialization => true,
            ActivityType::FaultTolerance => false,
            ActivityType::ControlMessage => false,
            ActivityType::DataMessage => false,
            ActivityType::Unknown => true,
            ActivityType::Waiting => true,
            ActivityType::BusyWaiting => true,
        }
    }
}

/// What "side" of the event did we log? E.g., for
/// scheduling events, it might be the start or end of the event;
/// for messages, we might log the sender or receiver.
#[derive(Primitive, Abomonation, PartialEq, Eq, PartialOrd, Ord, Debug, Hash, Clone, Copy)]
pub enum EventType {
    /// Start of an event (e.g. Scheduling)
    Start = 1,
    /// End of an event (e.g. Scheduling)
    End = 2,
    /// Sender end of an event (e.g. a data message)
    Sent = 3,
    /// Receiver end of an event (e.g. a data message)
    Received = 4,
    /// Some event that doesn't make sense
    Bogus = 5,
}

/// Potential errors while reading a serialized log
#[derive(Debug, Clone)]
pub enum LogReadError {
    /// end of file error
    Eof,
    /// decoding error
    DecodeError(String),
}

impl From<String> for LogReadError {
    fn from(msg: String) -> Self {
        LogReadError::DecodeError(msg)
    }
}

impl<'a> From<&'a str> for LogReadError {
    fn from(msg: &'a str) -> Self {
        LogReadError::DecodeError(msg.to_owned())
    }
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

// ***************************************************************************
// * Please always update tests and java/c++ code after changing the schema. *
// ***************************************************************************

/// A `LogRecord` constitutes the unified `struct` representation of
/// log messages from various stream processors.
///
/// It is the underlying structure from which the PAG construction starts.
/// If necessary, it can also be serialized e.g. into a `msgpack` representation.
#[derive(Abomonation, PartialEq, Eq, Hash, Debug, Clone)]
pub struct LogRecord {
    /// seq_no
    pub seq_no: u64,
    /// epoch
    pub epoch: u64,
    /// Event time in nanoseconds since the Epoch (midnight, January 1, 1970 UTC).
    pub timestamp: Timestamp,
    /// Context this event occured in; denotes which of the parallel timelines it belongs to.
    pub local_worker: Worker,
    /// Describes the instrumentation point which triggered this event.
    pub activity_type: ActivityType,
    /// Identifies which end of an edge this program event belongs to.
    /// E.g. start or end for scheduling, sent or received for communication events.
    pub event_type: EventType,
    /// Opaque label used to group the two records belonging to a program activity.
    pub correlator_id: Option<CorrelatorId>,
    /// Similar to `local_worker` but specifies the worker ID for the other end of a sent/received message.
    pub remote_worker: Option<Worker>,
    /// Unique id for the operator in the dataflow. This only applies for some event types, e.g. scheduling or processing.
    pub operator_id: Option<OperatorId>,
    /// Unique id for the channel in the dataflow. This only applies for some event types, e.g. data / control messages.
    pub channel_id: Option<ChannelId>,
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


impl std::fmt::Display for LogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LR @ {:?} w{}\t{:?} {:?}\tcorr: {:?}\tremote: {:?}\top: {:?}\tch: {:?}", self.timestamp, self.local_worker, self.event_type, self.activity_type, self.correlator_id, self.remote_worker, self.operator_id, self.channel_id)
    }
}
