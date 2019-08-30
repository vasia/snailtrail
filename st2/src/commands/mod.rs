//! Subcommand modules.
//!
//! Each of the program's subcommand logic is in a separate module here.

/// PAG visualization
pub mod viz;
/// Aggregate metrics export
pub mod metrics;
/// ST2 inspector
pub mod inspect;
/// ST2 graph algorithms
pub mod algo;
/// Invariants checker
pub mod invariants;
/// Online dashboard
pub mod dashboard;
