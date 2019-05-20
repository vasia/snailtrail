<p align="center">
  <img src="https://github.com/li1/snailtrail/raw/master/snail.png" width="250">
</p>

# SnailTrail

This is a fork of [SnailTrail](https://github.com/strymon-system/snailtrail), a tool to run online critical path analysis on various stream processors (see also the [SnailTrail NSDI'18 Paper](https://doi.org/10.3929/ethz-b-000228581)).

The fork builds upon the original repository and implements further algorithms for analyzing stream processors. It currently focuses on the 0.9 version of [Timely Dataflow and Differential Dataflow](https://github.com/timelydataflow) and won't refrain from breaking existing upstream abstractions (even though they should be relatively easy to add back in at a later point in time).

## Getting Started

To try out SnailTrail, decide between online (via TCP) and offline (from file) mode.

### Offline

1. First, start a computation you would like to log: As example, `cargo run --example triangles <input file> <batch size> <load balance factor> <#computation workers>` from within `timely-adapter`.

    E.g., `cargo run --example triangles livejournal.graph 100 3 -w2` will load the `livejournal.graph` to use in the triangles computation, which is started with a batch size of 100. It is distributed over two workers, which will each write out events to three files.
    
    If you don't have the triangles computation ready, you can try a very basic log that is easily tweakable with `cargo run --example custom_operator 3 -- -w2`.
2. Run SnailTrail to create a PAG: From `timely-snailtrail`, run `cargo run --example inspect <# SnailTrail workers> <# of (simulated) source computation workers> <from-file?>`. 

    E.g., `cargo run --example inspect 2 6 f` will run SnailTrail with two workers, reading from the 6 files (`2 workers * 3 load balance factor`) we generated in step 1.
3. This creates a PAG as a differential `Collection`, to log it and use it, tweak `timely-snailtrail/examples/inspect.rs`.

### Online

1. First start SnailTrail, similarly to Offline step 2, but without the `from-file?` flag set; e.g.: `cargo run --example inspect 2 6`.
2. Now, start the computation you would like to log with env variable `SNAILTRAIL_ADDR=localhost:8000`.

    E.g., just like in offline mode: `env SNAILTRAIL_ADDR=localhost:8000 cargo run --example triangles livejournal.graph 100 3 -w2`
3. This creates a PAG, to log it and use it, tweak `timely-snailtrail/examples/inspect.rs`.

### Debugging

Further debug logging of the examples and SnailTrail is provided by Rust's `log` and `env_log` crates. Passing `RUST_LOG=info` (or `trace`) as env variable when running examples should write out further logging to your `std::out`.

### Show me the code!

Check out the `Structure` section of this `README` for a high-level overview.

The "magic" mostly happens at 
- `connect.rs` for (1) logging a computation and (2) connecting to it from SnailTrail, 
- `timely-adapter/src/lib.rs` for the `LogRecord` creation,
- `timely-snailtrail/src/pag.rs` for the `PAG` creation, and the
- `inspect.rs`, `triangles.rs` and `custom_operator.rs` examples tying it all together.

*Disclaimer:* The PAG creation currently only creates local edges (I still need to update the remote edge creation for all the new stuff I added, they're commented out in `pag.rs`).

## Structure

### Overview

(Roughly in order of appearance)

#### In this repository

|Type | Crate    | Description |
| --------- | -------- | ----------- |
| adapter | `timely-adapter` | timely / differential 0.9 adapter |
| infrastructure | `logformat` | Shared definitions of core data types and serialization of traces. |
| infrastructure, algorithms | `timely-snailtrail` | PAG generation & algorithms for timely 0.9 with epochal semantics. |

#### Upstream

|Type | Crate    | Description |
| --------- | -------- | ----------- |
| adapter | `spark-parser` | Spark adapter |
| adapter | `tensorflow` | TensorFlow adapter |
| adapter | Flink  | not publicly available |
| adapter | Timely < 0.9 | not publicly available |
| adapter | Heron  | not publicly available |
| infrastructure | `logformat` | Shared definitions of core data types and serialization of traces (in Rust, Java). |
| infrastructure | `pag-construction` | Constructs the Program Activity Graph (PAG) from a flat stream of events which denote the start/end of computation and communication. Also has scripts to generate various plots. |
| algorithms | `snailtrail` | Calculates a ranking for PAG edges by computing how many times an edge appears in the set of all-pairs shortest paths ("critical participation", cf. the paper). |

### Adapters

Adapters read log traces from a stream processor (or a serialized representation) and convert the logged messages to `logformat`'s `LogRecord` representation. This representation can then be used for PAG construction.

Depending on the stream processor, window semantics also come into play here. For example, the `timely-adapter` currently uses an epoch-based window, which should make many algorithms on the PAG easier than working on a fixed window PAG.

### Infrastructure

Glue code, type definitions, (de)serialization, and intermediate representations that connect adapters to algorithms.

### Algorithms

Implementation of various algorithms that run on top of the PAG to provide insights into the analyzed distributed dataflow's health and performance.

## Docs

See the `docs` subfolder for additional documentation. Of course, also check out the `examples` and code documentation built with `cargo doc`.

## Resources

* [Hoffmann et al.: **SnailTrail Paper** (NSDI '18)](https://doi.org/10.3929/ethz-b-000228581)
* [Malte Sandstede: **A Short Introduction to SnailTrail** (ETH '19)](https://github.com/li1/talks/raw/master/snailtrail.pdf)
* [Vasia Kalavri: **Towards self-managed, re-configurable streaming dataflow systems** (UGENT '19)](https://www.youtube.com/watch?v=E947ynd_vGI)
* [Moritz Hoffmann: **SnailTrail: Generalizing Critical Paths for Online Analysis of Distributed Dataflows** (NSDI '18)](https://www.youtube.com/watch?v=h5kPd59v0U0)
* [Vasia Kalavri: **Online performance analysis of distributed dataflow systems** (O'Reilly Velocity London '18)](https://www.youtube.com/watch?v=AUQJkjx1Uh8)

## License

SnailTrail is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0), with portions covered by various BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
