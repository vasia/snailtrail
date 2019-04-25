<p align="center">
  <img src="https://github.com/li1/snailtrail/raw/master/snail.png" width="250">
</p>

# SnailTrail

This is a fork of [SnailTrail](https://github.com/strymon-system/snailtrail), a tool to run online critical path analysis on various stream processors (see also the [SnailTrail NSDI'18 Paper](https://doi.org/10.3929/ethz-b-000228581)).

The fork builds upon the original repository and implements further algorithms for analyzing stream processors. It currently focuses on the 0.9 version of [Timely Dataflow and Differential Dataflow](https://github.com/timelydataflow).

## Structure

### Overview

(Roughly in order of appearance)

|Type | Crate    | Description |
| --------- | -------- | ----------- |
| adapter | `timely-adapter` | timely / differential 0.9 adapter |
| adapter | `spark-parser` | Spark adapter |
| adapter | `tensorflow` | TensorFlow adapter |
| adapter | Flink  | not publicly available |
| adapter | Timely < 0.9 | not publicly available |
| adapter | Heron  | not publicly available |
| infrastructure | `logformat` | Shared definitions of core data types and serialization of traces (in Rust, Java). |
| infrastructure | `pag-construction` | Constructs the Program Activity Graph (PAG) from a flat stream of events which denote the start/end of computation and communication. Also has scripts to generate various plots. |
| infrastructure | `timely-pag` | PAG generation specifically for timely 0.9 with epochal semantics. |
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

## License

SnailTrail is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0), with portions covered by various BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
