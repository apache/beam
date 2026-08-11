---
title:  "Apache Beam 2.76.0"
date:   2026-08-?? 14:00:00 -0500
categories:
  - blog
  - release
authors:
  - vterentev
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

We are happy to present the new 2.76.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2760-2026-08-??) for this release.

<!--more-->

For more information on changes in 2.76.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/43).

## Highlights

### I/Os

* Upgraded Iceberg dependency to 1.11.0 (Java) ([#38925](https://github.com/apache/beam/issues/38925)).
* Add ArrowFlight IO (Java) ([#20116](https://github.com/apache/beam/issues/20116)).
* (Python) JmsIO (IBM MQ, ActiveMQ, and other providers) is now supported in Python via cross-language ([#30716](https://github.com/apache/beam/issues/30716)).
* Added a full Iceberg batch and streaming changelog source (CDC) ([#38831](https://github.com/apache/beam/issues/38831))
* Added a Delta Lake batch changelog source (CDC) ([#39492](https://github.com/apache/beam/issues/39492))

### New Features / Improvements

* Added `GroupIntoBatches` transform and the standard
  `beam:coder:sharded_key:v1` coder to the Go SDK, along with
  `beam.Coder.IsDeterministic`, `beam.PCollection.WindowingStrategy`,
  and `coder.RegisterDeterministicCoder` for opt-in deterministic
  custom coders (Go) ([#19868](https://github.com/apache/beam/issues/19868)).
* TriggerStateMachineRunner changes from BitSetCoder to SentinelBitSetCoder to
  encode finished bitset. SentinelBitSetCoder and BitSetCoder are state
  compatible. Both coders can decode encoded bytes from the other coder
  ([#38139](https://github.com/apache/beam/issues/38139)).
* (Python) Removed the `envoy-data-plane` (and transitive `betterproto`) dependency; `EnvoyRateLimiter` now uses a small vendored protobuf definition instead, resolving dependency conflicts for downstream projects ([#37854](https://github.com/apache/beam/issues/37854)).
* (Java) Supported acknowledge mode for JmsIO ([#39253](https://github.com/apache/beam/issues/39253)).
* (Python) Staged files directory is now automatically added to `sys.path` on the Python SDK worker at startup. This makes Python files provided via the '--files_to_stage' pipeline option importable in the pipeline code and makes it easier to initialize Python SDK harness at startup via the `--beam_plugins` pipeline option. For more information, see the [Staging Individual Files](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#staging-files) section of the dependency management docs. This behavior can be disabled by passing the '--experiments=no_staged_dir_in_sys_path' pipeline option ([#39431](https://github.com/apache/beam/issues/39431)).
* (Python) Added `equal_to_approx`, an `assert_that` matcher that compares numeric pipeline outputs with a configurable tolerance ([#18028](https://github.com/apache/beam/issues/18028)).
* (Python) `Timestamp` now supports variable subsecond precision, up to nanoseconds. The portable
  `beam:logical_type:timestamp:v1` logical type now maps to Python's `Timestamp` ([#39344](https://github.com/apache/beam/issues/39344)).
* (Python) Added `UnboundedSource`, an interface for reading an infinite stream of records with checkpointing, watermark reporting, and bundle finalization. Read one with `beam.io.Read`
  ([#19137](https://github.com/apache/beam/issues/19137)).
* (Python) Added `Watch`, a transform that polls a growing set of outputs for each input element, deduplicates outputs across poll rounds, and stops per a user-supplied termination condition
  ([#21521](https://github.com/apache/beam/issues/21521)).
* (Python) Added support to analyze core dumps created after python worker segmentation faults with `pystack` (or `gdb` if installed) using the `--profiler_agent=coredump` pipeline option. ([#39484](https://github.com/apache/beam/issues/39484)).
* (Python) Added `Sample.Any`, the Python equivalent of Java's `Sample.any`, which returns up to n arbitrary elements from a PCollection ([#18552](https://github.com/apache/beam/issues/18552)).
* (Java) Added per-element OpenTelemetry trace propagation across stages in the Dataflow Streaming Runner. Enable it with `--experiments=enable_otel_defaults,element_metadata_supported,disable_portable_worker`. Cloud Trace incurs additional cost. ([#33176](https://github.com/apache/beam/issues/33176))
* (Java) Added OpenTelemetry header propagation support for both reads and writes in KafkaIO and PubSubIO. ([#33176](https://github.com/apache/beam/issues/33176))
* (Java) Added OpenTelemetry tracing support for SpannerIO change streams ([#33176](https://github.com/apache/beam/issues/33176))

### Breaking Changes

* (Python) Removed `google-perftools` from the SDK container images. Users who wish to use `--profiler_agent=tcmalloc` should install google-perftools APT package in their custom container images separately ([#39323](https://github.com/apache/beam/issues/39323)).
* [IcebergIO] Reading a `timestamptz` column will now return a `Timestamp.MICROS` Beam logical type to preserve
  microseconds (the old Beam `Schema.FieldType#DATETIME` primitive type truncates past milliseconds). This may break
  the following use cases when a `timestamptz` column is present:
  * Existing streaming read pipelines.
  * Managed Iceberg batch reads when upgraded from an older SDK.
  * Python reads.

  Use pipeline option `--updateCompatibilityVersion=2.75.0` (or any older version) to keep the old behavior ([#39344](https://github.com/apache/beam/issues/39344)).
* `DoFn.process` returning a `str`, `bytes`, or `dict` (instead of an iterable wrapping one) now raises a `TypeError` rather than silently iterating per-character/byte/key (Python) ([#18712](https://github.com/apache/beam/issues/18712)).
* (Java) Added `DRAINING` and `DRAINED` states to `PipelineResult`, including runner state mappings and Dataflow update handling ([#39020](https://github.com/apache/beam/issues/39020)).
* (Python) Typehints of dataclass fields are honored during type inferences. To restore the behavior of fallback-to-any,
  use pipeline option `--exclude_infer_dataclass_field_type` ([#38797](https://github.com/apache/beam/issues/38797)).
  However fixing forward is recommended.
* (Java) IcebergIO and projects that use it must now be built with Java 17 or later as a result of Iceberg 1.11.0 upgrade ([#38925](https://github.com/apache/beam/issues/38925)).

### Bugfixes

* Fixed unresolved runtime `ValueProvider` options being stringified in Python Dataflow Flex Templates ([#39499](https://github.com/apache/beam/issues/39499)).
* Fixed unbounded checkpoint state growth for splittable DoFns that self-checkpoint on the portable Flink runner (Java) ([#27648](https://github.com/apache/beam/issues/27648)).
* Improved Java pipeline performance by avoiding repeated `DoFn` type descriptor resolution when creating cached invokers ([#39309](https://github.com/apache/beam/issues/39309)).
* (Python) Fixed a memory leak in Python SDK caused by storing exceptions with potentially large stack frames in a cache ([#39406](https://github.com/apache/beam/issues/39406)).

### Known Issues

* (Java) Projects using the Flink runner with Flink 2.1 or later alongside libraries requiring `org.lz4:lz4-java` (e.g., Kafka clients) may encounter a Gradle capability conflict, because Flink 2.1+ ships `at.yawk.lz4:lz4-java` which declares the same capability. To resolve, add a `capabilitiesResolution` rule to your `build.gradle` that selects `at.yawk.lz4:lz4-java` ([#38947](https://github.com/apache/beam/issues/38947)).

According to git shortlog, the following people contributed to the 2.76.0 release. Thank you to all contributors!

ADITYA RAJ, Abdelrahman Ibrahim, Ahmed Abualsaud, Alexander Kolb, Amar3tto, Andrew Crites, Arun Pandian, Aryankn29, Atharva Moroney, Avi Kondareddy, Bruno Volpato, Chamikara Jayalath, Chris Qiu, Claire McGinty, Danny McCormick, Derrick Williams, Elia Liu, Florian TREHAUT, Guflly, HansMarcus01, Ian Liao, Ivy Xu, Jack McCluskey, KRITI MITTAL, Kenneth Knowles, Lalit Yadav, Manvith Panyam, Minh Vu, Nikita Grover, PRADDZY, Peter Tran, Radosław Stankiewicz, Ryan Wigglesworth, Shahar Epstein, Shunping Huang, SreeramaYeshwanthGowd, Steven van Rossum, Tarun Annapareddy, Tejas Iyer, Tobias Kaymak, Tomasz Wojdat, Utkarsh Parekh, Venkata Bharath Malapati, Vitaly Terentyev, Yi Hu, ZIHAN DAI, aibrahiim, akshayjadiyanv, atognolas, claudevdm, janaom, jayjayakumar, raman118, shunping, tvalentyn
