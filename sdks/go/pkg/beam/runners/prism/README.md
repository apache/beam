<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam Go Prism Runner

Prism is a local portable Apache Beam runner authored in Go.

* Local, for fast startup and ease of testing on a single machine.
* Portable, in that it uses the Beam FnAPI to communicate with Beam SDKs of any language.
* Go simple concurrency enables clear structures for testing batch through streaming jobs.

It's intended to replace the current Go Direct runner, but also be for general
single machine use.

For Go SDK users:
  - `import "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"`
  - Short term: set runner to "prism" to use it, or invoke directly. &#x2611;
  - Medium term: switch the default from "direct" to "prism". &#x2611;
  - Long term: alias "direct" to "prism", and delete legacy Go direct runner.

Prisms allow breaking apart and separating a beam of light into
it's component wavelengths, as well as recombining them together.

The Prism Runner leans on this metaphor with the goal of making it
easier for users and Beam SDK developers alike to test and validate
aspects of Beam that are presently under represented.

## Configurability

Prism is configurable using YAML, which is eagerly validated on startup.
The configuration contains a set of variants to specify execution behavior,
either to support specific testing goals, or to emulate different runners.

Beam's implementation contains a number of details that are hidden from
users, and to date, no runner implements the same set of features. This
can make SDK or pipeline development difficult, since exactly what is
being tested will vary on the runner being used.

At the top level the configuration contains "variants", and the variants
configure the behaviors of different "handlers" in Prism.

Jobs will be able to provide a pipeline option to select which variant to
use. Multiple jobs on the same prism instance can use different variants.
Jobs which don't provide a variant will default to testing behavior.

All variants should execute the Beam Model faithfully and correctly,
and with few exceptions it should not be possible for there to be an
invalid execution. The machine's the limit.

It's not expected that all handler options are useful for pipeline authors,
These options should remain useful for SDK developers,
or more precise issue reproduction.

For more detail on the motivation, see Robert Burke's (@lostluck) Beam Summit 2022 talk:
https://2022.beamsummit.org/sessions/portable-go-beam-runner/.

Here's a non-exhaustive set of variants.

### Variant Highlight: "default"

The "default" variant is testing focused, intending to route out issues at development
time, rather than discovering them on production runners. Notably, this mode should
never use fusion, executing each Transform individually and independantly, one at a time.

This variant should be able to execute arbitrary pipelines, correctly, with clarity and
precision when an error occurs. Other features supported by the SDK should be enabled by default to
ensure good coverage, such as caches, or RPC reductions like sending elements in
ProcessBundleRequest and Response, as they should not affect correctness. Composite
transforms like Splitable DoFns and Combines should be expanded to ensure coverage.

Additional validations may be added as time goes on.

Does not retry or provide other resilience features, which may mask errors.

To ensure coverage, there may be sibling variants that use mutually exclusive alternative
executions.

### Variant Highlight: "fast"

Not Yet Implemented - Illustrative goal.

The "fast" variant is performance focused, intended for local scale execution.
A psuedo production execution. Fusion optimizations should be performed.
Large PCollection should be offloaded to persistent disk. Bundles should be
dynamically split. Multiple Bundles should be executed simultaneously. And so on.

Pipelines should execute as swiftly as possible within the bounds of correct
execution.

### Variant Hightlight: "flink" "dataflow" "spark" AKA Emulations

Not Yet Implemented - Illustrative goal.

Emulation variants have the goal of replicating on the local scale,
the behaviors of other runners. Flink execution never "lifts" Combines, and
doesn't dynamically split. Dataflow has different characteristics for batch
and streaming execution with certain execution charateristics enabled or
disabled.

As Prism is intended to implement all facets of Beam Model execution, the handlers
can have features selectively disabled to ensure

## Current Limitations

* Testing use only.
* Executing docker containers isn't yet implemented.
    * This precludes running the Java and Python SDKs, or their transforms for Cross Language.
    * Loopback execution only.
    * No stand alone execution.
* In Memory Only
    * Not yet suitable for larger jobs, which may have intermediate data that exceeds memory bounds.
    * Doesn't yet support sufficient intermediate data garbage collection for indefinite stream processing.
* Doesn't yet execute all beam pipeline features.

## Implemented so far.

* DoFns
    * Side Inputs
    * Multiple Outputs
* Flattens
* GBKs
    * Includes handling session windows.
    * Global Window
    * Interval Windowing
    * Session Windows.
* CoGBKs
* Combines lifted and unlifted.
* Expands Splittable DoFns
* Process Continuations (AKA Streaming transform support)
* Limited support for Process Continuations
  * Residuals are rescheduled for execution immeadiately.
  * The transform must be finite (and eventually return a stop process continuation)
* Basic Metrics support
* Stand alone execution support
  * Web UI available when run as a standalone command.
* Progess tracking
    * Channel Splitting
    * Dynamic Splitting
* FnAPI Optimizations
  * Fusion

## Next feature short list (unordered)

See https://github.com/apache/beam/issues/24789 for current status.

* Test Stream
* Triggers & Complex Windowing Strategy execution.
* State
* Timers
* "PubSub" Transform
* Support SDK Containers via Testcontainers
  * Cross Language Transforms
* FnAPI Optimizations
  * Data with ProcessBundleRequest & Response

This is not a comprehensive feature set, but a set of goals to best
support users of the Go SDK in testing their pipelines.

## How to contribute

Until additional structure is necessary, check the main issue
https://github.com/apache/beam/issues/24789 for the current
status, file an issue for the feature or bug to fix with `[prism]`
in the title, and refer to the main issue, before begining work
to avoid duplication of effort.

If a feature will take a long time, please send a PR to
link to your issue from this README to help others discover it.

Otherwise, ordinary [Beam contribution guidelines apply](https://beam.apache.org/contribute/).

# Long Term Goals

Once support for containers is implemented, Prism should become a target
for the Java Runner Validation tests, which are the current specification
for correct runner behavior. This will inform further feature developement.
