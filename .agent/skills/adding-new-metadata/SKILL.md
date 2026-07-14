---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: adding-new-metadata
description: Guide on how to add and propagate new metadata fields in Apache Beam's WindowedValue, extending protos, windmill persistence, and runner interfaces to avoid metadata loss.
---

# Adding New Metadata to WindowedValue

This skill provides a comprehensive guide on adding new metadata (e.g., CDC metadata, drain mode flags, OpenTelemetry trace context) to Apache Beam's `WindowedValue` and ensuring it propagates correctly through the execution engine. Failing to propagate metadata in all necessary places will result in metadata loss during pipeline execution.

## 1. Extending the Proto Model

When adding new metadata that must cross worker boundaries or be serialized by the Fn API, the proto definitions must be updated.

*   **Key Files:** `model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/beam_fn_api.proto`
*   **Action:** Add the new metadata field to the appropriate message (`ElementMetadata`).
*   **Note:** Add proper documentation in proto. Type of the field can be different from the type in WindowedValue, see OpenTelemetry trace context for example.

## 2. WindowedValue Interface and Implementations

The `WindowedValue` is the core container for elements flowing through a Beam pipeline. It holds the value, timestamp, windows, pane info, and any additional metadata.

### Core Interface Updates
*   **Key File:** `sdks/java/core/src/main/java/org/apache/beam/sdk/values/WindowedValue.java`
*   **Action:** Add getter methods for your new metadata.

### Concrete Implementations
You must update **all** concrete implementations of `WindowedValue` to store and return the new metadata. If you miss one, metadata will be silently dropped.
*   `ValueInGlobalWindow`
*   `ValueInSingleWindow`
*   `ValueInEmptyWindows` (often used inside runners, like Dataflow's worker package)
*   **Action:** Update constructors, factory methods (`of()`), fields in these classes and coders.

### OutputBuilder vs. Context Output
*   **IMPORTANT:** Do **not** add new arguments to legacy methods like `context.outputWindowedValue(...)` or `WindowedValue.of(value, timestamp, windows, pane)`. This causes brittleness and breaks the API for every new metadata field.
*   **Action:** Modify `OutputBuilder` (`sdks/java/core/src/main/java/org/apache/beam/sdk/values/OutputBuilder.java`) to accept the new metadata (e.g., `.withDrainMode(...)`, `.withTraceContext(...)`). Use the builder pattern when constructing outputs to propagate offset and record IDs smoothly.

## 3. Windmill Persistence (Dataflow Streaming Engine) Runner v1

For the Dataflow streaming runner, metadata must survive serialization to and from the Windmill backend.

*   **Serialization (Sink):**
    *   **File:** `runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/WindmillSink.java`
    *   **Action:** Extract the metadata from the `WindowedValue`, and add it to already created ElementMetadata proto builder.
*   **Deserialization (Reader):**
    *   **Files:** `runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/UngroupedWindmillReader.java` and `WindowingWindmillReader.java`
    *   **Action:** Extract the metadata from ElementMetadata proto and reconstruct the `WindowedValue` using the updated factory methods/builders that include the metadata. This is incremental work, as plenty of metadata is already extracted from the proto.

## 4. Propagation Across Core Classes

Metadata must be explicitly copied or forwarded whenever a `WindowedValue` is transformed, buffered, or processed.

### DoFn Runners (Java Core)
You must ensure that when a DoFn processes an element and outputs a new element, the appropriate metadata from the *input* is propagated to the *output* (unless explicitly changed by the logic).
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/SimpleDoFnRunner.java`
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/StatefulDoFnRunner.java`
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/LateDataDroppingDoFnRunner.java`
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/ProcessFnRunner.java`
*   `sdks/java/harness/src/main/java/org/apache/beam/fn/harness/FnApiDoFnRunner.java`

**Action:** When these runners call `outputWindowedValue()`, they should extract the metadata from the input or current context and attach it using the `OutputBuilder` or the new `WindowedValue` interfaces.

### Grouping and Reducing
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/ReduceFnRunner.java`
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/ReduceFnContextFactory.java`
*   **Action:** Ensure that during GroupByKey/Combine operations, if metadata needs to be preserved (e.g., `CausedByDrain`), it is correctly passed into the `ReduceFnContextFactory` and propagated when outputting the grouped results.

### Splittable DoFns (SDF)
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/OutputAndTimeBoundedSplittableProcessElementInvoker.java`
*   `sdks/java/core/src/main/java/org/apache/beam/sdk/util/construction/SplittableParDoNaiveBounded.java`

### Timers
If metadata needs to survive timer firings (e.g., knowing an `@OnTimer` fired because of a system drain), it must be added to Timer data structures. This is a bit of uncharted area which was only implemented for CausedByDrain metadata that comes from backend, not from persisted metadata. In order to persist all WindowedValue metadata across timer, more work has to be done, below are some pointers:
*   `runners/core-java/src/main/java/org/apache/beam/runners/core/TimerInternals.java` and implementations (e.g., `WindmillTimerInternals.java` in Dataflow).
*   **Action:** Add the field to `TimerData`, next to `CausedByDrain`. Propagate it when setting the timer and expose it when the timer fires so it bubbles up.
* Eventually, metadata from Timer lands in WindowedValue, so it can be exposed to users. Keep field names, types, and getters similar to WindowedValue as much as possible, as common interface may be introduced eventually.

## 5. Exposing Metadata to the User (DoFn Signatures)

User needs to access the metadata in their `DoFn` (e.g., `@ProcessElement public void process(ProcessContext c, CausedByDrain drain) { ... }`), you must update the reflection and bytecode generation logic.

*   **Files:**
    *   `sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/reflect/DoFnSignatures.java`
    *   `sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/reflect/DoFnSignature.java`
    *   `sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/reflect/DoFnInvoker.java`
    *   `sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/reflect/ByteBuddyDoFnInvokerFactory.java`
*   **Action:** Add logic to detect the new parameter type in the DoFn method signature. Generate bytecode using ByteBuddy to extract the property from the `WindowedValue` or context and pass it as an argument during method invocation.

## Checklist for Adding New Metadata

1.  [ ] Define the metadata in `beam_fn_api.proto` (if applicable).
2.  [ ] Add getters to the `WindowedValue` interface.
3.  [ ] Update `ValueInGlobalWindow`, `ValueInSingleWindow`, `ValueInEmptyWindows` to store the metadata.
4.  [ ] Update `OutputBuilder` to accept the metadata.
5.  [ ] Update `WindmillSink` to serialize the metadata to the backend.
6.  [ ] Update `UngroupedWindmillReader` and `WindowingWindmillReader` to deserialize the metadata.
7.  [ ] Update `WindmillKeyedWorkItem`.
8.  [ ] Update `SimpleDoFnRunner`, `StatefulDoFnRunner`, and `FnApiDoFnRunner` to propagate the metadata from input to output.
9.  [ ] Update `ReduceFnRunner` and `OutputAndTimeBoundedSplittableProcessElementInvoker` for complex transform propagation.
10. [ ] If required by timers, update `TimerData` and `TimerInternals`.
11. [ ] If exposed to the user, update `DoFnSignatures` and `ByteBuddyDoFnInvokerFactory`.
12. [ ] Update other runners (Flink, Spark) to ensure they propagate the new `WindowedValue` fields correctly in their specific operators/runners.
