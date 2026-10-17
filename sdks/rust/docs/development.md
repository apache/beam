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

# Developing the Rust SDK

This page gives the build, test and lint tasks for developers of the
SDK itself. Pipeline authors do not need these tasks. See the [SDK README](../README.md).

Run builds and tests through Gradle, like the rest of the Beam repository. The Gradle tasks call cargo, so a cargo command gives the same result:

```bash
# Core Lifecycle Tasks
./gradlew :sdks:rust:build             # Compile all crates in workspace
./gradlew :sdks:rust:check             # Run formatting checks and clippy linter
./gradlew :sdks:rust:test              # Run all tests (or -Ppkg=apache-beam-model)
./gradlew :sdks:rust:fmt               # Apply code formatting (cargo fmt)
./gradlew :sdks:rust:clean             # Remove the cargo target directory
```

## One way

Every public API change follows these rules:

- **No aliases or compatibility shims.** A rename removes the old name. The same
  change updates every caller, example and doc.
- **One public path per item.** Pipeline items are in their module
  (`transforms`, `transforms::sdf`, `windowing`, `coders`, ...). Runner and
  harness plumbing (`BundleHandler`, `ElementSink`, `HandlerContext`, ...) is
  only in `beam::internals`. The preludes are the only additional path.
- **Fluent methods mirror core transforms.** Each method is the snake_case name
  of exactly one core transform (`.group_by_key` → `GroupByKey`). It adds no
  semantics. Documented exception: `with_side_singleton` / `with_side_iter` /
  `with_side_map`. Each of these is a `ParDo` with one side input.
- **Fluent covers common cases.** For advanced configuration, use
  `.apply(Transform::new(..))`, which also chains. Do not add fluent variants
  for advanced configuration.
- **Construction.** Build transforms with `new(name, required..)` and
  `.with_*()` builders. Value types (e.g. `FixedWindows::of`) are exempt.
- **Errors.** User hooks (`DoFn`, `CombineFn`, state, timers) return `beam::Result`.
- **Prelude.** `beam::prelude` contains only the items that a typical pipeline author uses.

## Comments and documentation

Comments and rustdoc follow the writing rules of
[ASD-STE100 Simplified Technical English](https://www.asd-ste100.org/about_STE.html).
Beam, Rust and protocol terms are permitted technical names.

- **Short sentences.** At most 20 words for an instruction and 25 words for a
  description. One topic per sentence.
- **Active voice.** Write instructions in the imperative: "Call `finish` before
  `drop`", "Do not share the sink".
- **One word, one meaning.** Use the same term for the same thing in all files.
- **Say what the code cannot.** Write a comment for a constraint, a contract or a
  reason that is not clear from the code. Do not repeat what the code does.
- **No numbered steps.** If the order is important, say so in words or split the
  code into functions.
- **No history.** Do not write "used to", "now", "previously", "no longer" or
  "Regression:". Write the rule that is true for the current code. A test
  comment states the behavior that the test checks.
- **This SDK only.** Do not compare with other SDKs ("like Java", "same as Go",
  "the equivalent of"). Name another SDK only when the behavior depends on it:
  a cross-language transform that the code calls, or a wire format, URN or
  protocol rule that other SDKs must decode. Comparisons between SDKs go in
  `DESIGN.md`.
- **Plain words.** Use the simplest word that is correct. Do not use: thus, hence,
  therefore, obey, ensure, utilize, leverage, facilitate, via, prior to,
  subsequent, terminate, approximately, sufficient, robust, seamless,
  comprehensive, crucial, essential, simply, merely, basically, actually,
  effectively, notably, furthermore, moreover, additionally, "in order to",
  "note that", "stem from". Use so, follow, make sure, use, help, through,
  before, then, stop, about, enough, also, to.
- **Short.** A comment is shorter than the code it explains. Fill lines to about
  90 columns: one short sentence does not need its own line.
- **Rustdoc.** Start with a one-line summary. Do not repeat the item name
  ("Returns the key." on `fn key`). Add `# Errors`, `# Panics` or `# Safety` only
  when the condition is not clear from the signature and the summary.
