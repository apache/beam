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

# Rust SDK

A library for writing [Apache Beam](https://beam.apache.org/) pipelines in Rust.

The SDK is under active development. The design and its review are in the
RFC shared on the dev@beam.apache.org mailing list.

## Crate Layout

The SDK is a Cargo workspace with its root at `sdks/rust`:

| Crate | Path | Contents |
| :--- | :--- | :--- |
| `apache-beam-model` | `beam/model/` | Protobuf and gRPC bindings generated from the standard Beam model protos. |

## Building and Testing

Run builds through Gradle, like the rest of the Beam repository:

```bash
./gradlew :sdks:rust:build   # Compile all crates
./gradlew :sdks:rust:test    # Run all tests (one crate: -Ppkg=<crate>)
./gradlew :sdks:rust:check   # rustfmt --check + clippy -D warnings
./gradlew :sdks:rust:fmt     # Apply formatting
```

For the full task reference, see [docs/development.md](docs/development.md).

## Contributing

See the [Beam Contribution Guide](https://beam.apache.org/contribute/). Each new file must have the standard Apache 2.0 license header. Before you open a PR, run `./gradlew :sdks:rust:check` to verify compliance.
