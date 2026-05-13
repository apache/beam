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
# Apache Beam Spark 4 Runner

Experimental Beam runner for Apache Spark 4 (batch-only). Built on the shared
`runners/spark` source base via `spark_runner.gradle`'s per-version
source-overrides mechanism: this module contributes the small set of files
under `src/main/java/.../structuredstreaming/` that diverge from the Spark 3
implementation. See the parent `runners/spark/` module for the bulk of the
runner code.

## Requirements

* **Spark 4.0.2** (and other Spark 4.0.x patch releases)
* **Scala 2.13**
* **Java 17** — Spark 4 does not run on earlier JDKs

## Status

Batch only. Streaming is tracked in
[#36841](https://github.com/apache/beam/issues/36841).

## Known issues

### `StackOverflowError` from `slf4j-jdk14` on the runtime classpath

Spark 4 ships `org.slf4j:jul-to-slf4j` to route `java.util.logging` records
into SLF4J. If `org.slf4j:slf4j-jdk14` is also resolved at runtime — it routes
the other direction (SLF4J → JUL) — the first log line creates an infinite
loop:

```
java.lang.StackOverflowError
    at org.slf4j.bridge.SLF4JBridgeHandler.publish(...)
    at java.util.logging.Logger.log(...)
    at org.slf4j.impl.JDK14LoggerAdapter.log(...)
    at org.slf4j.bridge.SLF4JBridgeHandler.publish(...)
    ...
```

This is the same condition that broke the Spark 3 runner in
[#26985](https://github.com/apache/beam/issues/26985), fixed in
[#27001](https://github.com/apache/beam/pull/27001).

The shared `spark_runner.gradle` already excludes `slf4j-jdk14` from the
runner module's own `configurations.all`, so in-tree builds are unaffected.
Downstream Gradle consumers that assemble a runtime classpath against
`beam-runners-spark-4` should mirror that exclude:

```groovy
configurations.all {
    exclude group: "org.slf4j", module: "slf4j-jdk14"
}
```

For Maven, exclude `org.slf4j:slf4j-jdk14` from any dependency that pulls it
transitively (commonly the Beam SDK harness and several IO connectors).
