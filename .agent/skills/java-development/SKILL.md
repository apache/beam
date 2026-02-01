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

name: java-development
description: Guides Java SDK development in Apache Beam, including building, testing, running examples, and understanding the project structure. Use when working with Java code in sdks/java/, runners/, or examples/java/.
---

# Java Development in Apache Beam

## Project Structure

### Key Directories
- `sdks/java/core` - Core Java SDK (PCollection, PTransform, Pipeline)
- `sdks/java/harness` - SDK harness (container entrypoint)
- `sdks/java/io/` - I/O connectors (51+ connectors including BigQuery, Kafka, JDBC, etc.)
- `sdks/java/extensions/` - Extensions (SQL, ML, protobuf, etc.)
- `runners/` - Runner implementations:
  - `runners/direct-java` - Direct Runner (local execution)
  - `runners/flink/` - Flink Runner
  - `runners/spark/` - Spark Runner
  - `runners/google-cloud-dataflow-java/` - Dataflow Runner
- `examples/java/` - Java examples including WordCount

### Build System
Apache Beam uses Gradle with a custom `BeamModulePlugin`. Every Java project's `build.gradle` starts with:
```groovy
apply plugin: 'org.apache.beam.module'
applyJavaNature( ... )
```

## Common Commands

### Build Commands
```bash
# Compile a specific project
./gradlew -p sdks/java/core compileJava

# Build a project (compile + tests)
./gradlew :sdks:java:harness:build

# Run WordCount example
./gradlew :examples:java:wordCount
```

### Running Unit Tests
```bash
# Run all tests in a project
./gradlew :sdks:java:harness:test

# Run a specific test class
./gradlew :sdks:java:harness:test --tests org.apache.beam.fn.harness.CachesTest

# Run tests matching a pattern
./gradlew :sdks:java:harness:test --tests *CachesTest

# Run a specific test method
./gradlew :sdks:java:harness:test --tests *CachesTest.testClearableCache
```

### Running Integration Tests
Integration tests have filenames ending in `IT.java` and use `TestPipeline`.

```bash
# Run I/O integration tests on Direct Runner
./gradlew :sdks:java:io:google-cloud-platform:integrationTest

# Run with custom GCP project
./gradlew :sdks:java:io:google-cloud-platform:integrationTest \
  -PgcpProject=<project> -PgcpTempRoot=gs://<bucket>/path

# Run on Dataflow Runner
./gradlew :runners:google-cloud-dataflow-java:examplesJavaRunnerV2IntegrationTest \
  -PdisableSpotlessCheck=true -PdisableCheckStyle=true -PskipCheckerFramework \
  -PgcpProject=<project> -PgcpRegion=us-central1 -PgcsTempRoot=gs://<bucket>/tmp
```

### Code Formatting
```bash
# Format Java code
./gradlew spotlessApply
```

## Writing Integration Tests

```java
@Rule public TestPipeline pipeline = TestPipeline.create();

@Test
public void testSomething() {
  pipeline.apply(...);
  pipeline.run().waitUntilFinish();
}
```

Set pipeline options via `-DbeamTestPipelineOptions='[...]'`:
```bash
-DbeamTestPipelineOptions='["--runner=TestDataflowRunner","--project=myproject","--region=us-central1","--stagingLocation=gs://bucket/path"]'
```

## Using Modified Beam Code

### Publish to Maven Local
```bash
# Publish a specific module
./gradlew -Ppublishing -p sdks/java/io/kafka publishToMavenLocal

# Publish all modules
./gradlew -Ppublishing publishToMavenLocal
```

### Building SDK Container
```bash
# Build Java SDK container (for Runner v2)
./gradlew :sdks:java:container:java11:docker

# Tag and push
docker tag apache/beam_java11_sdk:2.XX.0.dev \
  "us-docker.pkg.dev/your-project/beam/beam_java11_sdk:custom"
docker push "us-docker.pkg.dev/your-project/beam/beam_java11_sdk:custom"
```

### Building Dataflow Worker Jar
```bash
./gradlew :runners:google-cloud-dataflow-java:worker:shadowJar
```

## Test Naming Conventions
- Unit tests: `*Test.java`
- Integration tests: `*IT.java`

## JUnit Report Location
After running tests, find HTML reports at:
`<project>/build/reports/tests/test/index.html`

## IDE Setup (IntelliJ)
1. Open `/beam` (the repository root, NOT `sdks/java`)
2. Wait for indexing to complete
3. Find `examples/java/build.gradle` and click Run next to wordCount task to verify setup
