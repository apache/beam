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

name: developing-new-io-connectors
description: End-to-end guide on developing new Apache Beam I/O connectors correctly, including core IO transforms, SchemaTransforms, URN proto definitions, Managed API integration, cross-language expansion service, and testing.
---

# Developing New Apache Beam I/O Connectors

This guide outlines the modern best practices and mandatory steps for building new Apache Beam I/O connectors. Modern Beam connectors are expected to be schema-aware, available in cross-language (Python, Go) pipelines via the Expansion Service, and seamlessly integrable with Beam YAML and the `Managed` I/O API.

## 1. Module Structure and Gradle Setup

New Java I/O connectors should reside under `sdks/java/io/<connector-name>`.

### Directory Layout
```text
sdks/java/io/<connector-name>/
├── build.gradle
└── src/
    ├── main/java/org/apache/beam/sdk/io/<connector-name>/
    │   ├── <Connector>IO.java
    │   ├── <Connector>ReadSchemaTransformProvider.java
    │   └── <Connector>WriteSchemaTransformProvider.java
    └── test/java/org/apache/beam/sdk/io/<connector-name>/
        ├── <Connector>IOTest.java
        └── <Connector>ReadSchemaTransformProviderTest.java
```

### Gradle Configuration (`build.gradle`)
Your `build.gradle` must use standard Beam Java module conventions and explicitly declare necessary dependencies.

```groovy
plugins { id 'org.apache.beam.module' }
applyJavaNature(
    // If <connector-name> contains hyphens, convert them to dots or underscores
    automaticModuleName: 'org.apache.beam.sdk.io.<connector_name>',
)

description = "Apache Beam :: SDKs :: Java :: IO :: <Connector Name>"
ext.summary = "Integration with <External System>."

dependencies {
    implementation project(path: ":sdks:java:core", configuration: "shadow")
    implementation project(path: ":model:pipeline", configuration: "shadow") // For URN definitions

    // Add external client libraries here
    implementation library.java.<client_dependency>

    // Handle strict dependency checking if necessary
    permitUnusedDeclared library.java.<client_dependency>

    // Standard test dependencies
    testImplementation project(path: ":sdks:java:core", configuration: "shadowTest")
    testImplementation library.java.junit
}
```

---

## 2. Core I/O Transform Implementation (`<Connector>IO.java`)

Follow Beam's canonical AutoValue builder pattern for user-facing API configuration. While core Java I/O connectors can be strongly typed using specific domain classes or Java generics (`<T>`) for idiomatic Java SDK usage, modern sources should also emphasize Beam `Row` and Schema support (e.g., via `.readRows()`). For excellent real-world implementations of this pattern, refer to `IcebergIO` and `DeltaIO`.

### Bounded & Unbounded Sources
Instead of legacy `Source` classes, implement reading via Beam's **Splittable DoFns (SDF)** framework for advanced features such as dynamic rebalancing and watermark support.

A primary read transform (such as `read()` or `readRows()`) typically extends `PTransform<PBegin, PCollection<T>>` (or `PCollection<Row>`). Using `PCollection` as input is meant for "ReadAll" operations (such as reading a collection of file patterns or queries).

An example SDF-based read transform is given below:

```java
public class MyIO {
  public static ReadRows readRows() {
    return new AutoValue_MyIO_ReadRows.Builder().build();
  }

  @AutoValue
  public abstract static class ReadRows extends PTransform<PBegin, PCollection<Row>> {
    public abstract @Nullable String getConfigurationOption();
    public abstract @Nullable Schema getSchema();
    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConfigurationOption(String value);
      public abstract Builder setSchema(Schema schema);
      public abstract ReadRows build();
    }

    public ReadRows withConfigurationOption(String value) {
      return toBuilder().setConfigurationOption(value).build();
    }

    public ReadRows withSchema(Schema schema) {
      return toBuilder().setSchema(schema).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input
          // `ReaderDoFn` is an SDF or source implementation that reads records and outputs `Row` objects.
          .apply(ParDo.of(new ReaderDoFn(getConfigurationOption())))
          .setRowSchema(getSchema());
    }
  }

  public static WriteRows writeRows() {
    return new AutoValue_MyIO_WriteRows.Builder().build();
  }

  @AutoValue
  public abstract static class WriteRows extends PTransform<PCollection<Row>, PDone> {
    public abstract @Nullable String getConfigurationOption();
    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConfigurationOption(String value);
      public abstract WriteRows build();
    }

    public WriteRows withConfigurationOption(String value) {
      return toBuilder().setConfigurationOption(value).build();
    }

    @Override
    public PDone expand(PCollection<Row> input) {
      input.apply("WriteRecords", ParDo.of(new WriterDoFn(getConfigurationOption())));
      return PDone.in(input.getPipeline());
    }
  }
}
```
* Please make sure that any code you add adheres to the Beam coding standards. These standards are documented here: https://beam.apache.org/contribute/code-guidelines/

* Especially scrutinize any logic that involves splitting the data for parallel processing since it is a common source of errors that can lead to data loss or data duplication related issues.

### Developing Sinks (Write Transforms)
When implementing data egress (Write transforms), avoid creating single-worker bottlenecks. Depending on your target system's transactional requirements, prefer one of the following canonical Beam sink patterns:

1. **DoFn-Based / Batching Sinks:** For external APIs or messaging systems (e.g., Kafka, Pub/Sub, NoSQL databases), use a `DoFn` that manages connections per bundle (`@StartBundle`, `@FinishBundle`) or utilizes `GroupIntoBatches` to perform highly efficient, parallel batched requests.
2. **Two-Phase Commit / Exactly-Once Sinks:** For transactional sinks (e.g., Relational DBs, Apache Iceberg, Delta Lake), implement a multi-stage `PTransform`:
   * **Write Shards:** Write records in parallel tasks to staging files or temporary transactions, emitting commit descriptors (`PCollection<CommitMessage>`).
   * **Global Commit:** Group the commit messages and execute a single final transaction to commit all staged files/shards.
3. **File-Based Sinks:** If your connector purely writes files, leverage Beam's `FileIO` core infrastructure (`FileIO.write()` / `FileIO.Sink`) rather than implementing custom file rolling and sharding logic.
4. **Error Reporting (Dead-Letter Queues):** Instead of failing the entire pipeline when an invalid element or API error occurs, modern Write transforms should optionally output a `PCollection<Row>` (or custom `WriteResult` / `PCollectionRowTuple`) containing failed records and explicit error metadata.

* Specially scrutinize any logic that can create duplicate data due to worker failures. Assume that any transform in the pipeline can fail and be retried multiple times by the Beam runner. If the sink does not handle this properly, it can lead to duplicate data in the target system.
---

## 3. Extending the Pipeline Model Proto (`external_transforms.proto`)

To standardize your transform identifier across SDKs, define its URN in Beam's protobuf schema.

1. Open `model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/external_transforms.proto`.
2. Add your read/write URNs under the appropriate enum (e.g., `ManagedTransforms.Urns`).

```protobuf
message ManagedTransforms {
  enum Urns {
    // ... existing entries
    MY_SYSTEM_READ = 15 [(org.apache.beam.model.pipeline.v1.beam_urn) =
      "beam:schematransform:org.apache.beam:my_system_read:v1"];
    MY_SYSTEM_WRITE = 16 [(org.apache.beam.model.pipeline.v1.beam_urn) =
      "beam:schematransform:org.apache.beam:my_system_write:v1"];
  }
}
```

3. Re-generate and compile the model protos:
```bash
./gradlew :model:pipeline:generateProto :model:pipeline:compileJava
```

---

## 4. Implementing `SchemaTransformProvider`

To expose your connector to cross-language pipelines and Beam YAML, create a typed `SchemaTransformProvider`.

```java
@AutoService(SchemaTransformProvider.class)
public class MyReadSchemaTransformProvider extends TypedSchemaTransformProvider<Configuration> {

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.MY_SYSTEM_READ);
  }

  @Override
  public String description() {
    return "Reads records from My System and outputs a PCollection of Beam Rows.";
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new MyReadSchemaTransform(configuration);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    @SchemaFieldDescription("Configuration option description.")
    public abstract String getConfigurationOption();

    public static Builder builder() {
      return new AutoValue_MyReadSchemaTransformProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConfigurationOption(String value);
      public abstract Configuration build();
    }
  }

  static class MyReadSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    MyReadSchemaTransform(Configuration configuration) {
      this.configuration = Objects.requireNonNull(configuration, "configuration cannot be null");
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> output = input.getPipeline().apply(
          MyIO.readRows().withConfigurationOption(configuration.getConfigurationOption()));
      return PCollectionRowTuple.of("output", output);
    }
  }
}
```

---

## 5. Integrating with Managed API (`Managed.java`)

Beam's `Managed` I/O transform provides a unified interface for data ingest/egress. To support it:

1. Open `sdks/java/managed/src/main/java/org/apache/beam/sdk/managed/Managed.java`.
2. Define a public constant identifier:
```java
public static final String MY_SYSTEM = "my_system";
```
3. Register your URNs in `READ_TRANSFORMS` or `WRITE_TRANSFORMS`:
```java
public static final Map<String, String> READ_TRANSFORMS =
    ImmutableMap.<String, String>builder()
        // ... existing transforms
        .put(MY_SYSTEM, getUrn(ExternalTransforms.ManagedTransforms.Urns.MY_SYSTEM_READ))
        .build();
```
4. Update the Javadoc block in `Managed.java` to list your new connector.

---

## 6. Expansion Service Registration

To enable non-Java SDKs (Python, Go) to discover and expand your new connector, include it in the standard Java Expansion Service.

1. Open `sdks/java/io/expansion-service/build.gradle`.
2. Add your module as a runtime dependency:
```groovy
dependencies {
    // ... existing dependencies
    runtimeOnly project(":sdks:java:io:<connector-name>")
}
```

---

## 7. Python & Beam YAML Integration

Once registered in the expansion service, your `SchemaTransform` can be utilized in Python and YAML. E.g., for `Managed` support in Python:

1. Open `sdks/python/apache_beam/transforms/managed.py`.
2. Export your identifier in `__all__` and map it to its URN in `Read._READ_TRANSFORMS` or `Write._WRITE_TRANSFORMS`:
```python
MY_SYSTEM = 'my_system'

__all__ = [
    # ... existing
    "MY_SYSTEM",
]

class Read(PTransform):
  _READ_TRANSFORMS = {
      # ... existing
      MY_SYSTEM: ManagedTransforms.Urns.MY_SYSTEM_READ.urn,
  }
```
3. In `sdks/python/apache_beam/transforms/external.py`, map the URN to the appropriate Expansion Service jar target in `MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING`.

---

## 8. Verification and Testing

Verify your new connector thoroughly across multiple abstraction layers:

### 1. Unit Tests
Test your core builder and `SchemaTransformProvider` translation.
```bash
./gradlew :sdks:java:io:<connector-name>:test
```

### 2. Managed API Translation
In `ManagedSchemaTransformTranslationTest.java` (under `sdks/java/managed`), you can verify the translation structure of your managed transform. Note that `ManagedTest.java` generally uses dummy/test providers (`TestSchemaTransformProvider`) to keep dependencies lightweight.
```bash
./gradlew :sdks:java:managed:test
```

### 3. Integration Tests (IT)
Create integration tests to test end-to-end data processing against real system instances, including `Managed.read(Managed.MY_SYSTEM)` usage.

* Test resources should be managed via `ResourceManager` classes under the `it/` directory.
* Add GitHub Actions to trigger your tests when changes are made to your connector code. Consider adding one for pre-commit and one for post-commit.

### 4. Documentation
Add any necessary documentation for your connector under the `website/www/site/content/en/documentation/io/built-in/` directory.

---

> [!TIP]
> **Canonical Reference Implementations:** When developing a new connector, we highly recommend studying **Apache Iceberg** ([IcebergIO.java](https://github.com/apache/beam/blob/master/sdks/java/io/iceberg/src/main/java/org/apache/beam/sdk/io/iceberg/IcebergIO.java)) and **Delta Lake** ([DeltaIO.java](https://github.com/apache/beam/blob/master/sdks/java/io/delta/src/main/java/org/apache/beam/sdk/io/delta/DeltaIO.java)) as state-of-the-art reference implementations.

For more details see the [Developing I/O connectors](https://beam.apache.org/documentation/io/developing-io-overview) guide.
