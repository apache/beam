---
title:  "Beam modernization at project 10-year mark"
date:   2026-09-30 20:00:01 -0800
categories:
  - blog
  - update
authors:
  - yhu

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

## Summary

This blog summarizes recent modernization efforts across the Apache Beam codebase mainly between Beam 2.68.0 and 2.75.0, such as major package upgrades, aligning APIs with new programming language standards, deprecations, etc. Note that **new features** are intentionally not included in the scope of this post, as it focuses on the modernization effort of existing components. Thank you to all contributors!

Originally part of the "Beam 3.0" proposal, systematic modernization efforts were planned towards Beam 2.75.0. As the community agreed on delivering improvements incrementally while maximally preserving compatibility, we instead named 2.75.0 the "10-year anniversary release"---marking 10 years since the first release ([v0.1.0-incubating](/blog/first-release)).

## Package Upgrades

### Core languages

#### \[Java\] Java 25 Support ([**2.69.0**](https://github.com/apache/beam/issues/35627)) and dropped Java 8 support ([**2.74.0**](https://github.com/apache/beam/issues/31678))

Java 8 has been supported since [Beam 2.3.0](/blog/beam-2.3.0). Since 2025, the ecosystem has shifted to Java 11 and Java 17 for certain core dependencies. Because Java 8 remained the baseline language version for Beam for a long time, a phased transition was conducted in subsequent Beam versions:

* Introduced a framework to allow different Beam components with different Java version compatibility ([**2.66.0**](https://github.com/apache/beam/pull/34858), [**2.67.0**](https://github.com/apache/beam/pull/35232)).

* Stopped publishing Java 8 SDK container image. Java 8 pipeline execution uses Java 11 container ([**2.66.0**](https://github.com/apache/beam/pull/35064)).

* IO expansion service ([**2.68.0**](https://github.com/apache/beam/pull/35981)) and certain components (IcebergIO, HCatalogIO) started moving to Java 11.

And finally, the Beam baseline moved to Java 11. As of Beam 2.75.0, certain Beam components require Java 17:

* IcebergIO
* DebeziumIO
* DeltaIO
* IO expansion service
* Spark 4 runner

#### \[Python\] Python 3.13 and 3.14 Support ([**2.69.0**](https://github.com/apache/beam/issues/34869), [**2.73.0**](https://github.com/apache/beam/issues/37247)) and removed Python 3.9 support ([**2.70.0**](https://github.com/apache/beam/issues/36665))

#### \[Go\] Minimum Go version updated to 1.26 ([**2.69.0**](https://github.com/apache/beam/issues/36461), [**2.73.0**](https://github.com/apache/beam/issues/37897))

#### \[SQL\] Upgraded Beam vendored Calcite from 1.28.0 to 1.40.0 for Beam SQL ([**2.68.0**](https://github.com/apache/beam/issues/35483)) and removed ZetaSQL support ([**2.68.0**](https://github.com/apache/beam/issues/34423))

Beam SQL was on Calcite 1.28.0 since Beam 2.35.0 (2021). As part of the Lakehouse initiative, Beam SQL has received renewed attention and moved to Calcite 1.40.0. A notable new feature is support for dialects such as BigQuery, SparkSQL, etc.

Here is an example of using a PostgreSQL dialect-provided scalar function in Beam YAML:

```yaml
pipeline:
  transforms:
    - type: Create
      name: CreateSampleData
      config:
        elements:
          - {id: 1, tags: "java python go"}
          - {id: 2, tags: "rust cpp"}
          - {id: 3, tags: "javascript typescript"}
    - type: Sql
      name: TransformWithPostgresFunction
      input: CreateSampleData
      config:
        query: "SELECT id, STRING_TO_ARRAY(tags, ' ') as tag_list FROM PCOLLECTION"
    - type: LogForTesting
      input: TransformWithPostgresFunction

options:
  calcite_connection_properties: {"fun": "postgresql"}
```

ZetaSQL support was removed; Calcite SQL with the BigQuery dialect is the preferred alternative within Beam, or migrating to use BigQuery directly is also recommended.

### Core dependencies

#### \[Java\] Upgraded Avro to 1.12 ([**2.74.0**](https://github.com/apache/beam/pull/38373))

Avro 1.12 was released back in August 2024 and dropped Java 8 support. As part of Java 8 support removal, Beam upgraded its Avro dependency to 1.12 in Beam 2.74.0.

#### \[Java\] Upgraded GCS connector (gcsio) to 3.x ([**2.74.0**](https://github.com/apache/beam/pull/38419))

GCS connector 3.0 was released back in December 2023 and dropped Java 8 support. As part of Java 8 support removal, Beam upgraded its GCS connector dependency to 3.1 in Beam 2.74.0. Note that there is a [performance regression](https://github.com/apache/beam/issues/39548) related to this upgrade in Beam 2.74.0 and 2.75.0 affecting pipelines with a moderate to heavy Cloud Storage read workload. To resolve this, upgrading to Beam 2.76.0 is recommended.

#### \[Python\] Protobuf 6.x Support ([**2.69.0**](https://github.com/apache/beam/pull/35477))

#### \[Python\] Migrated default pickler to cloudpickle and made dill optional ([**2.68.0**](https://github.com/apache/beam/pull/35725), [**2.69.0**](https://github.com/apache/beam/issues/21298))

Previously, the Python SDK relied on a pinned `dill==0.3.1.1` dependency for serialization, which frequently caused dependency conflicts and blocked new Python version upgrades. After switching the default pickler to `cloudpickle`, Beam migrated the deterministic fallback coder for complex types (`NamedTuple`, `Enum`, `dataclass`) to `cloudpickle` in Beam 2.68.0 and moved `dill` out of the core requirements into an optional extra (`apache-beam[dill]`) in Beam 2.69.0.

#### \[Python\] Migrating GCP clients off `google-apitools` ([**2.72.0**](https://github.com/apache/beam/pull/37309), [**2.75.0**](https://github.com/apache/beam/pull/37639))

The Python SDK has been migrating its internal GCP service integrations off the legacy `google-apitools` library and generated V1 clients to official `google-cloud-*` client libraries:

* Migrated Cloud Build client in container builder to `google-cloud-build` ([**2.72.0**](https://github.com/apache/beam/pull/37309)) and removed standalone `apitools` `HttpError` usages ([**2.72.0**](https://github.com/apache/beam/pull/37296)).

* Migrated Dataflow runner job submission and metrics client from generated `dataflow_v1b3` `apitools` bindings to `google-cloud-dataflow-client` ([**2.75.0**](https://github.com/apache/beam/pull/37639)).

BigQuery is now the last remaining `google-apitools` client in the Python SDK, and its migration is in progress.

#### \[YAML\] Switched JavaScript engine from js2py to QuickJS ([**2.75.0**](https://github.com/apache/beam/issues/38473))

Because `js2py` is no longer actively maintained, lacked Python 3.12+ support, and was affected by [CVE-2024-28397](https://www.cve.org/CVERecord?id=CVE-2024-28397), Beam YAML switched its JavaScript UDF execution engine to `quickjs`.

### IO dependencies

#### \[Java\] Upgraded Apache Iceberg to 1.9.2 ([**2.68.0**](https://github.com/apache/beam/pull/35981)) and 1.10.0 ([**2.69.0**](https://github.com/apache/beam/issues/36123))

Iceberg 1.7.0 dropped Java 8 support. After enabling individual Beam components to be compiled with different Java versions, IcebergIO was updated to build on top of Iceberg 1.9.2 in Beam 2.68.0. Later in Beam 2.69.0, it was upgraded to 1.10.0.

#### \[Java\] Upgraded HCatalogIO to Hive 4.0.1 ([**2.71.0**](https://github.com/apache/beam/issues/32189))

HCatalog 4.0 dropped Java 8 support. Beam moved from HCatalog 3.1.3 to 4.0.1 in Beam 2.71.0, requiring Java 11.

#### \[Java\] Elasticsearch 9 Support ([**2.71.0**](https://github.com/apache/beam/issues/36491))

#### \[Java\] Upgraded MongoDB Java driver from 3.x to 5.5.0 ([**2.68.0**](https://github.com/apache/beam/pull/35946))

#### \[Java\] Migrated ClickHouseIO to ClickHouse Java Client v2 ([**2.72.0**](https://github.com/apache/beam/issues/37610))

### Runners

#### Apache Flink 2.x Runner Support (2.0 since [**2.72.0**](https://github.com/apache/beam/issues/36947), 2.1 and 2.2 since [**2.75.0**](https://github.com/apache/beam/issues/38947)) and dropped Flink 1.17 and 1.18 runner support ([**2.75.0**](https://github.com/apache/beam/pull/39006))

Flink 2.0 dropped the legacy DataSet API. Batch pipelines now run on the Flink DataStream API.

#### Apache Spark 4 Runner Support ([**2.74.0**](https://github.com/apache/beam/issues/38255))

Spark 3.5 (and 4.0) deprecated the DStream API and improved the Dataset (Structured Streaming) API. Spark 4 streaming support is in progress.

## Programming language interface modernization

### Java

#### JUnit 5 support ([**2.69.0**](https://github.com/apache/beam/issues/18733))

Previously, `TestPipeline` relied on JUnit 4's `TestRule` (`@Rule`). Beam now provides a dedicated `org.apache.beam:beam-sdks-java-testing-junit` module featuring `TestPipelineExtension`, enabling JUnit 5 tests to inject `TestPipeline` via `@ExtendWith` (or configure custom options with `@RegisterExtension`) while maintaining backward compatibility with JUnit 4:

```java
import org.apache.beam.sdk.testing.TestPipelineExtension;
...

@ExtendWith(TestPipelineExtension.class)
class MyPipelineTest {

  @Test
  void testPipeline(TestPipeline pipeline) {
    PCollection<String> output = pipeline.apply("Create", Create.of("hello", "world"));

    PAssert.that(output).containsInAnyOrder("hello", "world");
    pipeline.run();
  }
}
```

#### DoFn OutputReceiver fluent OutputBuilder API for extended metadata ([**2.69.0**](https://github.com/apache/beam/issues/34902))

To support extensible per-element metadata (such as Change Data Capture operations, OpenTelemetry trace propagation, and pipeline drain indicators) without introducing combinatorial `outputWith*` method overloads on `OutputReceiver` and `ProcessContext`, `DoFn.OutputReceiver` now provides a fluent `OutputBuilder` API (`receiver.builder(element)...output()`). See the [Beam Element Extended Metadata design doc](https://s.apache.org/beam-element-extended-metadata) for details.

### Python

#### Dataclass-backed Beam portable Row ([**2.73.0**](https://github.com/apache/beam/issues/22085))

Python 3.7 (PEP 557) introduced `dataclass` and recommended it over `NamedTuple`, while the Beam Python SDK previously only used `NamedTuple` for Beam Rows under the Beam portable schema framework.

Beam Row also had limited support for custom types: an arbitrary object type could be assigned to a Beam Row and work within the Python SDK, yet break across language boundaries.

Incremental improvements have been made to address these limitations:

* Support `dataclass` for Beam Row ([**2.73.0**](https://github.com/apache/beam/issues/22085)).

* Preserving registered `NamedTuple` and `dataclass` types with `register_row` and field type inference ([**2.74.0**](https://github.com/apache/beam/issues/38108), [**2.75.0**](https://github.com/apache/beam/issues/38797)).

* Support for Python user types in Beam SQL ([**2.74.0**](https://github.com/apache/beam/issues/20738)).

Using Beam SQL with the Python SDK now supports rows containing arbitrary Python user types, as well as Python built-in types:

```python
class Arbitrary:
  def __init__(self, obj):
    self.obj = obj

  def __eq__(self, other):
    return self.obj == other.obj

with Pipeline() as p:
    out = (
        p | beam.Create([
            UserRow(1, Arbitrary(1.0), 1 + 2.5j),
            UserRow(1, Arbitrary("abc"), -1j),
        ])
        | SqlTransform("SELECT arb, complex FROM PCOLLECTION"))
# result: [(Arbitrary(1.0), 1 + 2.5j), (Arbitrary("abc"), -1j)]
```

#### Made Beartype the default fallback runtime type checker ([**2.74.0**](https://github.com/apache/beam/issues/38275))

Beam's custom typehint system previously fell back to `Any` for newer standard Python `typing` constructs. Integrating `beartype` as the default fallback enables fast runtime type checking for modern Python type annotations (which can be disabled if needed via `--disable_beartype`).

#### Enhanced typehint support

* for tagged output type hints ([**2.72.0**](https://github.com/apache/beam/issues/37434))
* for exception handling ([**2.73.0**](https://github.com/apache/beam/issues/37590), [**2.74.0**](https://github.com/apache/beam/issues/38173))

#### \[Python\] Split optional dependencies ([**2.69.0**](https://github.com/apache/beam/issues/21298), [**2.70.0**](https://github.com/apache/beam/issues/34554))

Feature- and connector-specific dependencies (`dill`, `hadoop`, `redis`, `interactive`, `tfrecord`, `yaml`) have been split out of the core `apache-beam` package into optional extras (e.g., `pip install apache-beam[gcp,interactive,yaml,redis,hadoop,tfrecord]`), reducing the default installation footprint and minimizing transitive dependency conflicts.

## Deprecations

### Sunsets

#### \[Runner\] Removed Apache Samza runner support ([**2.74.0**](https://github.com/apache/beam/issues/35448))

Apache Samza has been in a "dormant" state. As part of the Java 8 sunset, Samza runner support has been removed from Beam.

#### \[IO\] Removed Pub/Sub Lite IO support ([**2.72.0**](https://github.com/apache/beam/issues/37375))

GCP Pub/Sub Lite is no longer available for new customers after September 24, 2024. In line with this, Pub/Sub Lite IO has been sunset from Beam.

### Deprecations

Since the community has decided to keep the major version number for now and avoid deliberate breaking changes, the following components are marked as **deprecated** while remaining available.

#### \[Java\] Deprecated Beam Euphoria API ([**2.69.0**](https://github.com/apache/beam/issues/29451))

#### \[Java\] Removed deprecated Hadoop 2.x and 3.2.x versions from IcebergIO ([**2.69.0**](https://github.com/apache/beam/issues/36282))

#### \[Python\] Deprecated native Python SpannerIO in favor of cross-language wrapper ([**2.68.0**](https://github.com/apache/beam/issues/35860))

#### \[Runner\] Deprecated Twister2 runner support ([**2.68.0**](https://github.com/apache/beam/issues/35905))
