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

name: io-connectors
description: Guides development and usage of I/O connectors in Apache Beam. Use when working with I/O connectors, creating new connectors, or debugging data source/sink issues.
---

# I/O Connectors in Apache Beam

## Overview
I/O connectors enable reading from and writing to external data sources. Beam provides 51+ Java I/O connectors and several Python connectors.

## Java I/O Connectors Location
`sdks/java/io/`

### Available Connectors
| Category | Connectors |
|----------|------------|
| Cloud Storage | google-cloud-platform (BigQuery, Bigtable, Spanner, Pub/Sub, GCS), amazon-web-services2, azure, azure-cosmos |
| Databases | jdbc, mongodb, cassandra, hbase, redis, neo4j, clickhouse, influxdb, singlestore, elasticsearch |
| Messaging | kafka, pulsar, rabbitmq, amqp, jms, mqtt, solace |
| File Formats | parquet, csv, json, xml, thrift, iceberg |
| Other | snowflake, splunk, cdap, debezium, hadoop-format, kudu, solr, tika |

## Testing I/O Connectors

### Unit Tests
```bash
./gradlew :sdks:java:io:kafka:test
./gradlew :sdks:java:io:jdbc:test
```

### Integration Tests

#### On Direct Runner
```bash
./gradlew :sdks:java:io:google-cloud-platform:integrationTest
```

#### With Custom GCP Settings
```bash
./gradlew :sdks:java:io:google-cloud-platform:integrationTest \
  -PgcpProject=<project> \
  -PgcpTempRoot=gs://<bucket>/path
```

#### With Explicit Pipeline Options
```bash
./gradlew :sdks:java:io:jdbc:integrationTest \
  -DbeamTestPipelineOptions='["--runner=TestDirectRunner"]'
```

## Integration Test Framework
Located at `it/` directory:
- `it/common/` - Common test utilities
- `it/google-cloud-platform/` - GCP-specific test infrastructure
- `it/jdbc/` - JDBC test infrastructure
- `it/kafka/` - Kafka test infrastructure
- `it/testcontainers/` - Testcontainers support

## Writing Integration Tests

### Basic Structure
```java
@RunWith(JUnit4.class)
public class MyIOIT {
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Test
  public void testWriteAndRead() {
    // Write data
    writePipeline.apply(Create.of(testData))
                 .apply(MyIO.write().to(destination));
    writePipeline.run().waitUntilFinish();

    // Read and verify
    PCollection<String> results = readPipeline.apply(MyIO.read().from(destination));
    PAssert.that(results).containsInAnyOrder(expectedData);
    readPipeline.run().waitUntilFinish();
  }
}
```

### Using TestPipeline
```java
@Rule public TestPipeline pipeline = TestPipeline.create();
```

TestPipeline:
- Blocks on run by default (on TestDataflowRunner)
- Has 15-minute default timeout
- Reads options from `beamTestPipelineOptions` system property

## GCP I/O Connectors

### BigQuery
```java
// Read
pipeline.apply(BigQueryIO.readTableRows().from("project:dataset.table"));

// Write
data.apply(BigQueryIO.writeTableRows()
    .to("project:dataset.table")
    .withSchema(schema)
    .withWriteDisposition(WriteDisposition.WRITE_APPEND));
```

### Pub/Sub
```java
// Read
pipeline.apply(PubsubIO.readStrings().fromTopic("projects/project/topics/topic"));

// Write
data.apply(PubsubIO.writeStrings().to("projects/project/topics/topic"));
```

### Cloud Storage (TextIO)
```java
// Read
pipeline.apply(TextIO.read().from("gs://bucket/path/*.txt"));

// Write
data.apply(TextIO.write().to("gs://bucket/output").withSuffix(".txt"));
```

## Kafka Connector
```java
// Read
pipeline.apply(KafkaIO.<String, String>read()
    .withBootstrapServers("localhost:9092")
    .withTopic("topic")
    .withKeyDeserializer(StringDeserializer.class)
    .withValueDeserializer(StringDeserializer.class));

// Write
data.apply(KafkaIO.<String, String>write()
    .withBootstrapServers("localhost:9092")
    .withTopic("topic")
    .withKeySerializer(StringSerializer.class)
    .withValueSerializer(StringSerializer.class));
```

## JDBC Connector
```java
// Read
pipeline.apply(JdbcIO.<Row>read()
    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
        .create("org.postgresql.Driver", "jdbc:postgresql://host/db"))
    .withQuery("SELECT * FROM table"));

// Write
data.apply(JdbcIO.<Row>write()
    .withDataSourceConfiguration(config)
    .withStatement("INSERT INTO table VALUES (?, ?)"));
```

## Python I/O Location
`sdks/python/apache_beam/io/`

### Common Python I/Os
- `textio` - Text files
- `fileio` - General file operations
- `avroio` - Avro files
- `parquetio` - Parquet files
- `gcp/` - GCP connectors (BigQuery, Pub/Sub, Datastore, etc.)

## Cross-language I/O
Beam supports using I/O connectors from one SDK in another via the expansion service.

```bash
# Start Java expansion service
./gradlew :sdks:java:io:expansion-service:runExpansionService
```

## Creating New Connectors
See [Developing I/O connectors](https://beam.apache.org/documentation/io/developing-io-overview)

Key components:
1. **Source** - Reads data (bounded or unbounded)
2. **Sink** - Writes data
3. **Read/Write transforms** - User-facing API
