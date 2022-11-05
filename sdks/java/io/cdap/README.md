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

# CdapIO
CdapIO provides I/O transforms to for [CDAP](https://cdap.io/) plugins.

## What is CDAP?

[CDAP](https://cdap.io/) is an application platform for building and managing data applications in hybrid and multi-cloud environments.
It enables developers, business analysts, and data scientists to use a visual rapid development environment and utilize common patterns,
data, and application abstractions to accelerate the development of data applications, addressing a broader range of real-time and batch use cases.

[CDAP plugins](https://github.com/data-integrations) types:
- Batch source
- Batch sink
- Streaming source

To learn more about CDAP plugins please see [io.cdap.cdap.api.annotation.Plugin](https://javadoc.io/static/io.cdap.cdap/cdap-api/6.7.2/io/cdap/cdap/api/annotation/Plugin.html) and [Data Integrations](https://github.com/data-integrations) plugins repository.

## CDAP Batch plugins support in CDAP IO

CdapIO supports CDAP Batch plugins based on Hadoop `InputFormat` and `OutputFormat`. CDAP batch plugins support is implemented using [HadoopFormatIO](https://beam.apache.org/documentation/io/built-in/hadoop/), https://github.com/apache/beam/tree/master/sdks/java/io/hadoop-format.

CdapIO currently supporta for the following CDAP Batch plugins by referencing `CDAP plugin` class:
* [Hubspot](https://github.com/data-integrations/hubspot)
* [Salesforce](https://github.com/data-integrations/salesforce)
* [ServiceNow](https://github.com/data-integrations/servicenow-plugins)
* [Zendesk](https://github.com/data-integrations/zendesk)

To add CdapIO support for a new CDAP or custom Batch [Plugin](src/main/java/org/apache/beam/sdk/io/cdap/Plugin.java), pass corresponding classes:
* `CDAP plugin`
* Hadoop `InputFormat` for source
* Hadoop `OutputFormat` for sink
* Hadoop `FormatProvider`.

To learn more please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete).

## CDAP Streaming plugins support in CDAP IO

CdapIO supports CDAP Streaming plugins based on [Apache Spark Receiver](https://spark.apache.org/docs/2.4.0/streaming-custom-receivers.html) `org.apache.spark.streaming.receiver.Receiver`. CDAP streaming plugins support is implemented using [SparkReceiverIO](https://github.com/apache/beam/tree/master/sdks/java/io/sparkreceiver).

CdapIO supports CDAP Streaming plugins based on [Spark Receiver](https://spark.apache.org/docs/2.4.0/streaming-custom-receivers.html) `org.apache.spark.streaming.receiver.Receiver`  that implements [HasOffset](https://github.com/apache/beam/blob/master/sdks/java/io/sparkreceiver/src/main/java/org/apache/beam/sdk/io/sparkreceiver/HasOffset.java) interface.

CdapIO currently supporta for the following CDAP Streaming plugin by referencing `CDAP plugin` class:
* [Hubspot](https://github.com/data-integrations/hubspot)

To add CdapIO support for a new or custom CDAP Streaming SparkReceiver [Plugin](src/main/java/org/apache/beam/sdk/io/cdap/Plugin.java), register the new plugin via [MappingUtils](src/main/java/org/apache/beam/sdk/io/cdap/MappingUtils.java).

To learn more please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete).

## Dependencies

To use CdapIO please add a dependency on `beam-sdks-java-io-cdap`.

```maven
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-cdap</artifactId>
    <version>...</version>
</dependency>
```

## Documentation

The documentation and usage examples are maintained in JavaDoc for [CdapIO.java](src/main/java/org/apache/beam/sdk/io/cdap/CdapIO.java).
