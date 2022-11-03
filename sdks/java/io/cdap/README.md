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

CdapIO contains I/O transforms which allow you to read messages from CDAP plugins (see io.cdap.cdap.api.annotation.Plugin).

##What is CDAP?

[CDAP](https://cdap.io/) is an application platform for building and managing data applications in hybrid and multi-cloud environments.
It enables developers, business analysts, and data scientists to use a visual rapid development environment and utilize common patterns,
data, and application abstractions to accelerate the development of data applications, addressing a broader range of real-time and batch use cases.

[CDAP plugins](https://github.com/data-integrations) have similar structures and patterns and might be several types:
- Batch Source
- Batch Sink
- Streaming Source

##Batch plugins support in CDAP IO

Batch plugins support is implemented using [HadoopFormatIO](https://github.com/apache/beam/tree/master/sdks/java/io/hadoop-format).

CdapIO supports CDAP Batch plugins based on **Hadoop** *Input/OutputFormat*.

There are multiple CDAP Batch plugins that can be used just by *CDAP plugin class* without additional actions.
Also, **any custom** Batch [Plugin](src/main/java/org/apache/beam/sdk/io/cdap/Plugin.java) can be used by passing corresponding *CDAP plugin class* and **Hadoop** *Format* and *FormatProvider* classes (*Input* for source / *Output* for sink).

To learn more please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete).

##Streaming plugins support in CDAP IO

Streaming plugins support is implemented using [SparkReceiverIO](https://github.com/apache/beam/tree/master/sdks/java/io/sparkreceiver).

CdapIO supports CDAP Streaming plugins based on custom *SparkReceiver*(org.apache.spark.streaming.receiver.Receiver) that implements [HasOffset](https://github.com/apache/beam/blob/master/sdks/java/io/sparkreceiver/src/main/java/org/apache/beam/sdk/io/sparkreceiver/HasOffset.java) interface.

There are several Streaming plugins that can be used just by *CDAP plugin class* without any additional actions.
Also, **any custom** Streaming [Plugin](src/main/java/org/apache/beam/sdk/io/cdap/Plugin.java) can be used by registering it via [MappingUtils](src/main/java/org/apache/beam/sdk/io/cdap/MappingUtils.java).

To learn more please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete).

## Dependencies

To use CdapIO you must first add a dependency on `beam-sdks-java-io-cdap`.

```maven
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-cdap</artifactId>
    <version>...</version>
</dependency>
```

## Documentation

The documentation is maintained in JavaDoc for CdapIO class. It includes
usage examples and primary concepts.
- [CdapIO.java](src/main/java/org/apache/beam/sdk/io/cdap/CdapIO.java)
