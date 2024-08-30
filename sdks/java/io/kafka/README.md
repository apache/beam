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

KafkaIO contains I/O transforms which allow you to read/write messages from/to [Apache Kafka](http://kafka.apache.org/).

## Dependencies

To use KafkaIO you must first add a dependency on `beam-sdks-java-io-kafka`. KafkaIO supports
multiple versions of Kafka clients at run time. It does not pull a specific version `kafka-clients`
transitively. You need to include a compatible version of `kafka-clients` as runtime dependency.
Usually current and recent versions of Kafka are supported, please see JavaDoc for KafkaIO for
complete list.

```maven
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-kafka</artifactId>
    <version>...</version>
</dependency>

<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>a_recent_version</version>
  <scope>runtime</scope>
</dependency>
```

## Documentation

The documentation is maintained in JavaDoc for KafkaIO class. It includes
 usage examples and primary concepts.
- [KafkaIO.java](src/main/java/org/apache/beam/sdk/io/kafka/KafkaIO.java)

### Protobuf tests
This recreates the proto descriptor set included in this resource directory.

```bash
protoc \
 -Isdks/java/io/kafka/src/test/resources/ \
 --descriptor_set_out=sdks/java/io/kafka/src/test/resources/proto_byte/file_descriptor/proto_byte_utils.pb \
 sdks/java/io/kafka/src/test/resources/proto_byte/proto_byte_utils.proto
```