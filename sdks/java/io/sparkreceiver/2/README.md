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

SparkReceiverIO contains I/O transforms which allow you to read messages from Spark Receiver (org.apache.spark.streaming.receiver.Receiver).

## Dependencies

To use SparkReceiverIO you must first add a dependency on `beam-sdks-java-io-sparkreceiver`.

```maven
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-sparkreceiver</artifactId>
    <version>...</version>
</dependency>
```

## Documentation

The documentation is maintained in JavaDoc for SparkReceiverIO class. It includes
usage examples and primary concepts.
- [SparkReceiverIO.java](src/main/java/org/apache/beam/sdk/io/sparkreceiver/SparkReceiverIO.java)
