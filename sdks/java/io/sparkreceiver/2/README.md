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
# SparkReceiverIO

SparkReceiverIO provides I/O transforms to read messages from an [Apache Spark Receiver](https://spark.apache.org/docs/2.4.0/streaming-custom-receivers.html) `org.apache.spark.streaming.receiver.Receiver` as an unbounded source.

## Prerequistes

SparkReceiverIO supports [Spark Receivers](https://spark.apache.org/docs/2.4.0/streaming-custom-receivers.html) (Spark version 2.4).
1. Corresponding Spark Receiver should implement [HasOffset](https://github.com/apache/beam/blob/master/sdks/java/io/sparkreceiver/src/main/java/org/apache/beam/sdk/io/sparkreceiver/HasOffset.java) interface.
2. Records should have the numeric field that represents record offset. *Example:* `RecordId` field for Salesforce and `vid` field for Hubspot Receivers.
   For more details please see [GetOffsetUtils](https://github.com/apache/beam/tree/master/examples/java/cdap/src/main/java/org/apache/beam/examples/complete/cdap/utils/GetOffsetUtils.java) class from CDAP plugins examples.

## Adding support for a new Spark Receiver

To add SparkReceiverIO support for a new Spark `Receiver`, perform the following steps:
1. Add Spark Receiver to the Maven Central repository (see [Sonatype publishing guidelines](https://central.sonatype.org/publish/)). *Example:* [Hubspot CDAP plugin Maven repository](https://mvnrepository.com/artifact/io.cdap/hubspot-plugins/1.0.0).
2. Add Spark Receiver Maven dependency to the `build.gradle` file. *Example:* ``implementation "io.cdap:hubspot-plugins:1.0.0"``.
3. Implement function that will define how to get `Long offset` from the record of the Spark Receiver.
   *Example:* see [GetOffsetUtils](https://github.com/apache/beam/tree/master/examples/java/cdap/src/main/java/org/apache/beam/examples/complete/cdap/utils/GetOffsetUtils.java) class from CDAP plugins examples.
4. Construct `ReceiverBuilder` object by passing class of record that you want to read (e.g. String) and your Spark `Receiver` class name (dependency from step 2). *Example:*
   ```
      ReceiverBuilder<String, HubspotReceiver> receiverBuilder =
      new ReceiverBuilder<>(HubspotReceiver.class).withConstructorArgs();
   ```
5. Use your Spark `Receiver` with SparkReceiverIO:
    1. Pass correct `getOffsetFn` (from step 3) and correct `ReceiverBuilder` (from step 4). *Example:*
   ```
   SparkReceiverIO.Read<V> reader =
            SparkReceiverIO.<V>read()
                .withGetOffsetFn(getOffsetFn)
                .withSparkReceiverBuilder(receiverBuilder);
   ```


To learn more, please check out CDAP Streaming plugins [complete examples](https://github.com/apache/beam/tree/master/examples/java/cdap) where Spark Receivers are used.

## Dependencies

To use SparkReceiverIO, add a dependency on `beam-sdks-java-io-sparkreceiver`.

```maven
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-sparkreceiver</artifactId>
    <version>...</version>
</dependency>
```

## Documentation

The documentation and usage examples are maintained in JavaDoc for [SparkReceiverIO class](src/main/java/org/apache/beam/sdk/io/sparkreceiver/SparkReceiverIO.java).
