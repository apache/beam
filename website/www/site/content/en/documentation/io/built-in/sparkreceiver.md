---
title: "SparkReceiver IO"
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

# SparkReceiver IO

SparkReceiverIO is a transform for reading data from an Apache Spark Receiver as an unbounded source.

## Spark Receivers support

`SparkReceiverIO` currently supports [Apache Spark Receiver](https://spark.apache.org/docs/2.4.0/streaming-custom-receivers.html).

Requirements for `Spark Receiver`:
- Version of Spark should be 2.4.*.
- `Spark Receiver` should support work with offsets.
- `Spark Receiver` should implement [HasOffset](https://github.com/apache/beam/blob/master/sdks/java/io/sparkreceiver/2/src/main/java/org/apache/beam/sdk/io/sparkreceiver/HasOffset.java) interface.
- Records should have the numeric field that represents record offset.

For more details please see [SparkReceiverIO readme](https://github.com/apache/beam/blob/master/sdks/java/io/sparkreceiver/2/README.md).

## Streaming reading using SparkReceiverIO

In order to read from `Spark Receiver` you will need to pass:

- `getOffsetFn`, which is `SerializableFunction` that defines how to get `Long` record offset from a record.
- `receiverBuilder`, which is needed for building instances of `Spark Receiver` that use Apache Beam mechanisms instead of Spark environment.

You can easily create `receiverBuilder` object by passing the following parameters:

- Class of your `Spark Receiver`.
- Constructor arguments needed to create an instance of your `Spark Receiver`.

For example:

{{< highlight java >}}
//In this example, MyReceiver accepts a MyConfig object as its only constructor parameter.
MyConfig myPluginConfig = new MyConfig(authToken, apiServerUrl);
Object[] myConstructorArgs = new Object[] {myConfig};
ReceiverBuilder<String, MyReceiver<String>> myReceiverBuilder =
  new ReceiverBuilder<>(MyReceiver.class)
    .withConstructorArgs(myConstructorArgs);
{{< /highlight >}}

Then you will be able to pass this `receiverBuilder` object to `SparkReceiverIO`.

For example:

{{< highlight java >}}
SparkReceiverIO.Read<String> readTransform =
  SparkReceiverIO.<String>read()
    .withGetOffsetFn(Long::valueOf)
    .withSparkReceiverBuilder(myReceiverBuilder)
p.apply("readFromMyReceiver", readTransform);
{{< /highlight >}}

### Read data with optional parameters

Optionally you can pass the following optional parameters:

- `pullFrequencySec`, which is delay in seconds between polling for new records updates.
- `startOffset`, which is inclusive start offset from which the reading should be started.
- `timestampFn`, which is a `SerializableFunction` that defines how to get an `Instant timestamp` from a record.

For example:

{{< highlight java >}}
SparkReceiverIO.Read<String> readTransform =
  SparkReceiverIO.<String>read()
    .withGetOffsetFn(Long::valueOf)
    .withSparkReceiverBuilder(myReceiverBuilder)
    .withPullFrequencySec(1L)
    .withStartOffset(1L)
    .withTimestampFn(Instant::parse);
p.apply("readFromReceiver", readTransform);
{{< /highlight >}}

### Examples for specific Spark Receiver

#### CDAP Hubspot Receiver

{{< highlight java >}}
ReceiverBuilder<String, HubspotReceiver<String>> hubspotReceiverBuilder =
  new ReceiverBuilder<>(HubspotReceiver.class)
    .withConstructorArgs(hubspotConfig);
SparkReceiverIO.Read<String> readTransform =
  SparkReceiverIO.<String>read()
    .withGetOffsetFn(GetOffsetUtils.getOffsetFnForHubspot())
    .withSparkReceiverBuilder(hubspotReceiverBuilder)
p.apply("readFromHubspotReceiver", readTransform);
{{< /highlight >}}
