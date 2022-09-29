---
title: "Apache Beam: IO Standards for Syntax and semantics"
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
# Beam I/O Standards: API Syntax and Semantics

## Overview

This outlines a set of guidelines for Apache Beam's IO connectors. These
guidelines define the comunity's best practices to ease the review, development,
maintenance and usage of new connectors.

These guidelines are written with the following principles in mind:

- Consistency makes an API easier to learn
- If there are multiple ways of doing something, we shall encourage one for consistency.
- With a couple minutes of studying documentation, users should be able to pick up most IO connectors
- The design of a new IO should consider the possibility of evolution
- Transforms should integrate well with other Beam utilities


## Java

### General

<table class="table table-bordered">
<tr>
<th>Index</th><th>Guideline</th>
</tr>
<tr>
<td>G1</td>
<td>
<p>If the IO lives in Apache Beam it should be placed in the package :
org.apache.beam.sdk.io.{connector}</p>
<p>eg: org.apache.beam.sdk.io.bigquery</p>
</td>
</tr>
<tr>
<td>G2</td>
<td>There will be a class named {connector}IO which is the primary class used in working with the connector in a pipeline.
eg: org.apache.beam.sdk.io.bigquery.BigQueryIO</td>
</tr>
<tr>
<td>G3</td>
<td>Given that the IO will be placed in the package org.apache.beam.sdk.io.{connector}, it is also suggested that its integration tests will live under the package org.apache.beam.sdk.io.{connector}.testing. This will cause integration tests to work with the standard user-facing interfaces of the connector.

Unit tests should reside in the same package (i.e. org.apache.beam.sdk.io.{connector}), as they may often test internals of the connector.

</td></tr>
<tr>
<td>G4</td>
<td>An IO transform should avoid receiving user lambdas to map elements from a user type to a connector-specific type. Instead, they should interface with a connector-specific data type (with schema information when possible).

When necessary, then an IO transform should receive a type parameter that specifies the input type (for sinks) or output type (for sources) of the transform.

An IO transform may not have a type parameter only if it is certain that its output type will not change (e.g. FileIO.MatchAll and other FileIO transforms).
</td></tr>
<tr>
<td>G5</td>
<td>As part of the API of a Beam IO connector, it is discouraged to expose third-party libraries in the public API of a Beam API or connector. Instead, a Beam-native interface that can be translated into the third-library object is preferrable. This is because:
Exposing third-party libraries will make dependency upgrades difficult, because of Beam’s backwards compatibility guarantees
Exposing third-party libraries may force dependencies onto Beam users

In cases where the external library is complex, or changes frequently, the connector may expose library-specific classes in its API.

This requirement has the shortcoming that Beam will simply mirror external library API objects, but it will allow us to upgrade external libraries if needed, and let users deal with versions as well.

</td>
</tr>
</table>

### Classes / Methods Properties

<table>
<tr>
<th></th><th>Java Syntax</th><th>Semantics</th>
</tr>
<tr>
<td>C1</td>
<td>class IO.Read</td>
<td>Gives access to the class that represents reads within the I/O. The Read class should implement an interface similar to the builder pattern (e.g. withX(...).withY(...)).

This class should not be created directly by a user. It should be created by a top-level utility method (see M1, M2).
</td>
</tr>
<tr>
<td>C2</td>
<td>class IO.ReadAll</td>
<td>A few different sources implement runtime configuration for reading from a data source. This is a valuable pattern because it enables a purely batch source to become a more sophisticated streaming source.

As much as possible, this type of transform should have the type richness of a construction-time-configured transform:
Support Beam Row output with a schema known at construction-time
Extra configuration may be needed (and acceptable) in this case (e.g. a SchemaProvider parameter, a Schema parameter, a Schema Catalog or a utility of that sort).
The input PCollection should have a fixed type with a schema, so it can be easily manipulated by users.

E.g. JdbcIO.ReadAll, ParquetIO.ReadFiles</td>
</tr>
<tr>
<td>C3</td>
<td>class IO.Write</td>
<td>Gives access to the class that represents writes within the I/O. The Write class should implement an interface similar to the builder pattern (e.g. withX(...).withY(...)).

This class should not be created directly by a user. It should be created by a top-level utility method (see M1, M2).</td>
</tr>
<tr>
<td>C4</td>
<td>Other Transform Classes</td>
<td>Some data storage and external systems implement APIs that do not adjust easily to Read or Write semantics (e.g. FhirIO implements several different transforms that fetch or send data to Fhir).

These classes should be added only if it is impossible or prohibitively difficult to encapsulate their functionality as part of extra configuration of Read, Write and ReadAll transforms, to avoid increasing the cognitive load on users.

These classes should not be created directly by a user. They should be created by a top-level static method (much like M1, M2).
</td>
</tr>
<tr>
<td>C5</td>
<td>Utility Classes</td>
<td>Some connectors rely on other user-facing classes to set configuration parameters.
 (e.g. JdbcIO.DataSourceConfiguration). These classes should be nested within the {Connector}IO class.

This format makes them visible in the main Javadoc, and easy to discover by users.

</td>
</tr>
<tr>
<td>M1</td>
<td>Method IO&lt;T&gt;.write()</td>
<td>The top-level IO class will provide a static method to start constructing a IO.Write transform. This returns a PTransform with a single input PCollection, and a Write.Result output (see T2).

This method should not specify in its name any of the following :
Internal data format
Strategy used to write data
Input or output data type

The above should be specified via configuration parameters if possible. If not possible, then a new static method may be introduced, but this must be exceptional.</td>
</tr>
<tr>
<td>M2</td>
<td>Method IO&lt;T&gt;.read()</td>
<td>
The method to start constructing a IO.Read transform. This returns a PTransform with a single output PCollection (see T1).

This method should not specify in its name any of the following :
Internal data format
Strategy used to read data
Output data type

The above should be specified via configuration parameters if possible. If not possible, then a new static method may be introduced, but this must be exceptional, and documented in the IO header as part of the API.

The initial static constructor method may receive parameters if these are few and general, or if they are necessary to configure the transform (e.g. FhirIO.exportResourcesToGcs, JdbcIO.ReadWithPartitions needs a TypeDescriptor for initial configuration).

</td>
</tr>
<tr>
<td>M3</td>
<td>
IO.Read.from(source)
</td>
<td>
A Read transform must provide a from method where users can specify where to read from. If a transform can read from different kinds of sources (e.g. tables, queries, topics, partitions), then multiple implementations of this from method can be provided to accommodate this:

- IO.Read from(Query query)
- IO.Read from(Table table) / from(String table)
- IO.Read from (Topic topic)
- IO.Read from(Partition partition)

The input type for these methods can reflect the external source’s API (e.g. Kafka TopicPartition should use a Beam-implemented TopicPartition object).
</td>
</tr>
<tr>
<td>M4</td>
<td>
IO.Read.fromABC(String abc)
</td>
<td>
Discouraged
A Read transform that may read from different kinds of sources (e.g. tables, queries, topics, partitions) may provide several methods to specify the kind of source that it will read from:

IO.Read from(String table)
IO.Read fromQuery(String query)
IO.Read fromTable(String table)
IO.Read fromTopic(String topic)
IO.Read fromPartition(String partition)

This pattern is discouraged, because it increases the variations that users need to memorize and consider when utilizing an IO.
</td>
</tr>
<tr>
<td>M5</td>
<td>IO.Write.to(destination)</td>
<td>
A Write transform must provide a to method where users can specify where to write data. If a transform can write to different kinds of sources while still using the same input element type(e.g. tables, queries, topics, partitions), then multiple implementations of this from method can be provided to accommodate this:

IO.Write to(Query query)
IO.Write to(Table table) / from(String table)
IO.Write to(Topic topic)
IO.Write to(Partition partition)

The input type for these methods can use an Apache Beam utility that reflects the external source’s API (e.g. Kafka TopicPartition). Per G5, external libraries should not be exposed by Apache Beam., or an interface given by Apache Beam.

If different kinds of destinations require different types of input object types, then these should be done in separate IO connectors.

</td>
</tr>
<tr>
<td>M6</td>
<td>IO.Write.to(DynamicDestination destination)</td>
<td>
A write transform may enable writing to more than one destination. This can be a complicated pattern that should be implemented carefully (it is the preferred pattern for connectors that will likely have multiple destinations in a single pipeline).

The preferred pattern for this is to define a DynamicDestinations interface (e.g. BigQueryIO.DynamicDestinations) that will allow the user to define all necessary parameters for the configuration of the destination.

The DynamicDestinations interface also allows maintainers to add new methods over time (with default implementations to avoid breaking existing users) that will define extra configuration parameters if necessary.
</td>
</tr>
<tr>
<td>M7</td>
<td>IO.Write.toABC(destination)</td>
<td>
Discouraged
A Write transform that may write to different kinds of sources (e.g. tables, topics, partitions, buckets) may provide several methods to specify the kind of source that it will write to:

- IO.Write to(String table)
- IO.Write toTable(String table)
- IO.Write toTopic(String topic)
- IO.Write toPartition(String partition)

This pattern is discouraged, because it increases the variations that users need to memorize and consider when utilizing an IO.
</td>
</tr>
<tr>
<td>M8</td>
<td>class IO.Read.withX</td>
<td>
IO.Write.withX
withX provides a method for configuration to be passed to the Read method, where X represents the configuration to be created. With the exception of generic with statements ( defined below ) the I.O should attempt to match the name of the configuration option with that of the option name in the source.

These methods should return a new instance of the IO rather than modifying the existing instance.

E.g. TextIO.Read.withCompression

</td>
</tr>
<tr>
<td>M9</td>
<td>
IO.Read.withConfigObject
IO.Write.withConfigObject
</td>
<td>
Some connectors in Java receive a configuration object as part of their configuration. This pattern is encouraged only for particular cases. In most cases, a connector can hold all necessary configuration parameters at the top level.

To determine whether a multi-parameter configuration object is an appropriate parameter for a high level transform, the configuration object must:
- Hold only properties related to the connection/authentication parameters for the external data store (e.g. JdbcIO.DataSourceConfiguration).
  - Generally, secrets should not be passed as parameters, unless an alternative is not feasible. For secret management, a secret-management service or KMS is the recommended approach.
- Or mirror an API characteristic from the external data source (e.g. KafkaIO.Read.withConsumerConfigUpdates), without exposing that external API in the Beam API.
- The method should mirror the name of the API object (e.g. given an object SubscriptionStatConfig, a method would be withSubscriptionStatConfig).
Or when a connector can support different configuration ‘paths’ where a particular property requires other properties to be specified (e.g. BigQueryIO’s method will entail various different properties). (see last examples).

E.g. JdbcIO.DataSourceConfiguration, SpannerConfig, KafkaIO.Read.withConsumerConfigUpdates

BigQueryIO.write()
.withWriteConfig(FileLoadsConfig.withAvro()
.withTriggeringFrequency()...)

BigQueryIO.write()
.withWriteConfig(StreamingInsertsConfig.withDetailedError()
.withExactlyOnce().etc..)
</td>
</tr>
<tr>
<td>M10</td>
<td>class IO.Write.withFormatFunction</td>
<td>
<b>Discouraged - except for dynamic destinations</b>

For sources that can receive Beam Row-typed PCollections (see T2), the format function should not be necessary, because Beam should be able to format the input data based on its schema.

For sinks providing Dynamic Destination functionality, elements may carry data that helps determine their destination. These data may need to be removed before writing to their final destination.

To include this method, a connector should:
- Show that it’s not possible to perform data matching automatically
- Support Dynamic Destinations and need changes to the input data due to that reason

</td>
</tr>
<tr>
<td>M11</td>
<td>IO.Read.withCoder IO.Write.withCoder</td>
<td>
<b>Strongly Discouraged</b>

Sets the coder to use to encode/decode the element type of the output / input PCollection of this connector. In general, it is recommended that sources will:
Return Row objects with a schema that is automatically inferred
Automatically set the necessary coder by having fixed output/input types, or inferring their output/input types

If nether #1 and #2 are possible, then a withCoder method can be added.
</td>
</tr>
<tr>
<td>M12</td>
<td>IO.ABC.withEndpoint / with{IO}Client / withClient</td>
<td>
Connector transforms should provide a method to override the interface between themselves and the external system that they communicate with. This can enable various uses:

Local testing by mocking the destination service
User-enabled metrics, monitoring, and security handling in the client.
Integration testing based on emulators

E.g. BigQueryIO.Write.withTestServices(BigQueryServices)
</td>
</tr>
</table>

### Types

<table>
<tr>
<th></th><th>Java Syntax</th><th>semantics</th></tr>
<tr>
<td>T1</td>
<td>Method IO.Read.expand</td>
<td>
The expand method of a Read transform must return a PCollection object with a type. The type may be parameterized or fixed to a class.
</td>
</tr>
<tr>
<td>T2</td>
<td>Method IO.Read.expand’s PCollection type</td>
<td>
The type of the PCollection will usually be one of the following four options. For each of these option, the encoding / data is recommended to be as follows:

- A pre-set, basic Java type (e.g. String)
  - This encoding should be simple, and use a simple Beam coder (e.g. Utf8StringCoder)
- A pre-set POJO type (e.g. Metadata) with a schema
  - The preferred strategy for these is to define the output type as an @AutoValue, with @DefaultSchema and @SchemaCreate annotations. This will ensure compact, fast encoding with RowCoder.
- A Beam Row with a specific schema
- A type with a schema that’s not know a priori (i.e. at construction time)

In all cases, asking a user to pass a coder (e.g. withCoder(...)) is discouraged.
</td>
</tr>
<tr>
<td>T3</td>
<td>method IO.Write.expand</td>
<td>
The expand method of any write transform must return a type IO.Write.Result object that extends a PCollectionTuple. This object allows transforms to return metadata about the results of its writing and allows this write to be followed by other PTransforms.
If the Write transform would not need to return any metadata, a Write.Result object is still preferable, because it will allow the transform to evolve its metadata over time.

Examples of metadata:
- Failed elements and errors
- Successfully written elements
- API tokens from calls issued by the transform

E.g. BigQueryIO’s WriteResult
</td>
</tr>
</table>

### Evolution

Over time, IO connectors need to evolve to address new use cases, or use new APIs under the covers. Some examples of necessary evolution of an IO connector:
- A new data type needs to be supported within it (e.g. any-type partitioning in JdbcIO.ReadWithPartitions)
- A new backend API needs to be supported

<table>
<tr>
<th></th><th>Java Syntax</th><th>Semantics</th>
</tr>
<tr>
<td>E1</td>
<td>Top-level static methods</td>
<td>
In general, one should resist adding a completely new static method for functionality that can be captured as configuration within an existing method.

An example of too many top-level methods that could be supported via configuration is PubsubIO

A new top-level static method should only be added in the following cases:
- A new PTransform is being added that cannot / does not make sense to be supported as a simple configuration parameter of the existing PTransforms (e.g. FhirIO’s various transforms, any ReadAll transform).
- A feature that represents a new recommended standard to use the transform (e.g. JdbcIO’s ReadWithPartitions)
- A change in the usual way of interacting with this transform, or a parameter that’s necessary to initialize the transform (e.g. BigQueryIO.read(...) method vs read())
</td>
</tr></table>