---
title: "IO Standards"
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
# I/O Standards

## Overview

This Apache Beam I/O Standards document lays out the prescriptive guidance for 1P/3P developers developing an Apache Beam I/O connector. These guidelines aim to create best practices encompassing documentation, development and testing in a simple and concise manner.


### What are built-in I/O Connectors?

An I/O connector (I/O) living in the Apache Beam Github repository is known as a **Built-in I/O connector**. Built-in I/O’s have their [integration tests](#integration-tests) and performance tests routinely run by the Google Cloud Dataflow Team using the Dataflow Runner and metrics published publicly for [reference](#dashboard). Otherwise, the following guidelines will apply to both unless explicitly stated.


# Guidance


## Documentation

This section lays out the superset of all documentation that is expected to be made available with an I/O. The Apache Beam documentation referenced throughout this section can be found [here](/documentation/). And generally a good example to follow would be the built-in I/O, [Snowflake I/O](/documentation/io/built-in/snowflake/).


### Built-in I/O

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p>Provided code docs for the relevant language of the I/O. This should also have links to any external sources of information within the Apache Beam site or external documentation location.
         <p>Examples:
         <ul>
            <li><a href="https://beam.apache.org/releases/javadoc/current/overview-summary.html">Java doc</a>
            <li><a href="https://beam.apache.org/releases/pydoc/current/">Python doc</a>
            <li><a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam">Go doc</a>
         </ul>
      </td>
   </tr>
   <tr>
      <td>
         <p>Add a new page under <strong>I/O connector guides</strong> that covers specific tips and configurations. The following shows those for <a href="/documentation/io/built-in/parquet/">Parquet</a>, <a href="/documentation/io/built-in/hadoop/">Hadoop</a> and others.
         <p>Examples:
         <p><img src="/images/io-standards/io-connector-guides-screenshot.png" width="" alt="I/O connector guides screenshot" title="I/O connector guides screenshot"></img>
      </td>
   </tr>
   <tr>
      <td>
         <p>Formatting of the section headers in your Javadoc/Pythondoc should be consistent throughout such that programmatic information extraction for other pages can be enabled in the future.
         <p>Example <strong>subset</strong> of sections to include in your page in order:
         <ol>
            <li><a href="#before-you-start">Before you start</a>
            <li>{Connector}IO basics
            <li>Supported Features
               <ol>
                  <li><a href="#relational">Relational</a>
                  </li>
               </ol>
            <li><a href="#authentication">Authentication</a>
            <li>Reading from {Connector}
            <li>Writing to {Connector}
            <li><a href="#resource-scalability">Resource scalability</a>
            <li><a href="#limitations">Limitations</a>
            <li>Reporting an Issue
            </li>
         </ol>
         <p>Example:
         <p>The KafkaIO <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.html">JavaDoc</a>
      </td>
   </tr>
   <tr id="relational">
      <td>
         <p>I/O Connectors should include a table under <strong>Supported Features</strong> subheader that indicates the <a href="https://2022.beamsummit.org/sessions/relational-beam/">Relational Features</a> utilized.
         <p>Relational Features are concepts that can help improve efficiency and can optionally be implemented by an I/O Connector. Using end user supplied pipeline configuration (<a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/io/SchemaIO.html">SchemaIO</a>) and user query (<a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/FieldAccessDescriptor.html">FieldAccessDescriptor</a>) data, relational theory is applied to derive improvements such as faster pipeline execution, lower operation costs and less data read/written.
         <p>Example table:
         <p><img src="/images/io-standards/io-connector-relational-features-table.png" width="" alt="I/O connector guides screenshot" title="I/O connector guides screenshot"></img>
         <p>
{{< highlight markdown >}}
<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards-relational-features">
   <tr>
      <th>
         <p><strong>Relational Feature</strong>
      </th>
      <th>
         <p><strong>Supported</strong>
      </th>
      <th>
         <p><strong>Notes</strong>
      </th>
   </tr>
   <tr>
      <td>
         <p>Column Pruning
      </td>
      <td>
         <p>Yes/No
      </td>
      <td>
         <p>To Be Filled
      </td>
   </tr>
   <tr>
      <td>
         <p>Filter Pushdown
      </td>
      <td>
         <p>Yes/No
      </td>
      <td>
         <p>To Be Filled
      </td>
   </tr>
   <tr>
      <td>
         <p>Table Statistics
      </td>
      <td>
         <p>Yes/No
      </td>
      <td>
         <p>To Be Filled
      </td>
   </tr>
   <tr>
      <td>
         <p>Partition Metadata
      </td>
      <td>
         <p>Yes/No
      </td>
      <td>
         <p>To Be Filled
      </td>
   </tr>
   <tr>
      <td>
         <p>Metastore
      </td>
      <td>
         <p>Yes/No
      </td>
      <td>
         <p>To Be Filled
      </td>
   </tr>
</table>
</div>
{{< /highlight >}}
         </p>
         <p>Example implementations:
         <p>BigQueryIO <a href="https://github.com/apache/beam/blob/5bb13fa35b9bc36764895c57f23d3890f0f1b567/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.java#L1813">Column Pruning</a> via ProjectionPushdown to return only necessary columns indicated by an end user's query. This is achieved using BigQuery DirectRead API.
      </td>
   </tr>
   <tr>
      <td>
         <p>Add a page under <strong>Common pipeline patterns</strong>, if necessary, outlining common usage patterns involving your I/O.
         <p><a href="/documentation/patterns/bigqueryio/">https://beam.apache.org/documentation/patterns/bigqueryio/</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Update <strong>I/O Connectors</strong> with your I/O’s information
         <p>Example:
         <p><a href="/documentation/io/connectors/#built-in-io-connectors">https://beam.apache.org/documentation/io/connectors/#built-in-io-connectors</a>
         <p><img src="/images/io-standards/io-supported-via-screenshot.png" width="" alt="alt_text" title="image_tooltip">
      </td>
   </tr>
   <tr id="before-you-start">
      <td>
         <p>Provide setup steps to use the I/O, under a <strong>Before you start</strong> header.
         <p>Example:
         <p><a href="/documentation/io/built-in/parquet/#before-you-start">https://beam.apache.org/documentation/io/built-in/parquet/#before-you-start</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Include a canonical read/write code snippet after the initial description for each supported language. The below example shows Hadoop with examples for Java.
         <p>Example:
         <p><a href="/documentation/io/built-in/hadoop/#reading-using-hadoopformatio">https://beam.apache.org/documentation/io/built-in/hadoop/#reading-using-hadoopformation</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Indicate how timestamps for elements are assigned. This includes batch sources to allow for future I/Os which may provide more useful information than <code>current_time()</code>.
         <p>Example:
      </td>
   </tr>
   <tr>
      <td>
         <p>Indicate how timestamps are advanced; for Batch sources this will be marked as n/a in most cases.
      </td>
   </tr>
   <tr>
      <td>
         <p>Outline any temporary resources (for example, files) that the connector will create.
         <p>Example:
         <p>BigQuery batch loads first create a temp GCS location
         <p><a href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.java#L455">https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.java#L455</a>
      </td>
  </tr>
  <tr id="authentication">
      <td>
         <p>Provide, under an <strong>Authentication</strong> subheader, how to acquire partner authorization material to securely access the source/sink.
         <p>Example:
         <p><a href="/documentation/io/built-in/snowflake/#authentication">https://beam.apache.org/documentation/io/built-in/snowflake/#authentication</a>
         <p>Here BigQuery names it permissions but the topic covers similarities
         <p><a href="https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html">https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html</a>
      </td>
  </tr>
  <tr>
      <td>
         <p>I/Os should provide links to the Source/Sink documentation within <strong>Before you start</strong> header.
         <p>Example:
         <p><a href="/documentation/io/built-in/snowflake/">https://beam.apache.org/documentation/io/built-in/snowflake/</a>
      </td>
  </tr>
  <tr>
      <td>
         <p>Indicate if there is native or X-language support in each language with a link to the docs.
         <p>Example:
         <p>Kinesis I/O has a native implementation of java and X-language support for python but no support for Golang.
      </td>
  </tr>
  <tr id="limitations">
   <td>
      <p>Indicate known limitations under a <strong>Limitations</strong> header. If the limitation has a tracking issue, please link it inline.
      <p>Example:
      <p><a href="/documentation/io/built-in/snowflake/#limitations">https://beam.apache.org/documentation/io/built-in/snowflake/#limitations</a>
   </td>
  </tr>
</table>
</div>



### I/O (not built-in)

Custom I/Os are not included in the Apache Beam Github repository. Some examples would be [Solace](https://github.com/SolaceProducts/solace-apache-beam)IO.

<div class="table-container-wrapper">
<table class="table table-bordered table-connectors">
   <tr>
      <td>
         <p>Update the Other I/O Connectors for Apache Beam table with your information.
         <p><a href="/documentation/io/connectors/#other-io-connectors-for-apache-beam">The aformentioned table</a>
      </td>
   </tr>
</table>
</div>
## Development

This section outlines API syntax, semantics and recommendations for features that should be adopted for new as well as existing Apache Beam I/O Connectors.

The I/O Connector development guidelines are written with the following principles in mind:



* Consistency makes an API easier to learn.
    * If there are multiple ways of doing something, we should strive to be consistent first
* With a couple minutes of studying documentation, users should be able to pick up most I/O connectors.
* The design of a new I/O should consider the possibility of evolution.
* Transforms should integrate well with other Beam utilities.


### All SDKs


#### Pipeline Configuration / Execution / Streaming / Windowing semantics guidelines

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Topic
      </th>
      <th>
         <p>Semantics
      </th>
   </tr>
   <tr>
      <td>
         <p>Pipeline Options
      </td>
      <td>
         <p>An I/O should rarely rely on a PipelineOptions subclass to tune internal parameters.
         <p>If neccesary, a connector-related pipeline options class should:
         <ul>
            <li>Document clearly, for each option, the effect it has and why one may modify it.
            <li>Option names must be namespaced to avoid collisions
            <li>Class Name: <code>{Connector}Options</code>
            <li>Method names: <code>set{Connector}{Option}</code>, <code>get{Connector}{Option}</code>
            </li>
      </td>
   </tr>
   <tr>
      <td>
         <p>Source Windowing
      </td>
      <td>
         <p>A source must return elements in the GlobalWindow unless explicitly parameterized in the API by the user.
         <p>Allowable Non-global-window patterns:
         <ul>
            <li>ReadFromIO(window_by=...)
            <li>ReadFromIO.IntoFixedWindows(...)
            <li>ReadFromIO(apply_windowing=True/False) (e.g. <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.periodicsequence.html#apache_beam.transforms.periodicsequence.PeriodicImpulse">PeriodicImpulse</a>)
            <li>IO.read().withWindowing(...)
            <li>IO.read().windowBy(...)
            <li>IO.read().withFixedWindows(...)
            </li>
         </ul>
      </td>
   </tr>
   <tr>
      <td>
         <p>Sink Windowing
      </td>
      <td>
         <p>A sink should be Window agnostic and handle elements sent with any Windowing method, unless explicitly parameterized or expressed in its API.
         <p>A sink may change windowing of a PCollection internally in any way. However, the metadata that it returns as part of its result object must be:
         <ul>
            <li>must be in the same window as the input, unless explicitly decared otherwise in the API.
            <li>must have accurate timestamps
            <li>may contain additional information about windowing (e.g. a BigQuery job may have a timestamp, but also a window associated with it).
         </ul>
         <p>Allowable non-global-window patterns:
         <ul>
            <li>WriteToIO(triggering_frequency=...) - e.g. <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html#apache_beam.io.gcp.bigquery.WriteToBigQuery">WriteToBigQuery</a> (This only sets the windowing within the transform - input data is still in the Global Window).
            <li>WriteBatchesToIO(...)
            <li>WriteWindowsToIO(...)
            </li>
         </ul>
      </td>
   </tr>
   <tr>
      <td>
         <p>Throttling
      </td>
      <td>
         <p>A streaming sink (or any transform accessing an external service) may implement throttling of its requests to prevent from overloading the external service.
         <p>TODO: Beam should expose throttling utilities (<a href="https://github.com/apache/beam/issues/24743">Tracking Issue</a>):
         <ul>
            <li>Per-key fixed throttling
            <li>Adaptive throttling with sink-reported backpressure
            <li>Ramp-up throttling from a start point
            </li>
         </ul>
      </td>
   </tr>
   <tr>
      <td>
         <p>Error handling
      </td>
      <td>
         <p>TODO: <a href="https://github.com/apache/beam/issues/24742">Tracking Issue</a>
      </td>
   </tr>
</table>
</div>



### Java


#### General

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p>The primary class used in working with the connector should be named <strong>{connector}IO</strong>
         <p>Example:
         <p>The BigQuery I/O is <strong>org.apache.beam.sdk.io.bigquery.BigQueryIO</strong>
      </td>
   </tr>
   <tr>
      <td>
         <p>The class should be placed in the package <strong>org.apache.beam.sdk.io.{connector}</strong>
         <p>Example:
         <p>The BigQueryIO belongs in the java package <a href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.java">org.apache.beam.sdk.io.bigquery</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>The unit/integration/performance tests should live under the package <strong>org.apache.beam.sdk.io.{connector}.testing</strong>. This will cause the various tests to work with the standard user-facing interfaces of the connector.
         <p>Unit tests should reside in the same package (i.e. <strong>org.apache.beam.sdk.io.{connector}</strong>), as they may often test internals of the connector.
         <p>The BigQueryIO belongs in the java package <a href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.java">org.apache.beam.sdk.io.bigquery</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>An I/O transform should avoid receiving user lambdas to map elements from a user type to a connector-specific type. Instead, they should interface with a connector-specific data type (with schema information when possible).
         <p>When necessary, an I/O transform should receive a type parameter that specifies the input type (for sinks) or output type (for sources) of the transform.
         <p>An I/O transform may not have a type parameter <strong>only if it is certain that its output type will not change</strong> (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/FileIO.MatchAll.html">FileIO.MatchAll</a> and other <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/FileIO.html">FileIO transforms</a>).
      </td>
   </tr>
   <tr>
      <td>
         <p>It is highly discouraged to directly expose third-party libraries in the public API part of the I/O Connector for the following reasons:
         <ul>
            <li>It reduces Apache Beam’s compatibility guarantees - Changes to third-party libraries can/will directly break existing user’s pipelines.
            <li>It makes code maintainability hard - If libraries are directly exposed at API level, a dependency change will require multiple changes throughout the I/O implementation code
            <li>It forces third-party dependencies onto end users
            </li>
         </ul>
         <p>Instead, we highly recommend exposing Beam-native interfaces and an adaptor that holds mapping logic.
         <p>If you believe that the library in question is extremely static in nature. Please note it in the I/O itself.
      </td>
   </tr>
   <tr>
      <td>
         <p>Source and Sinks should be abstracted with a PTransform wrapper, and internal classes be declared protected or private. By doing so implementation details can be added/changed/modified without breaking implementation by dependencies.
      </td>
   </tr>
</table>
</div>


#### Classes / Methods / Properties

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Java Syntax
      </th>
      <th>
         <p>Semantics
      </th>
   </tr>
   <tr>
      <td>
         <p>class IO.Read
      </td>
      <td>
         <p>Gives access to the class that represents reads within the I/O. The <code>Read</code> class should implement a  fluent interface similar to the fluentbuilder pattern (e.g. <code>withX(...).withY(...)</code>). Together with default values, it provide a fail-fast (with immediate validation feedback after each <code>.withX()</code>) that is slightly less verbose than the builder pattern.
         <p>A user should <strong>not</strong> create this class directly. It should be created by a <a href="#static-method-read">top-level utility method</a>.
      </td>
   </tr>
   <tr>
      <td>
         <p>class IO.ReadAll
      </td>
      <td>
         <p>A few different sources implement runtime configuration for reading from a data source. This is a valuable pattern because it enables a purely batch source to become a more sophisticated streaming source.
         <p>As much as possible, this type of transform should have the type richness of a construction-time-configured transform:
         <ul>
            <li>Support Beam Row output with a schema known at construction-time.
            <li>Extra configuration may be needed (and acceptable) in this case (e.g. a SchemaProvider parameter, a Schema parameter, a Schema Catalog or a utility of that sort).
            <li>The input PCollection should have a fixed type with a schema, so it can be easily manipulated by users.
            </li>
         </ul>
         <p>Example:
         <p><a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.ReadAll.html">JdbcIO.ReadAll</a>, <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/parquet/ParquetIO.ReadFiles.html">ParquetIO.ReadFiles</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>class IO.Write
      </td>
      <td>
         <p>Gives access to the class that represents writes within the I/O. The Write class should implement a fluent interface pattern (e.g. <code>withX(...).withY(...)</code>) as described further above for <code>IO.Read</code>.
         <p>A user should not create this class directly. It should be created by a <a href="#static-method-write">top-level utility method</a>.
      </td>
   </tr>
   <tr>
      <td>
         <p>Other Transform Classes
      </td>
      <td>
         <p>Some data storage and external systems implement APIs that do not adjust easily to Read or Write semantics (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/healthcare/FhirIO.html">FhirIO implements several different transforms</a> that fetch or send data to Fhir).
         <p>These classes should be added <strong>only if it is impossible or prohibitively difficult to encapsulate their functionality as part of extra configuration of Read, Write and ReadAll</strong> transforms, to avoid increasing the cognitive load on users.
         <p>A user should not create these classes directly. They should be created by a <a href="#static-method-read">top-level static method</a>.
      </td>
   </tr>
   <tr>
      <td>
         <p>Utility Classes
      </td>
      <td>
         <p>Some connectors rely on other user-facing classes to set configuration parameters.
         <p>(e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.DataSourceConfiguration.html">JdbcIO.DataSourceConfiguration</a>). These classes should be <strong>nested within the {Connector}IO class</strong>.
         <p>This format makes them visible in the main Javadoc, and easy to discover by users.
      </td>
   </tr>
   <tr id="static-method-write">
      <td>
         <p>Method IO&lt;T&gt;.write()
      </td>
      <td>
         <p>The top-level I/O class will provide a <strong>static method</strong> to start constructing an I/O.Write transform. This returns a PTransform with a single input PCollection, and a Write.Result output.
         <p>This method should not specify in its name any of the following:
         <ul>
            <li>Internal data format
            <li>Strategy used to write data
            <li>Input or output data type
            </li>
         </ul>
      <p>The above should be specified via configuration parameters if possible. <strong>If impossible</strong>, then <strong>a new static method</strong> may be introduced, but this <strong>must be exceptional</strong>.
      </td>
   </tr>
   <tr id="static-method-read">
      <td>
         <p>Method IO&lt;T&gt;.read()
      </td>
      <td>
         <p>The method to start constructing an I/O.Read transform. This returns a PTransform with a single output PCollection.
         <p>This method should not specify in its name any of the following:
         <ul>
            <li>Internal data format
            <li>Strategy used to read data
            <li>Output data type
            </li>
         </ul>
      <p>The above should be specified via configuration parameters if possible. <strong>If not possible</strong>, then <strong>a new static method</strong> may be introduced, but this <strong>must be exceptional, and documented in the I/O header as part of the API</strong>.
      <p>The initial static constructor method may receive parameters if these are few and general, or if they are necessary to configure the transform (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/healthcare/FhirIO.html#exportResourcesToGcs-java.lang.String-java.lang.String-">FhirIO.exportResourcesToGcs</a>, <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.html#readWithPartitions-org.apache.beam.sdk.values.TypeDescriptor-">JdbcIO.ReadWithPartitions</a> needs a TypeDescriptor for initial configuration).
      </td>
   </tr>
   <tr id="read-from-source">
      <td>
         <p>IO.Read.from(source)
      </td>
      <td>
         <p>A Read transform must provide a <strong>from</strong> method where users can specify where to read from. If a transform can read from different <em>kinds</em> of sources (e.g. tables, queries, topics, partitions), then multiple implementations of this from method can be provided to accommodate this:
         <ul>
            <li>IO.Read from(Query query)
            <li>IO.Read from(Table table) / from(String table)
            <li>IO.Read from (Topic topic)
            <li>IO.Read from(Partition partition)
            </li>
         </ul>
         <p>The input type for these methods can reflect the external source’s API (e.g. <a href="https://kafka.apache.org/27/javadoc/?org/apache/kafka/common/TopicPartition.html">Kafka TopicPartition</a> should use a <strong>Beam-implemented</strong> TopicPartition object).
         <p>Sometimes, there may be multiple <strong>from</strong> locations that use the same input type, which means we cannot leverage method overloading. With this in mind, use a new method to enable this situation.
         <ul>
            <li>IO.Read from(String table)
            <li>IO.Read fromQuery(String query)
            </li>
         </ul>
      </td>
   </tr>
   <tr>
      <td>
         <p>IO.Read.fromABC(String abc)
      </td>
      <td>
         <p><string>This pattern is discouraged if method overloading is possible</strong>, follow guidance in <strong><a href="#read-from-source">Read.from(source)</a></strong>.
      </td>
   </tr>
   <tr id="write-to-destination">
      <td>
         <p>IO.Write.to(destination)
      </td>
      <td>
         <p>A Write transform must provide a <strong>to</strong> method where users can specify where to write data. If a transform can write to different <em>kinds</em> of sources while still using the same input element type(e.g. tables, queries, topics, partitions), then multiple implementations of this from method can be provided to accommodate this:
         <ul>
            <li>IO.Write to(Query query)
            <li>IO.Write to(Table table) / from(String table)
            <li>IO.Write to(Topic topic)
            <li>IO.Write to(Partition partition)
            </li>
         </ul>
         <p>The input type for these methods can reflect the external sink's API (e.g. <a href="https://kafka.apache.org/27/javadoc/?org/apache/kafka/common/TopicPartition.html">Kafka TopicPartition</a> should use a <strong>Beam-implemented</strong> TopicPartition object).
         <p>If different kinds of destinations require different types of input object types, then these should be done in separate I/O connectors.
         <p>Sometimes, there may be multiple <strong>from</strong> locations that use the same input type, which means we cannot leverage method overloading. With this in mind, use a new method to enable this situation.
         <ul>
            <li>IO.Write to(String table)
            <li>IO.Write toTable(String table)
            </li>
         </ul>
      </td>
   </tr>
   <tr>
      <td>
         <p>IO.Write.to(DynamicDestination destination)
      </td>
      <td>
         <p>A write transform may enable writing to more than one destination. This can be a complicated pattern that should be implemented carefully (it is the preferred pattern for connectors that will likely have multiple destinations in a single pipeline).
         <p>The preferred pattern for this is to define a DynamicDestinations interface (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/DynamicDestinations.html">BigQueryIO.DynamicDestinations</a>) that will allow the user to define all necessary parameters for the configuration of the destination.
         <p>The DynamicDestinations interface also allows maintainers to add new methods over time (with <strong>default implementations</strong> to avoid breaking existing users) that will define extra configuration parameters if necessary.
      </td>
   </tr>
   <tr>
      <td>
         <p>IO.Write.toABC(destination)
      </td>
      <td>
         <p><string>This pattern is discouraged if method overloading is possible</strong>, follow guidance in <strong><a href="#write-to-destination">Write.to(destination)</a></strong>.
      </td>
   </tr>
   <tr>
      <td>
         <p>class IO.Read.withX
         <p>IO.Write.withX
      </td>
      <td>
         <p>withX provides a method for configuration to be passed to the Read method, where X represents the configuration to be created. With the exception of generic with statements ( defined below ) the I/O should attempt to match the name of the configuration option with that of the option name in the source.
         <p>These methods should return a new instance of the I/O rather than modifying the existing instance.
         <p>Example:
         <p><a href="https://beam.apache.org/releases/javadoc/2.35.0/org/apache/beam/sdk/io/TextIO.Read.html#withCompression-org.apache.beam.sdk.io.Compression-">TextIO.Read.withCompression</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>IO.Read.withConfigObject
         <p>IO.Write.withConfigObject
      </td>
      <td>
         <p>Some connectors in Java receive a configuration object as part of their configuration. <strong>This pattern is encouraged only for particular cases</strong>. In most cases, a connector can hold all necessary configuration parameters at the top level.
         <p>To determine whether a multi-parameter configuration object is an appropriate parameter for a high level transform, the configuration object must:
         <ul>
            <li>Hold only properties related to the connection/authentication parameters for the external data store (e.g. JdbcIO.DataSourceConfiguration).
            <ul>
               <li>Generally, <strong>secrets should not be passed as parameters</strong>, unless an alternative is not feasible. For secret management, a secret-management service or KMS is the recommended approach.
            </ul>
            <li><strong>Or </strong>mirror an API characteristic from the external data source (e.g. KafkaIO.Read.withConsumerConfigUpdates), without exposing that external API in the Beam API.
            <ul>
               <li>The method should mirror the name of the API object (e.g. given an object SubscriptionStatConfig, a method would be withSubscriptionStatConfig).
            </ul>
            <li><strong>Or</strong> when a connector can support different configuration ‘paths’ where a particular property requires other properties to be specified (e.g. BigQueryIO’s method will entail various different properties). (see last examples).
            </li>
         </ul>
         <p>Example:
         <p><a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.DataSourceConfiguration.html">JdbcIO.DataSourceConfiguration</a>, <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/spanner/SpannerConfig.html">SpannerConfig</a>, <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.ReadSourceDescriptors.html#withConsumerConfigUpdates-java.util.Map-">KafkaIO.Read.withConsumerConfigUpdates</a>
         <p>
{{< highlight >}}
BigQueryIO.write()
  .withWriteConfig(FileLoadsConfig.withAvro()
                                 .withTriggeringFrequency()...)

BigQueryIO.write()
  .withWriteConfig(StreamingInsertsConfig.withDetailedError()
                                  .withExactlyOnce().etc..)
{{< /highlight >}}
         </p>
      </td>
   </tr>
   <tr>
      <td>
         <p>class IO.Write.withFormatFunction
      </td>
      <td>
         <p><strong>Discouraged - except for dynamic destinations</strong>
         <p>For sources that can receive Beam Row-typed PCollections, the format function should not be necessary, because Beam should be able to format the input data based on its schema.
         <p>For sinks providing Dynamic Destination functionality, elements may carry data that helps determine their destination. These data may need to be removed before writing to their final destination.
         <p>To include this method, a connector should:
         <ul>
            <li>Show that it’s not possible to perform data matching automatically.
            <li>Support Dynamic Destinations and need changes to the input data due to that reason.
            </li>
         </ul>
      </td>
   </tr>
   <tr>
      <td>
         <p>IO.Read.withCoder
         <p>IO.Write.withCoder
      </td>
      <td>
         <p><strong>Strongly Discouraged</strong>
         <p>Sets the coder to use to encode/decode the element type of the output / input PCollection of this connector. In general, it is recommended that sources will:
         <ol>
            <li>Return Row objects with a schema that is automatically inferred.
            <li>Automatically set the necessary coder by having fixed output/input types, or inferring their output/input types.
            </li>
         </ol>
         <p>If nether #1 and #2 are possible, then a <code>withCoder(...)</code> method can be added.
      </td>
   </tr>
   <tr>
      <td>
         <p>IO.ABC.withEndpoint / with{IO}Client / withClient
      </td>
      <td>
         <p>Connector transforms should provide a method to override the interface between themselves and the external system that they communicate with. This can enable various uses:
         <p>Sets the coder to use to encode/decode the element type of the output / input PCollection of this connector. In general, it is recommended that sources will:
         <ul>
            <li>Local testing by mocking the destination service
            <li>User-enabled metrics, monitoring, and security handling in the client.
            <li>Integration testing based on emulators
            </li>
         </ul>
         <p>Example:
         <p><a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.Write.html#withTestServices-org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices-">BigQueryIO.Write.withTestServices</a>(<a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryServices.html">BigQueryServices</a>)
      </td>
   </tr>
</table>
</div>

#### Types

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Java Syntax
      </th>
      <th>
         <p>Semantics
      </th>
   </tr>
   <tr>
      <td>
         <p>Method IO.Read.expand
      </td>
      <td>
         <p>The expand method of a Read transform must return a PCollection object with a type. The type may be parameterized or fixed to a class.
         <p>A user should <strong>not</strong> create this class directly. It should be created by a <a href="#static-method-read">top-level utility method</a>.
      </td>
   </tr>
   <tr>
      <td>
         <p>Method IO.Read.expand’s PCollection type
      </td>
      <td>
         <p>The type of the PCollection will usually be one of the following four options. For each of these option, the encoding / data is recommended to be as follows:
         <ul>
            <li>A pre-defined, basic Java type (e.g. String)
            <ul>
               <li>This encoding should be simple, and use a simple Beam coder (e.g. Utf8StringCoder)
               </li>
            </ul>
            <li>A pre-set POJO type (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/fs/MatchResult.Metadata.html">Metadata</a>) with a schema
            <ul>
               <li>The preferred strategy for these is to define the output type as an <a href="https://stackoverflow.com/questions/62546191/how-do-i-use-an-autovalue-data-type-for-my-pcollection-in-apache-beam">@AutoValue, with @DefaultSchema and @SchemaCreate</a> annotations. This will ensure compact, fast encoding with RowCoder.
               </li>
            </ul>
            <li>A Beam Row with a specific schema
            <li>A type with a schema that’s not known at construction time
            </li>
         </ul>
         <p>In all cases, asking a user to pass a coder (e.g. <code>withCoder(...)</code>) is <strong>discouraged</strong>.
      </td>
   </tr>
   <tr>
      <td>
         <p>method IO.Write.expand
      </td>
      <td>
         <p>The expand method of any write transform must return a type IO.Write.Result object that extends a PCollectionTuple. This object allows transforms to return metadata about the results of its writing and allows this write to be followed by other PTransforms.
         <p>If the Write transform would not need to return any metadata, a Write.Result object <strong>is still preferable</strong>, because it will allow the transform to evolve its metadata over time.
         <p>Examples of metadata:
         <ul>
            <li>Failed elements and errors
            <li>Successfully written elements
            <li>API tokens from calls issued by the transform
            </li>
         </ul>
         <p>Examples:
         <p>BigQueryIO’s <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/WriteResult.html">WriteResult</a>
      </td>
   </tr>
</table>
</div>

#### Evolution

Over time, I/O need to evolve to address new use cases, or use new APIs under the covers. Some examples of necessary evolution of an I/O:

* A new data type needs to be supported within it (e.g. [any-type partitioning in JdbcIO.ReadWithPartitions](https://github.com/apache/beam/pull/15848))
* A new backend API needs to be supported

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Java Syntax
      </th>
      <th>
         <p>semantics
      </th>
   </tr>
   <tr>
      <td>
         <p>Top-level static methods
      </td>
      <td>
         <p>In general, one should resist adding a completely new static method for functionality that can be captured as configuration within an existing method.
         <p>An example of too many top-level methods that could be supported via configuration is <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.html">PubsubIO</a>
         <p>A new top-level static method should only be added in the following cases:
         <ul>
            <li>A new PTransform is being added that cannot / does not make sense to be supported as a simple configuration parameter of the existing PTransforms (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/healthcare/FhirIO.html">FhirIO’s various transforms</a>, any ReadAll transform).
            <li>A feature that represents a new recommended standard to use the transform (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.html">JdbcIO’s ReadWithPartitions</a>)
            <li>A change in the usual way of interacting with this transform, or a parameter that’s necessary to initialize the transform (e.g. <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html">BigQueryIO.read(...) method vs read()</a>)
            </li>
         </ul>
      </td>
   </tr>
</table>
</div>

### Python


#### General

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p>If the I/O lives in Apache Beam it should be placed in the package <strong>apache_beam.io.{connector}</strong> or <strong>apache_beam.io.{namespace}.{connector}</strong>
         <p>Example:
         <p>apache_beam.io.fileio and apache_beam.io.gcp.bigquery
      </td>
   </tr>
   <tr>
      <td>
         <p>There will be a module named {connector}.py which is the primary entry point used in working with the connector in a pipeline <strong>apache_beam.io.{connector}</strong> or <strong>apache_beam.io.{namespace}.{connector}</strong>
         <p>Example:
         <p>apache_beam.io.gcp.bigquery / apache_beam/io/gcp/bigquery.py
         <p>Another possible layout: apache_beam/io/gcp/bigquery/bigquery.py (automatically import public classes in bigquery/__init__.py)
      </td>
   </tr>
   <tr>
      <td>
         <p>The connector must define an <code>__all__</code> attribute in its main file, and export only classes and methods meant to be accessed by users.
      </td>
   </tr>
   <tr>
      <td>
         <p>If the I/O implementation exists in a single module (a single file), then the file {connector}.py can hold it.
         <p>Otherwise, the connector code should be defined within a directory (connector package) with an __init__.py file that documents the public API.
         <p>If the connector defines other files containing utilities for its implementation, these files must clearly document the fact that they are not meant to be a public interface.
      </td>
  </tr>
</table>
</div>


#### Classes / Methods / Properties

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Python Syntax
      </th>
      <th>
         <p>semantics
      </th>
   </tr>
   <tr>
      <td>
         <p>callable ReadFrom{Connector}
      </td>
      <td>
         <p>This gives access to the PTransform to read from a given data source. It allows you to configure it via the arguments that it receives. For long lists of optional parameters, they may be defined as parameters with a default value.
         <p>Q. Java uses a builder pattern. Why can’t we do that in Python? <a href="https://stackoverflow.com/a/11977454/1255356">Optional parameters can serve the same role</a> in Python.
         <p>Example:
         <p><a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html">apache_beam.io.gcp.bigquery.ReadFromBigQuery</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>callable ReadAllFrom{Connector}
      </td>
      <td>A few different sources implement runtime configuration for reading from a data source. This is a valuable pattern because it enables a purely batch source to become a more sophisticated streaming source.
         <p>As much as possible, this type of transform should have the type richness and safety of a construction-time-configured transform:
         <ul>
            <li>Support output with a schema known at construction-time
            <ul>
               <li>Extra configuration may be needed (and acceptable) in this case (e.g. a SchemaProvider parameter, a Schema parameter, a Schema Catalog or a utility of that sort).
               </li>
            </ul>
            <li>The input PCollection should have a fixed type with a schema, so it can be easily manipulated by users.
            </li>
         </ul>
         <p>Example:
         <p><a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html#readallfrombigquery">ReadAllFromBigQuery</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>callable WriteTo{Connector}
      </td>
      <td>
         <p>This gives access to the PTransform to write into a given data sink. It allows you to configure it via the arguments that it receives. For long lists of optional parameters, they may be defined as parameters with a default value.
         <p>Q. Java uses a builder pattern. Why can’t we do that in Python? <a href="https://stackoverflow.com/a/11977454/1255356">Optional parameters can serve the same </a>
         <p><a href="https://stackoverflow.com/a/11977454/1255356">role</a> in Python.
         <p>Example:
         <p><a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html">apache_beam.io.gcp.bigquery.WriteToBigQuery</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>callables Read/Write
      </td>
      <td>
         <p>A top-level transform initializer (ReadFromIO/ReadAllFromIO/WriteToIO) must aim to require the fewest possible parameters, to simplify its usage, and allow users to use them quickly.
      </td>
   </tr>
   <tr>
      <td>
         <p>parameter ReadFrom{Connector}({source})
         <p>parameter WriteTo{Connector}({sink})
      </td>
      <td>
         <p>The first parameter in a Read or Write I/O connector must specify the source for readers or the destination for writers.
         <p>If a transform can read from different <em>kinds</em> of sources (e.g. tables, queries, topics, partitions), then the suggested approaches by order of preference are:
         <ol>
            <li>Retain a single argument, but auto-infer the source/sink type (e.g. <a href="https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html#pandas-read-sql"><code>pandas.read_sql(...)</code></a> supporting tables and queries)
            <li>Add a new argument for each possible source/sink type (e.g. ReadFromBigQuery having query/table parameters)
            </li>
         </ol>
      </td>
   </tr>
   <tr>
      <td>
         <p>parameter WriteToIO(destination={multiple_destinations})
      </td>
      <td>
         <P>A write transform may enable writing to more than one destination. This can be a complicated pattern that should be implemented carefully (it is the preferred pattern for connectors that will likely have multiple destinations in a single pipeline).
         <p>The preferred API pattern in Python is to pass callables (e.g. <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html#apache_beam.io.gcp.bigquery.WriteToBigQuery">WriteToBigQuery</a>) for all parameters that will need to be configured. In general, examples of callable parameters may be:
         <ul>
            <li>Destination callable → Should receive an element, and return a destination for that element
            <li>Other examples
            <ul>
               <li>Schema callable → Should receive a destination and return a schema for the destination
               <li>Format function → Should receive a record (and maybe a destination) and format the record to be inserted.
               </li>
            </ul>
            </li>
         </ul>
         <p>Using these callables also allows maintainers to add new parameterizable callables over time (with <strong>default values</strong> to avoid breaking existing users) that will define extra configuration parameters if necessary.
         <p><strong>Corner case</strong>: It is often necessary to pass side inputs to some of these callables. The recommended pattern is to have an extra parameter in the constructor to include these side inputs (e.g. <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html#apache_beam.io.gcp.bigquery.WriteToBigQuery">WriteToBigQuery’s table_side_inputs parameter</a>)
      </td>
   </tr>
   <tr>
      <td>
         <p>parameter ReadFromIO(param={param_val})
         <p>parameter WriteToIO(param={param_val})
      </td>
      <td>
         <p>Any additional configuration can be added as optional parameters in the constructor of the I/O. Whenever possible, mandatory extra parameters should be avoided. Optional parameters should have reasonable default values, so that picking up a new connector will be as easy as possible.
      </td>
   </tr>
   <tr>
      <td>
         <p>parameter ReadFromIO(config={config_object})
      </td>
      <td>
         <p><strong>Discouraged</strong>
         <p>Some connectors in Python may receive a complex configuration object as part of their configuration. <strong>This pattern is discouraged</strong>, because a connector can hold all necessary configuration parameters at the top level.
         <p>To determine whether a multi-parameter configuration object is an appropriate parameter for a high level transform, the configuration object must:
         <ul>
            <li>Hold only properties related to the connection/authentication parameters for the external data store (e.g. <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.kafka.html#apache_beam.io.kafka.ReadFromKafkaSchema">ReadFromKafka(consumer_config parameter)</a>)
            <li><strong>Or </strong>mirror an API characteristic from the external data source (e.g. <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.debezium.html">ReadFromDebezium(connector_properties parameter)</a>)
            </li>
         </ul>
      </td>
   </tr>
</table>
</div>



#### Types

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Python Syntax
      </th>
      <th>
         <p>Semantics
      </th>
   </tr>
   <tr>
      <td>
         <p>Output of method ReadFromIO.expand
      </td>
      <td>
         <p>The expand method of a Read transform must return a PCollection object with a type, and be annotated with the type. Preferred PCollection types in Python are (in order of preference):
         <p>Simple Python types if the  (bytes, str, numbers)
         <p>For complex types:
         <ol>
            <li>A NamedTuple or DataClass with a set schema, encoded with RowCoder
            <li>A Python dictionary
            <ol>
               <li>The dictionaries should be encoded via RowCoder, if possible.
               </li>
            </ol>
            <li>A preset Python class, if a schema is not possible
            </li>
         </ol>
      </td>
   </tr>
   <tr>
      <td>
         <p>Output of method WriteToIO.expand
      </td>
      <td>
         <p>The expand method of any write transform must return a Python object with a <strong>fixed class type</strong>. The recommended name for the class is <strong>WriteTo{IO}Result</strong>. This object allows transforms to return metadata about the results of its writing.
         <p>If the Write transform would not need to return any metadata, a Python object with a class type <strong>is still preferable</strong>, because it will allow the transform to evolve its metadata over time.
         <p>Examples of metadata:
         <ul>
            <li>Failed elements and errors
            <li>Successfully written elements
            <li>API tokens from calls issued by the transform
            </li>
         </ul>
         <p>Example:
         <p>BigQueryIO’s <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/WriteResult.html">WriteResult</a>
         <p>Motivating example (a bad pattern): WriteToBigQuery’s inconsistent dictionary results[<a href="https://github.com/apache/beam/blob/v2.34.0/sdks/python/apache_beam/io/gcp/bigquery.py#L2138">1</a>][<a href="https://github.com/apache/beam/blob/b3b184361100492f92fcc51ff82a0fcd962d5ee0/sdks/python/apache_beam/io/gcp/bigquery_file_loads.py#L1203-L1207">2</a>]
      </td>
   </tr>
   <tr>
      <td>
         <p>Input of method WriteToIO.expand
      </td>
      <td>
         <p>The expand method of a Write transform must return a PCollection object with a type, and be annotated with the type. Preferred PCollection types in Python are the same as the output types for a ReadFromIO referenced in T1.
      </td>
   </tr>
</table>
</div>

### GoLang


#### General

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p>If the I/O lives in Apache Beam it should be placed in the package:
         <p><strong>{connector}io</strong>
         <p>Example:
         <p><a href="https://github.com/apache/beam/blob/master/sdks/go/pkg/beam/io/avroio/avroio.go">avroio</a> and <a href="https://github.com/apache/beam/blob/master/sdks/go/pkg/beam/io/bigqueryio/bigquery.go">bigqueryio</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Integration and Performance tests should live under the same package as the I/O itself
         <p><strong>{connector}io</strong>
      </td>
   </tr>
</table>
</div>

### Typescript


#### Classes / Methods / Properties

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Typescript Syntax
      </th>
      <th>
         <p>Semantics
      </th>
   </tr>
   <tr>
      <td>
         <p>function readFromXXX
      </td>
      <td>
         <p>The method to start constructing an I/O.Read transform.
      </td>
   </tr>
   <tr>
      <td>
         <p>function writeToXXX
      </td>
      <td>
         <p>The method to start constructing an I/O.Write transform.
      </td>
   </tr>
</table>
</div>

## Testing

An I/O should have unit tests, integration tests, and performance tests. In the following guidance we explain what each type of test aims to achieve, and provide a _baseline_ standard of test coverage. Do note that the actual test cases and business logic of the actual test would vary depending on specifics of each source/sink but we have included some suggested test cases as a baseline.

This guide complements the [Apache Beam I/O transform testing guide](https://beam.apache.org/documentation/io/testing/) by adding specific test cases and scenarios. For general information regarding testing Beam I/O connectors, please refer to that guide.

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p>Integration and performance tests should live under the package org.apache.beam.sdk.io.{connector}.testing. This will cause the various tests to work with the standard user-facing interfaces of the connector.
         <p>Unit tests should reside in the same package (i.e. org.apache.beam.sdk.io.{connector}), as they may often test internals of the connector.
      </td>
   </tr>
</table>
</div>

### Unit Tests

I/O unit tests need to efficiently test the functionality of the code. Given that unit tests are expected to be executed many times over multiple test suites (for example, for each Python version) these tests should execute relatively fast and should not have side effects. We recommend trying to achieve 100% code coverage through unit tests.

When possible, unit tests are favored over integration tests due to faster execution time and low resource usage. Additionally, unit tests can be easily included in pre-commit tests suites (for example, Jenkins <strong>beam_PreCommit_*</strong> test suites) hence has a better chance of discovering regressions early. Unit tests are also preferred for error conditions.

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p>The unit testing class should be part of the same package as the IO and named {connector}IOTest.
         <p>Example:
         <p><a href=https://github.com/apache/beam/blob/v2.43.0/sdks/java/io/cassandra/src/test/java/org/apache/beam/sdk/io/cassandra/CassandraIOTest.java>sdks/java/io/cassandra/src/test/java/org/apache/beam/sdk/io/cassandra/CassandraIOTest.java</a>
      </td>
   </tr>
</table>
</div>

#### Suggested Test Cases

<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Functionality to test
      </th>
      <th>
         <p>Description
      </th>
      <th>
         <p>Example(s)
      </th>
   </tr>
   <tr>
      <td>
         <p>Reading with default options
      </td>
      <td>
         <p>Preferably runs a pipeline locally using DirectRunner and a fake of the data store. But can be a unit test of the source transform using mocks.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/f9ae6d53e2e6ad8346cee955d646f7198dbb6502/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java#L473">BigtableIOTest.testReading</a>
         <p><a href="https://github.com/apache/beam/blob/cb28a5b0265a04d60ad005684d0fbb4db74128f2/sdks/python/apache_beam/io/gcp/pubsub_test.py#L477">pubsub_test.TestReadFromPubSub.test_read_messages_success</a>
         <p><a href="https://github.com/apache/beam/blob/689e70b5131620540faf52e2f1e2dca7a36f269d/sdks/java/io/cassandra/src/test/java/org/apache/beam/sdk/io/cassandra/CassandraIOTest.java#L320">CassandraIOTest.testRead</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Writing with default options
      </td>
      <td>
         <p>Preferably runs a pipeline locally using DirectRunner and a fake of the data store. But can be a unit test of the sink transform using mocks.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/f9ae6d53e2e6ad8346cee955d646f7198dbb6502/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java#L1184">BigtableIOTest.testWriting</a>
         <p><a href="https://github.com/apache/beam/blob/cb28a5b0265a04d60ad005684d0fbb4db74128f2/sdks/python/apache_beam/io/gcp/pubsub_test.py#L810">pubsub_test.TestWriteToPubSub.test_write_messages_success</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Reading with additional options
      </td>
      <td>
         <p>For every option available to users.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/f9ae6d53e2e6ad8346cee955d646f7198dbb6502/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java#L620">BigtableIOTest.testReadingWithFilter</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Writing with additional options
      </td>
      <td>
         <p>For every option available to users. For example, writing to dynamic destinations.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/5b3f70bec72b6b646fe97d4eb7f8bd715dd562a8/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java#L1410">BigTableIOTest.testReadWithBigTableOptionsSetsRetryOptions</a>
         <p><a href="https://github.com/apache/beam/blob/cd05896ebc385d12f7a7801f3bbba0127bef8b3b/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java#L270">BigQueryIOWriteTest.testWriteDynamicDestinations</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Reading additional element types
      </td>
      <td>
         <p>If the data store read schema supports different data types.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/5b3f70bec72b6b646fe97d4eb7f8bd715dd562a8/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOReadTest.java#L434">BigQueryIOReadTest.testReadTableWithSchema</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Writing additional element types
      </td>
      <td>
         <p>If the data store write schema supports different data types.
      </td>
      <td>
      </td>
   </tr>
   <tr>
      <td>
         <p>Display data
      </td>
      <td>
         <p>Tests that the source/sink populates display data correctly.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/8bda63bc8ea0c1de9ec29d0da080df1769c65a2b/sdks/java/extensions/avro/src/test/java/org/apache/beam/sdk/extensions/avro/io/AvroIOTest.java#L174">AvroIOTest.testReadDisplayData</a>
         <p><a href="https://github.com/apache/beam/blob/f9ae6d53e2e6ad8346cee955d646f7198dbb6502/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/datastore/DatastoreV1Test.java#L220">DatastoreV1Test.testReadDisplayData</a>
         <p><a href="https://github.com/apache/beam/blob/4012a46d3aa7b2a4c628f1352c8b579733c71b41/sdks/python/apache_beam/io/gcp/bigquery_test.py#L187">bigquery_test.TestBigQuerySourcetest_table_reference_display_data</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Initial splitting
      </td>
      <td>
         <p>There can be many variations of these tests. Please refer to examples for details.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/05f9e775b8970602760aa56644285741c70190d0/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOReadTest.java#L659">BigqueryIOReadTest.estBigQueryQuerySourceInitSplit</a>
         <p><a href="https://github.com/apache/beam/blob/cb28a5b0265a04d60ad005684d0fbb4db74128f2/sdks/python/apache_beam/io/avroio_test.py#L241">avroio_test.AvroBase.test_read_with_splitting</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Dynamic work rebalancing
      </td>
      <td>
         <p>There can be many variations of these tests. Please refer to examples for details.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/09bbb48187301f18bec6d9110741c69b955e2b5a/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java#L670">BigTableIOTest.testReadingSplitAtFractionExhaustive</a>
         <p><a href="https://github.com/apache/beam/blob/cb28a5b0265a04d60ad005684d0fbb4db74128f2/sdks/python/apache_beam/io/avroio_test.py#L309">avroio_test.AvroBase.test_dynamic_work_rebalancing_exhaustive</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Schema support
      </td>
      <td>
         <p>Reading a PCollection&lt;Row> or writing a PCollection&lt;Row>
         <p>Should verify retrieving schema from a source, and pushing/verifying the schema for a sink.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/6ba27f0b86e464d912a16b17b554fe68db03bc69/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOReadTest.java#L434">BigQueryIOReadTest.testReadTableWithSchema</a>
         <p><a href="https://github.com/apache/beam/blob/6ba27f0b86e464d912a16b17b554fe68db03bc69/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java#L1129">BigQueryIOWriteTest.testSchemaWriteLoads</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Validation test
      </td>
      <td>
         <p>Tests that source/sink transform is validated correctly, i.e. incorrect/incompatible configurations are rejected with actionable errors.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/cd05896ebc385d12f7a7801f3bbba0127bef8b3b/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java#L1560">BigQueryIOWriteTest.testWriteValidatesDataset</a>
         <p><a href="https://github.com/apache/beam/blob/d99ebb69f7adc0afe1ffc607d1bbdda80eb2e08a/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/pubsub/PubsubIOTest.java#L107">PubsubIOTest.testTopicValidationSuccess</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Metrics
      </td>
      <td>
         <p>Confirm that various read/write metrics get set
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/c57c983c8ae7d84926f9cf42f7c40af8eaf60545/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIOReadTest.java#L333">SpannerIOReadTest.testReadMetrics</a>
         <p><a href="https://github.com/apache/beam/blob/25e6008e8919c2f31eaebae2662b44e02f9f37a1/sdks/python/apache_beam/io/gcp/bigtableio_test.py#L59">bigtableio_test.TestWriteBigTable.test_write_metrics</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Read All
      </td>
      <td>
         <p>Test read all (PCollection&lt;Read Config>) version of the test works
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/09bbb48187301f18bec6d9110741c69b955e2b5a/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIOReadTest.java#L503">SpannerIOReadTest.readAllPipeline</a>
         <p><a href="https://github.com/apache/beam/blob/689e70b5131620540faf52e2f1e2dca7a36f269d/sdks/java/io/cassandra/src/test/java/org/apache/beam/sdk/io/cassandra/CassandraIOTest.java#L381">CassandraIOTest.readAllQuery</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Sink batching test
      </td>
      <td>
         <p>Make sure that sinks batch data before writing if the sinks performace batching for performance reasons.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/c57c983c8ae7d84926f9cf42f7c40af8eaf60545/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIOWriteTest.java#L1200">SpannerIOWriteTest.testBatchFn_cells</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Error handling
      </td>
      <td>
         <p>Make sure that various errors (for example, HTTP error codes) from a data store are handled correctly
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/cd05896ebc385d12f7a7801f3bbba0127bef8b3b/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java#L2185">BigQueryIOWriteTest.testExtendedErrorRetrieval</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Retry policy
      </td>
      <td>
         <p>Confirms that the source/sink retries requests as expected
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/cd05896ebc385d12f7a7801f3bbba0127bef8b3b/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java#L822">BigQueryIOWriteTest.testRetryPolicy</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Output PCollection from a sink
      </td>
      <td>
         <p>Sinks should produce a PCollection that subsequent steps could depend on.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/cd05896ebc385d12f7a7801f3bbba0127bef8b3b/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOWriteTest.java#L1912">BigQueryIOWriteTest.testWriteTables</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Backlog byte reporting
      </td>
      <td>
         <p>Tests to confirm that the unbounded source transforms report backlog bytes correctly.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/cc0b2c5f3529e1896778dddeb6c740d40c7fb977/sdks/java/io/amazon-web-services2/src/test/java/org/apache/beam/sdk/io/aws2/kinesis/KinesisReaderTest.java#L170">KinesisReaderTest.getSplitBacklogBytesShouldReturnBacklogUnknown</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Watermark reporting
      </td>
      <td>
         <p>Tests to confirm that the unbounded source transforms report the watermark correctly.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/64dc9c62dce2e5af1f52c93a04702f17bfa89a60/sdks/java/io/kinesis/src/test/java/org/apache/beam/sdk/io/kinesis/WatermarkPolicyTest.java#L40">WatermarkPolicyTest.shouldAdvanceWatermarkWithTheArrivalTimeFromKinesisRecords</a>
      </td>
   </tr>
</table>
</div>


### Integration Tests


Integration tests test end-to-end interactions between the Beam runner and the data store a given I/O connects to. Since these usually involve remote RPC calls, integration tests take a longer time to execute. Additionally, Beam runners may use more than one worker when executing integration tests. Due to these costs, an integration test should only be implemented when a given scenario cannot be covered by a unit test.


<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p><strong>Implementing at least one integration test that involves interactions between Beam and the external storage system is required for submission.</strong>
      </td>
   </tr>
   <tr>
      <td>
         <p>I/O connectors that involve both source and a sink, Beam guide recommends implementing tests in the write-then-read form so that both read and write can be covered by the same test pipeline.
      </td>
   </tr>
   <tr>
      <td>
         <p>The integration testing class should be part of the same package as the I/O and named <strong>{connector}IOIT</strong>.
         <p>For example:
         <p><a href="https://github.com/apache/beam/blob/689e70b5131620540faf52e2f1e2dca7a36f269d/sdks/java/io/cassandra/src/test/java/org/apache/beam/sdk/io/cassandra/CassandraIOIT.java">sdks/java/io/cassandra/src/test/java/org/apache/beam/sdk/io/cassandra/CassandraIOIT.java</a>
      </td>
   </tr>
</table>
</div>


#### Suggested Test Cases


<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <th>
         <p>Test type
      </th>
      <th>
         <p>Description
      </th>
      <th>
         <p>Example(s)
      </th>
   </tr>
   <tr>
      <td>
         <p>“Write then read” test using Dataflow
      </td>
      <td>
         <p>Writes generated data to the datastore and reads the same data back from the datastore using Dataflow.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/774008de21090c635dc23c58b2f7d9d4aaa40cbf/sdks/java/io/jdbc/src/test/java/org/apache/beam/sdk/io/jdbc/JdbcIOIT.java#L129">JdbcIOIT.testWriteThenRead</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>“Write then read all” test using Dataflow
      </td>
      <td>
         <p>Same as “write then read” but for sources that support reading a PCollection of source configs. All future (SDF) sources are expected to support this.
         <p>If the same transform is used for “read” and “read all” forms or of the two transforms are essentially the same (for example, read transform is a simple wrapper of the read all or vise versa) just adding a single “read all” test should be sufficient.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/17a5c26bc0cbb57139d69683eaefc4b998c15866/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/spanner/SpannerReadIT.java#L173">SpannerReadIT.testReadAllRecordsInDb</a>
      </td>
   </tr>
   <tr>
      <td>
         <p>Unbounded write then read using Dataflow
      </td>
      <td>
         <p>A pipeline that continuously writes and reads data. Such a pipeline should be canceled to verify the results. This is only for connectors that support unbounded read.
      </td>
      <td>
         <p><a href="https://github.com/apache/beam/blob/95e9de7891593fa73d582cb6ba5ac0333b2675ff/sdks/java/io/kafka/src/test/java/org/apache/beam/sdk/io/kafka/KafkaIOIT.java#L134">KafkaIOIT.testKafkaIOReadsAndWritesCorrectlyInStreaming</a>
      </td>
   </tr>
</table>
</div>



### Performance Tests

**Because the Performance testing framework is still in flux, performance tests can be a follow-up submission after the actual I/O code.**

**The Performance testing framework does not yet support GoLang or Typescript.**

Performance benchmarks are a critical part of best practices for I/Os as they effectively address several areas:



* To evaluate if the cost and performance of a specific I/O or dataflow template meets the customer’s business requirements.
* To illustrate performance regressions and improvements to I/O or dataflow templates between code changes.
* To help end customers estimate costs and plan capacity to meet their SLOs.


#### Dashboard

Google runs performance tests routinely for built-in I/Os and publishes them to an externally viewable dashboard for [Java](https://s.apache.org/beam-community-metrics/d/bnlHKP3Wz/java-io-it-tests-dataflow?orgId=1) and [Python](https://s.apache.org/beam-community-metrics/d/gP7vMPqZz/python-io-it-tests-dataflow?orgId=1).


<p><img src="/images/io-standards/io-connector-performance-test-dashboard-screenshot.png" alt="Dataflow performance test dashboard" title="Dataflow performance test dashboard"></img>


#### Guidance


<div class="table-container-wrapper">
<table class="table table-bordered table-io-standards">
   <tr>
      <td>
         <p>Use the same tests for integration and performance tests when possible. Performance tests are usually the same as an integration test but involve a larger volume of data. Testing frameworks (internal and external) provide features to track performance benchmarks related to these tests and provide dashboards/tooling to detect anomalies.
      </td>
   </tr>
   <tr id="resource-scalability">
      <td>
         <p>Include a <strong>Resource Scalability</strong> section into your page under <strong> <a href="#built-in-io">Built-in I/O connector guides</a> </strong>documentation<strong> </strong> which will indicate the upper bounds which the IO has integration tests for.
         <p>For example:
         <p>An indication that KafkaIO has integration tests with <strong>xxxx</strong> topics. The documentation can state if the connector authors believe that the connector can scale beyond the integration test number, however this will make it clear to the user the limits of the tested paths.
         <p>The documentation should clearly indicate the configuration that was followed for the limits. For example using runner x and configuration option a.
      </td>
   </tr>
   <tr>
      <td>
         <p>Document the performance / internal metrics that your I/O collects including what they mean, and how they can be used (some connectors collect and publish performance metrics like latency/bundle size/etc)
      </td>
   </tr>
   <tr>
      <td>
         <p>Include expected performance characteristics of the I/O based on performance tests that the connector has in place.
      </td>
   </tr>
</table>
</div>
