---
title: "Apache Beam: Developing I/O connectors for Java"
aliases: /documentation/io/authoring-java/
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
# Developing I/O connectors for Java

To connect to a data store that isn’t supported by Beam’s existing I/O
connectors, you must create a custom I/O connector that usually consist of a
source and a sink. All Beam sources and sinks are composite transforms; however,
the implementation of your custom I/O depends on your use case. Before you
start, read the
[new I/O connector overview](/documentation/io/developing-io-overview/)
for an overview of developing a new I/O connector, the available implementation
options, and how to choose the right option for your use case.

This guide covers using the `Source` and `FileBasedSink` interfaces using Java.
The Python SDK offers the same functionality, but uses a slightly different API.
See [Developing I/O connectors for Python](/documentation/io/developing-io-python/)
for information specific to the Python SDK.

## Basic code requirements {#basic-code-reqs}

Beam runners use the classes you provide to read and/or write data using
multiple worker instances in parallel. As such, the code you provide for
`Source` and `FileBasedSink` subclasses must meet some basic requirements:

  1. **Serializability:** Your `Source` or `FileBasedSink` subclass, whether
     bounded or unbounded, must be Serializable. A runner might create multiple
     instances of your `Source` or `FileBasedSink` subclass to be sent to
     multiple remote workers to facilitate reading or writing in parallel.

  1. **Immutability:**
     Your `Source` or `FileBasedSink` subclass must be effectively immutable.
     All private fields must be declared final, and all private variables of
     collection type must be effectively immutable. If your class has setter
     methods, those methods must return an independent copy of the object with
     the relevant field modified.

     You should only use mutable state in your `Source` or `FileBasedSink`
     subclass if you are using lazy evaluation of expensive computations that
     you need to implement the source or sink; in that case, you must declare
     all mutable instance variables transient.

  1. **Thread-Safety:** Your code must be thread-safe. If you build your source
     to work with dynamic work rebalancing, it is critical that you make your
     code thread-safe. The Beam SDK provides a helper class to make this easier.
     See [Using Your BoundedSource with dynamic work rebalancing](#bounded-dynamic)
     for more details.

  1. **Testability:** It is critical to exhaustively unit test all of your
     `Source` and `FileBasedSink` subclasses, especially if you build your
     classes to work with advanced features such as dynamic work rebalancing. A
     minor implementation error can lead to data corruption or data loss (such
     as skipping or duplicating records) that can be hard to detect.

     To assist in testing `BoundedSource` implementations, you can use the
     SourceTestUtils class. `SourceTestUtils` contains utilities for automatically
     verifying some of the properties of your `BoundedSource` implementation. You
     can use `SourceTestUtils` to increase your implementation's test coverage
     using a wide range of inputs with relatively few lines of code. For
     examples that use `SourceTestUtils`, see the
     [AvroSourceTest](https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/io/AvroSourceTest.java) and
     [TextIOReadTest](https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/io/TextIOReadTest.java)
     source code.

In addition, see the [PTransform style guide](/contribute/ptransform-style-guide/)
for Beam's transform style guidance.

## Implementing the Source interface

To create a data source for your pipeline, you must provide the format-specific
logic that tells a runner how to read data from your input source, and how to
split your data source into multiple parts so that multiple worker instances can
read your data in parallel. If you're creating a data source that reads
unbounded data, you must provide additional logic for managing your source's
watermark and optional checkpointing.

Supply the logic for your source by creating the following classes:

  * A subclass of `BoundedSource` if you want to read a finite (batch) data set,
    or a subclass of `UnboundedSource` if you want to read an infinite (streaming)
    data set. These subclasses describe the data you want to read, including the
    data's location and parameters (such as how much data to read).

  * A subclass of `Source.Reader`. Each Source must have an associated Reader that
    captures all the state involved in reading from that `Source`. This can
    include things like file handles, RPC connections, and other parameters that
    depend on the specific requirements of the data format you want to read.

  * The `Reader` class hierarchy mirrors the Source hierarchy. If you're extending
    `BoundedSource`, you'll need to provide an associated `BoundedReader`. if you're
    extending `UnboundedSource`, you'll need to provide an associated
    `UnboundedReader`.

  * One or more user-facing wrapper composite transforms (`PTransform`) that
    wrap read operations. [PTransform wrappers](#ptransform-wrappers) discusses
    why you should avoid exposing your sources.


### Implementing the Source subclass

You must create a subclass of either `BoundedSource` or `UnboundedSource`,
depending on whether your data is a finite batch or an infinite stream. In
either case, your `Source` subclass must override the abstract methods in the
superclass. A runner might call these methods when using your data source. For
example, when reading from a bounded source, a runner uses these methods to
estimate the size of your data set and to split it up for parallel reading.

Your `Source` subclass should also manage basic information about your data
source, such as the location. For example, the example `Source` implementation
in Beam’s [DatastoreIO](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/io/gcp/datastore/DatastoreIO.html)
class takes host, datasetID, and query as arguments. The connector uses these
values to obtain data from Cloud Datastore.

#### BoundedSource

`BoundedSource` represents a finite data set from which a Beam runner may read,
possibly in parallel. `BoundedSource` contains a set of abstract methods that
the runner uses to split the data set for reading by multiple workers.

To implement a `BoundedSource`, your subclass must override the following
abstract methods:

  * `split`: The runner uses this method to split your finite data
    into bundles of a given size.

  * `getEstimatedSizeBytes`: The runner uses this method to estimate the total
    size of your data, in bytes.

  * `createReader`: Creates the associated `BoundedReader` for this
    `BoundedSource`.

You can see a model of how to implement `BoundedSource` and the required
abstract methods in Beam’s implementations for Cloud BigTable
([BigtableIO.java](https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIO.java))
and BigQuery ([BigQuerySourceBase.java](https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQuerySourceBase.java)).

#### UnboundedSource

`UnboundedSource` represents an infinite data stream from which the runner may
read, possibly in parallel. `UnboundedSource` contains a set of abstract methods
that the runner uses to support streaming reads in parallel; these include
*checkpointing* for failure recovery, *record IDs* to prevent data duplication,
and *watermarking* for estimating data completeness in downstream parts of your
pipeline.

To implement an `UnboundedSource`, your subclass must override the following
abstract methods:

  * `split`: The runner uses this method to generate a list of
    `UnboundedSource` objects which represent the number of sub-stream instances
    from which the service should read in parallel.

  * `getCheckpointMarkCoder`: The runner uses this method to obtain the Coder for
    the checkpoints for your source (if any).

  * `requiresDeduping`: The runner uses this method to determine whether the data
    requires explicit removal of duplicate records. If this method returns true,
    the runner will automatically insert a step to remove duplicates from your
    source's output.  This should return true if and only if your source
    provides record IDs for each record. See `UnboundedReader.getCurrentRecordId`
    for when this should be done.

  * `createReader`: Creates the associated `UnboundedReader` for this
    `UnboundedSource`.

### Implementing the Reader subclass

You must create a subclass of either `BoundedReader` or `UnboundedReader` to be
returned by your source subclass's `createReader` method. The runner uses the
methods in your `Reader` (whether bounded or unbounded) to do the actual reading
of your dataset.

`BoundedReader` and `UnboundedReader` have similar basic interfaces, which
you'll need to define. In addition, there are some additional methods unique to
`UnboundedReader` that you'll need to implement for working with unbounded data,
and an optional method you can implement if you want your `BoundedReader` to
take advantage of dynamic work rebalancing. There are also minor differences in
the semantics for the `start()` and `advance()` methods when using
`UnboundedReader`.

#### Reader methods common to both BoundedReader and UnboundedReader

A runner uses the following methods to read data using `BoundedReader` or
`UnboundedReader`:

  * `start`: Initializes the `Reader` and advances to the first record to be read.
    This method is called exactly once when the runner begins reading your data,
    and is a good place to put expensive operations needed for initialization.

  * `advance`: Advances the reader to the next valid record. This method must
    return false if there is no more input available. `BoundedReader` should stop
    reading once advance returns false, but `UnboundedReader` can return true in
    future calls once more data is available from your stream.

  * `getCurrent`: Returns the data record at the current position, last read by
    start or advance.

  * `getCurrentTimestamp`: Returns the timestamp for the current data record. You
    only need to override `getCurrentTimestamp` if your source reads data that has
    intrinsic timestamps. The runner uses this value to set the intrinsic
    timestamp for each element in the resulting output `PCollection`.

#### Reader methods unique to UnboundedReader

In addition to the basic `Reader` interface, `UnboundedReader` has some
additional methods for managing reads from an unbounded data source:

  * `getCurrentRecordId`: Returns a unique identifier for the current record.
    The runner uses these record IDs to filter out duplicate records. If your
    data has logical IDs present in each record, you can have this method return
    them; otherwise, you can return a hash of the record contents, using at
    least a 128-bit hash. It is incorrect to use Java's `Object.hashCode()`, as
    a 32-bit hash is generally insufficient for preventing collisions, and
    `hasCode()` is not guaranteed to be stable across processes.

    Implementing `getCurrentRecordId` is optional if your source uses a
    checkpointing scheme that uniquely identifies each record. For example, if
    your splits are files and the checkpoints are file positions up to which all
    data has been read, you do not need record IDs. However, record IDs can
    still be useful if upstream systems writing data to your source occasionally
    produce duplicate records that your source might then read.

  * `getWatermark`: Returns a watermark that your `Reader` provides. The watermark
    is the approximate lower bound on timestamps of future elements to be read
    by your `Reader`. The runner uses the watermark as an estimate of data
    completeness. Watermarks are used in windowing and triggers.

  * `getCheckpointMark`: The runner uses this method to create a checkpoint in
    your data stream. The checkpoint represents the progress of the
    `UnboundedReader`, which can be used for failure recovery. Different data
    streams may use different checkpointing methods; some sources might require
    received records to be acknowledged, while others might use positional
    checkpointing. You'll need to tailor this method to the most appropriate
    checkpointing scheme. For example, you might have this method return the
    most recently acked record(s).

  * `getCheckpointMark` is optional; you don't need to implement it if your data
    does not have meaningful checkpoints. However, if you choose not to
    implement checkpointing in your source, you may encounter duplicate data or
    data loss in your pipeline, depending on whether your data source tries to
    re-send records in case of errors.

You can read a bounded `PCollection` from an `UnboundedSource` by specifying
either `.withMaxNumRecords` or `.withMaxReadTime` when you read from your
source.  `.withMaxNumRecords` reads a fixed maximum number of records from your
unbounded source, while `.withMaxReadTime` reads from your unbounded source for
a fixed maximum time duration.

#### Using your BoundedSource with dynamic work rebalancing {#bounded-dynamic}

If your source provides bounded data, you can have your `BoundedReader` work
with dynamic work rebalancing by implementing the method `splitAtFraction`. The
runner may call `splitAtFraction` concurrently with start or advance on a given
reader so that the remaining data in your `Source` can be split and
redistributed to other workers.

When you implement `splitAtFraction`, your code must produce a
mutually-exclusive set of splits where the union of those splits matches the
total data set.

If you implement `splitAtFraction`, you must implement both `splitAtFraction`
and `getFractionConsumed` in a thread-safe manner, or data loss is possible. You
should also unit-test your implementation exhaustively to avoid data duplication
or data loss.

To ensure that your code is thread-safe, use the `RangeTracker` thread-safe
helper object to manage positions in your data source when implementing
`splitAtFraction` and `getFractionConsumed`.

We highly recommended that you unit test your implementations of
`splitAtFraction` using the `SourceTestUtils` class. `SourceTestUtils` contains
a number of methods for testing your implementation of `splitAtFraction`,
including exhaustive automatic testing.

### Convenience Source and Reader base classes

The Beam SDK contains some convenient abstract base classes to help you create
`Source` and `Reader` classes that work with common data storage formats, like
files.

#### FileBasedSource

If your data source uses files, you can derive your `Source` and `Reader`
classes from the `FileBasedSource` and `FileBasedReader` abstract base classes.
`FileBasedSource` is a bounded source subclass that implements code common to
Beam sources that interact with files, including:

  * File pattern expansion
  * Sequential record reading
  * Split points


## Using the FileBasedSink abstraction {#using-filebasedsink}

If your data source uses files, you can implement the `FileBasedSink`
abstraction to create a file-based sink. For other sinks, use `ParDo`,
`GroupByKey`, and other transforms offered by the Beam SDK for Java. See the
[developing I/O connectors overview](/documentation/io/developing-io-overview/)
for more details.

When using the `FileBasedSink` interface, you must provide the format-specific
logic that tells the runner how to write bounded data from your pipeline's
`PCollection`s to an output sink. The runner writes bundles of data in parallel
using multiple workers.

Supply the logic for your file-based sink by implementing the following classes:

  * A subclass of the abstract base class `FileBasedSink`. `FileBasedSink`
    describes a location or resource that your pipeline can write to in
    parallel. To avoid exposing your sink to end-users, your `FileBasedSink`
    subclass should be protected or private.

  * A user-facing wrapper `PTransform` that, as part of the logic, calls
    [WriteFiles](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/io/WriteFiles.java)
    and passes your `FileBasedSink` as a parameter. A user should not need to
    call `WriteFiles` directly.

The `FileBasedSink` abstract base class implements code that is common to Beam
sinks that interact with files, including:

  * Setting file headers and footers
  * Sequential record writing
  * Setting the output MIME type

`FileBasedSink` and its subclasses support writing files to any Beam-supported
`FileSystem` implementations. See the following Beam-provided `FileBasedSink`
implementations for examples:

  * [TextSink](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/io/TextSink.java) and
  * [AvroSink](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/io/AvroSink.java).


## PTransform wrappers {#ptransform-wrappers}

When you create a source or sink that end-users will use, avoid exposing your
source or sink code. To avoid exposing your sources and sinks to end-users, your
new classes should be protected or private. Then, implement a user-facing
wrapper `PTransform`. By exposing your source or sink as a transform, your
implementation is hidden and can be arbitrarily complex or simple. The greatest
benefit of not exposing implementation details is that later on, you can add
additional functionality without breaking the existing implementation for users.

For example, if your users’ pipelines read from your source using
`read` and you want to insert a reshard into the pipeline, all
users would need to add the reshard themselves (using the `GroupByKey`
transform). To solve this, we recommended that you expose the source as a
composite `PTransform` that performs both the read operation and the reshard.

See Beam’s [PTransform style guide](/contribute/ptransform-style-guide/#exposing-a-ptransform-vs-something-else)
for additional information about wrapping with a `PTransform`.

