---
title: "Apache Beam: Developing I/O connectors for Python"
aliases:
  - /documentation/io/authoring-python/
  - /documentation/sdks/python-custom-io/
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
# Developing I/O connectors for Python

**IMPORTANT:** Please use ``Splittable DoFn`` to develop your new I/O. For more details, please read
the [new I/O connector overview](/documentation/io/developing-io-overview/).

To connect to a data store that isn’t supported by Beam’s existing I/O
connectors, you must create a custom I/O connector that usually consist of a
source and a sink. All Beam sources and sinks are composite transforms; however,
the implementation of your custom I/O depends on your use case. Before you
start, read the [new I/O connector overview](/documentation/io/developing-io-overview/)
for an overview of developing a new I/O connector, the available implementation
options, and how to choose the right option for your use case.

This guide covers using the [Source and FileBasedSink interfaces](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.iobase.html)
for Python. The Java SDK offers the same functionality, but uses a slightly
different API. See [Developing I/O connectors for Java](/documentation/io/developing-io-java/)
for information specific to the Java SDK.

## Basic code requirements {#basic-code-reqs}

Beam runners use the classes you provide to read and/or write data using
multiple worker instances in parallel. As such, the code you provide for
`Source` and `FileBasedSink` subclasses must meet some basic requirements:

  1. **Serializability:** Your `Source` or `FileBasedSink` subclass must be
     serializable.  The service may create multiple instances of your `Source`
     or `FileBasedSink` subclass to be sent to multiple remote workers to
     facilitate reading or writing in parallel. The *way* the source and sink
     objects are serialized is runner specific.

  1. **Immutability:** Your `Source` or `FileBasedSink` subclass must be
     effectively immutable. You should only use mutable state in your `Source`
     or `FileBasedSink` subclass if you are using lazy evaluation of expensive
     computations that you need to implement the source.

  1. **Thread-Safety:** Your code must be thread-safe. The Beam SDK for Python
     provides the `RangeTracker` class to make this easier.

  1. **Testability:** It is critical to exhaustively unit-test all of your
     `Source` and `FileBasedSink` subclasses. A minor implementation error can
     lead to data corruption or data loss (such as skipping or duplicating
     records) that can be hard to detect. You can use test harnesses and utility
     methods available in the [source_test_utils module](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/source_test_utils.py)
     to develop tests for your source.

In addition, see the [PTransform style guide](/contribute/ptransform-style-guide/)
for Beam's transform style guidance.

## Implementing the Source interface

To create a new data source for your pipeline, you'll need to provide the format-specific logic that tells the service how to read data from your input source, and how to split your data source into multiple parts so that multiple worker instances can read your data in parallel.

If you're creating a data source that reads unbounded data, also provide the
logic for managing your source's watermark and checkpointing.

Supply the logic for your new source by creating the following classes:

  * A subclass of `BoundedSource` if you want to read a finite (batch) data set,
    or a subclass of `UnboundedSource` if you want to read an infinite
    (streaming) data set. The class describes the data you want to read,
    including the data's location and parameters (such as how much data to
    read).
  * For a `BoundedSource`, a subclass of `RangeTracker`. `RangeTracker` is a
    thread-safe object used to manage a range for a given position type.
  * For an `UnboundedSource`, a subclass of `UnboundedReader`, which holds the
    state involved in reading the stream, and a subclass of `CheckpointMark`,
    which records the position that a reader resumes from.
  * One or more user-facing wrapper composite transforms (`PTransform`) that
    wrap read operations. [PTransform wrappers](#ptransform-wrappers) discusses
    why you should avoid exposing your sources, and walks through how to create
    a wrapper.

You can find `BoundedSource` and `RangeTracker` in the
[apache_beam.io.iobase module](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.iobase.html),
and the unbounded classes in the
[apache_beam.io.unbounded_source module](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.unbounded_source.html).

### Implementing the Source subclass

Create a subclass of either `BoundedSource` or `UnboundedSource`, depending on
whether your data is a finite batch or an infinite stream. In either case, the
subclass overrides the methods that a runner uses to split the data and to
create a reader for it.

#### BoundedSource

`BoundedSource` represents a finite data set from which the service reads, possibly in parallel. `BoundedSource` contains a set of methods that the service uses to split the data set for reading by multiple remote workers.

To implement a `BoundedSource`, your subclass must override the following methods:

* `estimate_size`: Services use this method to estimate the *total size* of your data, in bytes. This estimate is in terms of external storage size, before performing decompression or other processing.

* `split`: Service use this method to split your finite data into bundles of a given size.

* `get_range_tracker`: Services use this method to get the `RangeTracker` for a given position range, and use the information to report progress and perform dynamic splitting of sources.

* `read`: This method returns an iterator that reads data from the source, with respect to the boundaries defined by the given `RangeTracker` object.

#### UnboundedSource

`UnboundedSource` represents an infinite data stream from which the runner may
read, possibly in parallel. `UnboundedSource` contains a set of methods that
support streaming reads in parallel; these include *checkpointing* for failure
recovery and *watermarking* for estimating data completeness in downstream parts
of your pipeline.

`UnboundedSource` is experimental, and its API may change in
backwards-incompatible ways.

To implement an `UnboundedSource`, your subclass must override the following
methods:

* `split`: The SDK uses this method to generate a list of `UnboundedSource`
objects that represent the sub-streams to read in parallel. Each sub-source must
be independent and must not share mutable state with its siblings, because the
runner may read them concurrently on different workers. Return `[self]` if the
source cannot be split. Splitting happens once, before any checkpoint exists.

* `create_reader`: Creates the associated `UnboundedReader` for this
`UnboundedSource`. When `checkpoint_mark` is `None`, the reader starts at the
beginning of the stream. Otherwise it resumes strictly after the position that
the mark encodes and does not re-deliver records that a previous bundle already
read.

* `get_checkpoint_mark_coder`: Returns the `Coder` for this source's
`CheckpointMark` instances. The SDK calls it while encoding and decoding a
reader's position, so it should be side-effect free and should not perform I/O.

Override `default_output_coder` to return a coder for your record type. The
default is a pickle coder, and a tighter coder also gives the output
`PCollection` an element type.

`UnboundedSource` has no per-record deduplication hook. If your data source can
deliver the same record more than once, drop the duplicates with the
[Deduplicate or DeduplicatePerKey](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.transforms.deduplicate.html)
transform after the read.

### Implementing the RangeTracker and UnboundedReader subclasses

A runner uses these classes to do the actual reading of your data set and to
track a reader's progress. A `BoundedSource` reads through its `read` method,
which claims positions from a `RangeTracker`. An `UnboundedSource` reads through
an `UnboundedReader`, which also reports a watermark and produces the checkpoint
marks that a runner resumes from.

#### RangeTracker

A `RangeTracker` is a thread-safe object used to manage the current range and current position of the reader of a `BoundedSource` and protect concurrent access to them.

To implement a `RangeTracker`, you should first familiarize yourself with the following definitions:

* **Position-based sources** - A position-based source can be described by a range of positions of an ordered type, and the records read by the source can be described by positions of that type. For example, for a record within a file, the position can be the starting byte offset of the record. The position type for the record in this case is `long`.

    The main requirement for position-based sources is **associativity**: Reading records in position range '[A, B)' and records in position range '[B, C)' should give the same records as reading records in position range '[A, C)', where 'A' &lt;= 'B' &lt;= 'C'.  This property ensures that no matter how many arbitrary sub-ranges a range of positions is split into, the total set of records they describe stays the same.

    The other important property is how the source's range relates to positions of records in the source. In many sources each record can be identified by a unique starting position. In this case:

     * All records returned by a source '[A, B)' must have starting positions in this range.
     * All but the last record should end within this range. The last record may or may not extend past the end of the range.
     * Records must not overlap.

    Such sources should define "read '[A, B)'" as "read from the first record starting at or after 'A', up to but not including the first record starting at or after 'B'".

    Some examples of such sources include reading lines or CSV from a text file, reading keys and values from a database, etc.

    The concept of *split points* allows to extend the definitions for dealing with sources where some records cannot be identified by a unique starting position.

* **Split points** - A split point describes a record that is the first one returned when reading the range from and including position **A** up to infinity (i.e. [A, infinity)).

    Some sources may have records that are not directly addressable. For example, imagine a file format consisting of a sequence of compressed blocks. Each block can be assigned an offset, but records within the block cannot be directly addressed without decompressing the block. Let us refer to this hypothetical format as *CBF (Compressed Blocks Format)*.

    Many such formats can still satisfy the associativity property. For example, in CBF, reading [A, B) can mean "read all the records in all blocks whose starting offset is in [A, B)".

    To support such complex formats, Beam introduces the notion of *split points*. A record is a split point if there exists a position **A** such that the record is the first one to be returned when reading the range [A, infinity). In CBF, the only split points would be the first records in each block.

    Split points allow us to define the meaning of a record's position and a source's range in the following cases:

     * For a record that is at a split point, its position is defined to be the largest **A** such that reading a source with the range [A, infinity) returns this record.
     * Positions of other records are only required to be non-decreasing.
     * Reading the source [A, B) must return records starting from the first split point at or after **A**, up to but not including the first split point at or after **B**. In particular, this means that the first record returned by a source MUST always be a split point.
     * Positions of split points must be unique.

    As a result, for any decomposition of the full range of the source into position ranges, the total set of records will be the full set of records in the source, and each record will be read exactly once.

* **Consumed positions** - Consumed positions refer to records that have been read.

    As the source is being read, and records read from it are being passed to the downstream transforms in the pipeline, we say that positions in the source are being *consumed*. When a reader has read a record (or promised to a caller that a record will be returned), positions up to and including the record's start position are considered *consumed*.

    Dynamic splitting can happen only at *unconsumed* positions. If the reader just returned a record at offset 42 in a file, dynamic splitting can happen only at offset 43 or beyond. Otherwise, that record could be read twice (by the current reader and the reader of the new task).

#### RangeTracker methods

To implement a `RangeTracker`, your subclass must override the following methods:

* `start_position`: Returns the starting position of the current range, inclusive.

* `stop_position`: Returns the ending position of the current range, exclusive.

* `try_claim`: This method is used to determine if a record at a split point is within the range. This method should modify the internal state of the `RangeTracker` by updating the last-consumed position to the given starting `position` of the record being read by the source. The method returns true if the given position falls within the current range.

* `set_current_position`: This method updates the last-consumed position to the given starting position of a record being read by a source. You can invoke this method for records that do not start at split points, and this should modify the internal state of the `RangeTracker`. If the record starts at a split point, you must invoke `try_claim` instead of this method.

* `position_at_fraction`: Given a fraction within the range [0.0, 1.0), this method will return the position at the given fraction compared to the position range [`self.start_position`, `self.stop_position`).

* `try_split`: This method attempts to split the current range into two parts around a suggested position. It is allowed to split at a different position, but in most cases it will split at the suggested position.

This method splits the current range [`self.start_position`, `self.stop_position`) into a "primary" part [`self.start_position`, `split_position`), and a "residual" part [`split_position`, `self.stop_position`), assuming that `split_position` has not been consumed yet.

If `split_position` has already been consumed, the method returns `None`.  Otherwise, it updates the current range to be the primary and returns a tuple (`split_position`, `split_fraction`).  `split_fraction` should be the fraction of size of range [`self.start_position`, `split_position`) compared to the original (before split) range [`self.start_position`, `self.stop_position`).

* `fraction_consumed`: Returns the approximate fraction of consumed positions in the source.

**Note:** Methods of class `iobase.RangeTracker` may be invoked by multiple threads, hence this class must be made thread-safe, for example, by using a single lock object.

#### UnboundedReader

An `UnboundedReader` holds the state involved in reading one `UnboundedSource`,
such as connections and buffers. `start` is called exactly once, then `advance`
is called repeatedly; whenever either returns `True`, the current record is
available through `get_current` and `get_current_timestamp`.

To implement an `UnboundedReader`, your subclass must override the following
methods:

* `start`: Initializes the reader, positions it at the first record, and returns
whether one is available. This is a good place for expensive initialization.

* `advance`: Advances to the next record and returns whether one is available. A
`False` return means that no data is available right now, which differs from the
end of the stream: a reader signals a permanent end by reporting a watermark of
`MAX_TIMESTAMP`. This method should not block; return `False` when no data is
currently available instead of waiting for more.

* `get_current`: Returns the record at the current position, last read by `start`
or `advance`.

* `get_current_timestamp`: Returns the event-time timestamp of the current
record, which becomes the timestamp of the output element.

* `get_watermark`: Returns a watermark, the approximate lower bound on the
timestamps of the records that this reader produces in the future. The runner
uses the watermark as an estimate of data completeness in windowing and triggers.
The watermark is treated as monotonic.

* `get_checkpoint_mark`: Returns a `CheckpointMark` that records how far the
reader has read. It is called only at a bundle boundary, and the mark is passed
back to `create_reader` to resume.

* `close`: Releases the reader's resources. The default is a no-op.

#### Checkpoint marks

A `CheckpointMark` is a durable, serializable position in the stream. The runner
persists it with the coder from `get_checkpoint_mark_coder` and hands it to
`create_reader` when a bundle resumes or when a worker recovers from a failure.

Override `finalize_checkpoint` to acknowledge or commit the consumed records
upstream, for example to ack the messages on a queue. It is called after the
runner has durably committed the work covered by that mark. Finalization is best
effort: a mark may never be finalized, and a retried bundle may re-cut a mark
over an overlapping span, so acknowledge by absolute position and keep the method
idempotent.

### Convenience Source base classes

The Beam SDK for Python contains some convenient abstract base classes to help you easily create new sources.

#### FileBasedSource

`FileBasedSource` is a framework for developing sources for new file types. You can derive your `BoundedSource` class from the [FileBasedSource](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/filebasedsource.py) class.

To create a source for a new file type, you need to create a sub-class of `FileBasedSource`.  Sub-classes of `FileBasedSource` must implement the method `FileBasedSource.read_records()`.

See [AvroSource](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/avroio.py) for an example implementation of `FileBasedSource`.


### Reading from a new Source

The following example, `CountingSource`, demonstrates an implementation of `BoundedSource` and uses the SDK-provided `RangeTracker` called `OffsetRangeTracker`.

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_custom_source_new_source >}}
{{< /highlight >}}

To read data from the source in your pipeline, use the `Read` transform:

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_custom_source_use_new_source >}}
{{< /highlight >}}

**Note:** When you create a source that end-users are going to use, we
recommended that you do not expose the code for the source itself as
demonstrated in the example above. Use a wrapping `PTransform` instead.
[PTransform wrappers](#ptransform-wrappers) discusses why you should avoid
exposing your sources, and walks through how to create a wrapper.


### Reading from an UnboundedSource

The following example, `QueueSource`, reads from a partitioned message queue.
`my_queue` stands in for the client library of the system you read from.

{{< highlight >}}
import apache_beam as beam
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.utils.timestamp import Timestamp


class QueueCheckpointMark(CheckpointMark):
  def __init__(self, offset):
    self.offset = offset

  def finalize_checkpoint(self):
    # Acknowledging an absolute offset is safe to repeat.
    my_queue.ack_through(self.offset)


class QueueReader(UnboundedReader):
  def __init__(self, partition, offset):
    self._partition = partition
    self._offset = offset
    self._message = None

  def start(self):
    return self.advance()

  def advance(self):
    message = my_queue.poll(self._partition, self._offset)
    if message is None:
      return False
    self._offset = message.offset
    self._message = message
    return True

  def get_current(self):
    return self._message.body

  def get_current_timestamp(self):
    return Timestamp(micros=self._message.event_time_micros)

  def get_watermark(self):
    return Timestamp(micros=my_queue.oldest_pending_micros(self._partition))

  def get_checkpoint_mark(self):
    return QueueCheckpointMark(self._offset)

  def close(self):
    my_queue.disconnect(self._partition)


class QueueSource(UnboundedSource):
  def __init__(self, topic, partition=None):
    self._topic = topic
    self._partition = partition

  def split(self, desired_num_splits, options=None):
    if self._partition is not None:
      return [self]
    return [
        QueueSource(self._topic, partition)
        for partition in my_queue.partitions(self._topic)
    ]

  def create_reader(self, options, checkpoint_mark):
    offset = None if checkpoint_mark is None else checkpoint_mark.offset
    return QueueReader(self._partition, offset)

  def get_checkpoint_mark_coder(self):
    return beam.coders.PickleCoder()

  def default_output_coder(self):
    return beam.coders.BytesCoder()
{{< /highlight >}}

To read data from the source in your pipeline, use the `Read` transform, which
dispatches an `UnboundedSource` automatically:

{{< highlight >}}
with beam.Pipeline(options=pipeline_options) as p:
  orders = p | beam.io.Read(QueueSource('orders'))
{{< /highlight >}}

A bundle ends when the reader runs out of data, and also once the reader has
emitted `max_records_per_bundle` records or spent `max_read_time_seconds` in the
bundle, so the runner commits the checkpoint and runs finalization before the
read resumes. An idle reader is polled again after `poll_interval` seconds.
Apply `ReadFromUnboundedSource` directly to change these defaults:

{{< highlight >}}
from apache_beam.io.unbounded_source import ReadFromUnboundedSource

orders = p | ReadFromUnboundedSource(
    QueueSource('orders'),
    poll_interval=5,
    max_records_per_bundle=1000,
    max_read_time_seconds=30)
{{< /highlight >}}

**Note:** As with a bounded source, we recommend that you do not expose the code
for the source itself to end-users. Use a wrapping `PTransform` instead.
[PTransform wrappers](#ptransform-wrappers) discusses why you should avoid
exposing your sources, and walks through how to create a wrapper.


## Using the FileBasedSink abstraction

If your data source uses files, you can implement the [FileBasedSink](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.filebasedsink.html)
abstraction to create a file-based sink. For other sinks, use `ParDo`,
`GroupByKey`, and other transforms offered by the Beam SDK for Python. See the
[developing I/O connectors overview](/documentation/io/developing-io-overview/)
for more details.

When using the `FileBasedSink` interface, you must provide the format-specific
logic that tells the runner how to write bounded data from your pipeline's
`PCollection`s to an output sink. The runner writes bundles of data in parallel
using multiple workers.

Supply the logic for your file-based sink by implementing the following classes:

  * A subclass of the abstract base class `FileBasedSink`. `FileBasedSink`
    describes a location or resource that your pipeline can write to in
    parallel. To avoid exposing your sink to end-users, use the `_` prefix when
    creating your `FileBasedSink` subclass.

  * A user-facing wrapper `PTransform` that, as part of the logic, calls
    `Write` and passes your `FileBasedSink` as a parameter. A user should not
    need to call `Write` directly.

The `FileBasedSink` abstract base class implements code that is common to Beam
sinks that interact with files, including:

  * Setting file headers and footers
  * Sequential record writing
  * Setting the output MIME type

`FileBasedSink` and its subclasses support writing files to any Beam-supported
`FileSystem` implementations. See the following Beam-provided `FileBasedSink`
implementation for an example:

  * [TextSink](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/textio.py)


## PTransform wrappers {#ptransform-wrappers}

When you create a source or sink that end-users will use, avoid exposing your
source or sink code. To avoid exposing your sources and sinks to end-users, your
new classes should use the `_` prefix. Then, implement a user-facing
wrapper `PTransform`.`By exposing your source or sink as a transform, your
implementation is hidden and can be arbitrarily complex or simple. The greatest
benefit of not exposing implementation details is that later on, you can add
additional functionality without breaking the existing implementation for users.

For example, if your users’ pipelines read from your source using
`beam.io.Read` and you want to insert a reshard into the pipeline, all
users would need to add the reshard themselves (using the `GroupByKey`
transform). To solve this, we recommended that you expose the source as a
composite `PTransform` that performs both the read operation and the reshard.

See Beam’s [PTransform style guide](/contribute/ptransform-style-guide/#exposing-a-ptransform-vs-something-else)
for additional information about wrapping with a `PTransform`.

The following examples change the source and sink from the above sections so
that they are not exposed to end-users. For the source, rename `CountingSource`
to `_CountingSource`. Then, create the wrapper `PTransform`, called
`ReadFromCountingSource`:

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_custom_source_new_ptransform >}}
{{< /highlight >}}

Finally, read from the source:

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_custom_source_use_ptransform >}}
{{< /highlight >}}

For the sink, rename `SimpleKVSink` to `_SimpleKVSink`. Then, create the wrapper `PTransform`, called `WriteToKVSink`:

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_custom_sink_new_ptransform >}}
{{< /highlight >}}

Finally, write to the sink:

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_custom_sink_use_ptransform >}}
{{< /highlight >}}
