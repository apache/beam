---
layout: section
title: "Apache Beam: Creating New Sources and Sinks with the Python SDK"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/python-custom-io/
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
# Creating New Sources and Sinks with the Python SDK

The Apache Beam SDK for Python provides an extensible API that you can use to create new data sources and sinks. This tutorial shows how to create new sources and sinks using [Beam's Source and Sink API](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py).

* Create a new source by extending the `BoundedSource` and `RangeTracker` interfaces.
* Create a new sink by implementing the `Sink` and `Writer` classes.


## Why Create a New Source or Sink

You'll need to create a new source or sink if you want your pipeline to read data from (or write data to) a storage system for which the Beam SDK for Python does not provide [native support]({{ site.baseurl }}/documentation/programming-guide/#pipeline-io).

In simple cases, you may not need to create a new source or sink. For example, if you need to read data from an SQL database using an arbitrary query, none of the advanced Source API features would benefit you. Likewise, if you'd like to write data to a third-party API via a protocol that lacks deduplication support, the Sink API wouldn't benefit you. In such cases it makes more sense to use a `ParDo`.

However, if you'd like to use advanced features such as dynamic splitting and size estimation, you should use Beam's APIs and create a new source or sink.


## Basic Code Requirements for New Sources and Sinks {#basic-code-reqs}

Services use the classes you provide to read and/or write data using multiple worker instances in parallel. As such, the code you provide for `Source` and `Sink` subclasses must meet some basic requirements:

### Serializability

Your `Source` or `Sink` subclass must be serializable. The service may create multiple instances of your `Source` or `Sink` subclass to be sent to multiple remote workers to facilitate reading or writing in parallel. Note that the *way* the source and sink objects are serialized is runner specific.

### Immutability

Your `Source` or `Sink` subclass must be effectively immutable. You should only use mutable state in your `Source` or `Sink` subclass if you are using lazy evaluation of expensive computations that you need to implement the source.

### Thread-Safety

Your code must be thread-safe. The Beam SDK for Python provides the `RangeTracker` class to make this easier.

### Testability

It is critical to exhaustively unit-test all of your `Source` and `Sink` subclasses. A minor implementation error can lead to data corruption or data loss (such as skipping or duplicating records) that can be hard to detect.

You can use test harnesses and utility methods available in the [source_test_utils module](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/source_test_utils.py) to develop tests for your source.


## Creating a New Source

You should create a new source if you'd like to use the advanced features that the Source API provides:

* Dynamic splitting
* Progress estimation
* Size estimation
* Splitting into parts of particular size recommended by the service

For example, if you'd like to read from a new file format that contains many records per file, or if you'd like to read from a key-value store that supports read operations in sorted key order.

To create a new data source for your pipeline, you'll need to provide the format-specific logic that tells the service how to read data from your input source, and how to split your data source into multiple parts so that multiple worker instances can read your data in parallel.

You supply the logic for your new source by creating the following classes:

* A subclass of `BoundedSource`, which you can find in the [iobase.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py) module. `BoundedSource` is a source that reads a finite amount of input records. The class describes the data you want to read, including the data's location and parameters (such as how much data to read).
* A subclass of `RangeTracker`, which you can find in the [iobase.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py) module. `RangeTracker` is a thread-safe object used to manage a range for a given position type.

### Implementing the BoundedSource Subclass

`BoundedSource` represents a finite data set from which the service reads, possibly in parallel. `BoundedSource` contains a set of methods that the service uses to split the data set for reading by multiple remote workers.

To implement a `BoundedSource`, your subclass must override the following methods:

* `estimate_size`: Services use this method to estimate the *total size* of your data, in bytes. This estimate is in terms of external storage size, before performing decompression or other processing.

* `split`: Service use this method to split your finite data into bundles of a given size.

* `get_range_tracker`: Services use this method to get the `RangeTracker` for a given position range, and use the information to report progress and perform dynamic splitting of sources.

* `read`: This method returns an iterator that reads data from the source, with respect to the boundaries defined by the given `RangeTracker` object.

### Implementing the RangeTracker Subclass

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

#### RangeTracker Methods

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

### Convenience Source Base Classes

The Beam SDK for Python contains some convenient abstract base classes to help you easily create new sources.

#### FileBasedSource

`FileBasedSource` is a framework for developing sources for new file types. You can derive your `BoundedSource` class from the [FileBasedSource](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/filebasedsource.py) class.

To create a source for a new file type, you need to create a sub-class of `FileBasedSource`.  Sub-classes of `FileBasedSource` must implement the method `FileBasedSource.read_records()`.

See [AvroSource](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/avroio.py) for an example implementation of `FileBasedSource`.


## Reading from a New Source

The following example, `CountingSource`, demonstrates an implementation of `BoundedSource` and uses the SDK-provided `RangeTracker` called `OffsetRangeTracker`.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_source_new_source %}```

To read data from the source in your pipeline, use the `Read` transform:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_source_use_new_source %}```

**Note:** When you create a source that end-users are going to use, it's recommended that you do not expose the code for the source itself as demonstrated in the example above, but rather use a wrapping `PTransform` instead. See [PTransform wrappers](#ptransform-wrappers) to see how and why to avoid exposing your sources.


## Creating a New Sink

You should create a new sink if you'd like to use the advanced features that the Sink API provides, such as global initialization and finalization that allow the write operation to appear "atomic" (i.e. either all data is written or none is).

A sink represents a resource that can be written to using the `Write` transform. A parallel write to a sink consists of three phases:

1. A sequential initialization phase. For example, creating a temporary output directory.
2. A parallel write phase where workers write bundles of records.
3. A sequential finalization phase. For example, merging output files.

For example, if you'd like to write to a new table in a database, you should use the Sink API. In this case, the initializer will create a temporary table, the writer will write rows to it, and the finalizer will rename the table to a final location.

To create a new data sink for your pipeline, you'll need to provide the format-specific logic that tells the sink how to write bounded data from your pipeline's `PCollection`s to an output sink. The sink writes bundles of data in parallel using multiple workers.

You supply the writing logic by creating the following classes:

* A subclass of `Sink`, which you can find in the [iobase.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py) module.  `Sink` describes the location or resource to write to. Depending on the type of sink, your `Sink` subclass may contain fields such as the path to an output directory on a filesystem or a database table name. `Sink` provides three methods for performing a write operation to the sink it describes. Your subclass of `Sink` must implement these three methods: `initialize_write()`, `open_writer()`, and `finalize_write()`.

* A subclass of `Writer`, which you can find in the [iobase.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py) module. `Writer` writes a bundle of elements from an input `PCollection` to your designated data sink. `Writer` defines two methods: `write()`, which writes a single record from the bundle, and `close()`, which is called once at the end of writing a bundle.

### Implementing the Sink Subclass

Your `Sink` subclass describes the location or resource to which your pipeline writes its output. This might include a file system location, the name of a database table or dataset, etc.

To implement a `Sink`, your subclass must override the following methods:

* `initialize_write`: This method performs any necessary initialization before writing to the output location. Services call this method before writing begins. For example, you can use `initialize_write` to create a temporary output directory.

* `open_writer`: This method enables writing a bundle of elements to the sink.

* `finalize_write`:This method finalizes the sink after all data is written to it. Given the result of initialization and an iterable of results from bundle writes, `finalize_write` performs finalization after writing and closes the sink. This method is called after all bundle write operations are complete.

**Caution:** `initialize_write` and `finalize_write` are conceptually called once: at the beginning and end of a `Write` transform.  However, when you implement these methods, you must ensure that they are **idempotent**, as they may be called multiple times on different machines in the case of failure, retry, or for redundancy.

### Implementing the Writer Subclass

Your `Writer` subclass implements the logic for writing a bundle of elements from a `PCollection` to output location defined in your `Sink`. Services may instantiate multiple instances of your `Writer` in different threads on the same worker, so access to any static members or methods must be thread-safe.

To implement a `Writer`, your subclass must override the following abstract methods:

* `write`: This method writes a value to your `Sink` using the current writer.

* `close`: This method closes the current writer.

#### Handling Bundle IDs

When the service calls `Sink.open_writer`, it will pass a unique bundle ID for the records to be written. Your `Writer` must use this bundle ID to ensure that its output does not interfere with that of other `Writer` instances that might have been created in parallel. This is particularly important as the service may retry write operations multiple times in case of failure.

For example, if your `Sink`'s output is file-based, your `Writer` class might use the bundle ID as a filename suffix to ensure that your `Writer` writes its records to a unique output file not used by other `Writer`s. You can then have your `Writer`'s `close` method return that file location as part of the write result.

### Convenience Sink and Writer Base Classes

The Beam SDK for Python contains some convenient abstract base classes to help you create `Source` and `Reader` classes that work with common data storage formats, like files.

#### FileSink

If your data source uses files, you can derive your `Sink` and `Writer` classes from the `FileBasedSink` and `FileBasedSinkWriter` classes, which can be found in the [filebasedsink.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/filebasedsink.py) module. These classes implement code common sinks that interact with files, including:

* Setting file headers and footers
* Sequential record writing
* Setting the output MIME type


## Writing to a New Sink

Consider a simple key-value storage that writes a given set of key-value pairs to a set of tables. The following is the key-value storage's API:

* `connect(url)` Connects to the storage and returns an access token which can be used to perform further operations.
* `open_table(access_token, table_name)` Creates a table named 'table_name'. Returns a table object.
* `write_to_table(access_token, table, key, value)` Writes a key, value pair to the given table.
* `rename_table(access_token, old_name, new_name)` Renames the table named 'old_name' to 'new_name'.

The following code demonstrates how to implement the `Sink` class for this key-value storage.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_sink_new_sink %}```

The following code demonstrates how to implement the `Writer` class for this key-value storage.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_sink_new_writer %}```

The following code demonstrates how to write to the sink using the `Write` transform.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_sink_use_new_sink %}```

**Note:** When you create a sink that end-users are going to use, it's recommended that you do not expose the code for the sink itself as demonstrated in the example above, but rather use a wrapping `PTransform` instead. See [PTransform wrappers](#ptransform-wrappers) to see how and why to avoid exposing your sinks.


## PTransform Wrappers

If you create a new source or sink for your own use, such as for learning purposes, you should create them as explained in the sections above and use them as demonstrated in the examples.

However, when you create a source or sink that end-users are going to use, instead of exposing the source or sink itself, you should create a wrapper `PTransform`. Ideally, a source or sink should be exposed to users simply as "something that can be applied in a pipeline", which is actually a `PTransform`. That way, its implementation can be hidden and arbitrarily complex or simple.

The greatest benefit of not exposing the implementation details is that later on you will be able to add additional functionality without breaking the existing implementation for users.  For example, if your users' pipelines read from your source using `beam.io.Read(...)` and you want to insert a reshard into the pipeline, all of your users would need to add the reshard themselves (using the `GroupByKey` transform). To solve this, it's recommended that you expose your source as a composite `PTransform` that performs both the read operation and the reshard.

To avoid exposing your sources and sinks to end-users, it's recommended that you use the `_` prefix when creating your new source and sink classes. Then, create a wrapper `PTransform`.

The following examples change the source and sink from the above sections so that they are not exposed to end-users. For the source, rename `CountingSource` to `_CountingSource`. Then, create the wrapper `PTransform`, called `ReadFromCountingSource`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_source_new_ptransform %}```

Finally, read from the source:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_source_use_ptransform %}```

For the sink, rename `SimpleKVSink` to `_SimpleKVSink`. Then, create the wrapper `PTransform`, called `WriteToKVSink`:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_sink_new_ptransform %}```

Finally, write to the sink:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:model_custom_sink_use_ptransform %}```
