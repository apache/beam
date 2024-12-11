---
title: "Beam glossary"
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

# Apache Beam glossary

## Aggregation

A transform pattern for computing a value from multiple input elements. Aggregation is similar to the reduce operation in the [MapReduce](https://en.wikipedia.org/wiki/MapReduce) model. Aggregation transforms include Combine (applies a user-defined function to all elements in the aggregation), Count (computes the count of all elements in the aggregation), Max (computes the maximum element in the aggregation), and Sum (computes the sum of all elements in the aggregation).

For a list of built-in aggregation transforms, see:

* [Java Transform catalog](/documentation/transforms/java/overview/#aggregation)
* [Python Transform catalog](/documentation/transforms/python/overview/#aggregation)

To learn more, see:

* [Basics of the Beam model: Aggregation](/documentation/basics/#aggregation)

## Apply

A method for invoking a transform on an input PCollection (or set of PCollections) to produce one or more output PCollections. The `apply` method is attached to the PCollection (or value). Invoking multiple Beam transforms is similar to method chaining, but with a difference: You apply the transform to the input PCollection, passing the transform itself as an argument, and the operation returns the output PCollection. Because of Beam’s deferred execution model, applying a transform does not immediately execute that transform.

To learn more, see:

* [Applying transforms](/documentation/programming-guide/#applying-transforms)

## Batch processing

A data processing paradigm for working with finite, or bounded, datasets. A bounded PCollection represents a dataset of a known, fixed size. Reading from a batch data source, such as a file or a database, creates a bounded PCollection. A batch processing job eventually ends, in contrast to a streaming job, which runs until cancelled.

To learn more, see:

* [Size and boundedness](/documentation/programming-guide/#size-and-boundedness)

## Bounded data

A dataset of a known, fixed size (alternatively, a dataset that is not growing over time). A PCollection can be bounded or unbounded, depending on the source of the data that it represents. Reading from a batch data source, such as a file or a database, creates a bounded PCollection. Beam also supports reading a bounded amount of data from an unbounded source.

To learn more, see:

* [Size and boundedness](/documentation/programming-guide/#size-and-boundedness)

## Bundle

The processing and commit/retry unit for elements in a PCollection. Instead of processing all elements in a PCollection simultaneously, Beam processes the elements in bundles. The runner handles the division of the collection into bundles, and in doing so it may optimize the bundle size for the use case. For example, a streaming runner might process smaller bundles than a batch runner.

To learn more, see:

* [Bundling and persistence](/documentation/runtime/model/#bundling-and-persistence)

## Coder

A component that describes how the elements of a PCollection can be encoded and decoded. To support distributed processing and cross-language portability, Beam needs to be able to encode each element of a PCollection as bytes. The Beam SDKs provide built-in coders for common types and language-specific mechanisms for specifying the encoding of a PCollection.

To learn more, see:

* [Data encoding and type safety](/documentation/programming-guide/#data-encoding-and-type-safety)

## CoGroupByKey

A PTransform that takes two or more PCollections and aggregates the elements by key. In effect, CoGroupByKey performs a relational join of two or more key/value PCollections that have the same key type. While GroupByKey performs this operation over a single input collection, CoGroupByKey operates over multiple input collections.

To learn more, see:

* [CoGroupByKey](/documentation/programming-guide/#cogroupbykey)
* [CoGroupByKey (Java)](/documentation/transforms/java/aggregation/cogroupbykey/)
* [CoGroupByKey (Python)](/documentation/transforms/python/aggregation/cogroupbykey/)

## Collection

See [PCollection](/documentation/glossary/#pcollection).

## Combine

A PTransform for combining all elements of a PCollection or all values associated with a key. When you apply a Combine transform, you have to provide a user-defined function (UDF) that contains the logic for combining the elements or values. The combining function should be [commutative](https://en.wikipedia.org/wiki/Commutative_property) and [associative](https://en.wikipedia.org/wiki/Associative_property), because the function is not necessarily invoked exactly once on all values with a given key.

To learn more, see:

* [Combine](/documentation/programming-guide/#combine)
* [Combine (Java)](/documentation/transforms/java/aggregation/combine/)
* [CombineGlobally (Python)](/documentation/transforms/python/aggregation/combineglobally/)
* [CombinePerKey (Python)](/documentation/transforms/python/aggregation/combineperkey/)
* [CombineValues (Python)](/documentation/transforms/python/aggregation/combinevalues/)

## Composite transform

A PTransform that expands into many PTransforms. Composite transforms have a nested structure, in which a complex transform applies one or more simpler transforms. These simpler transforms could be existing Beam operations like ParDo, Combine, or GroupByKey, or they could be other composite transforms. Nesting multiple transforms inside a single composite transform can make your pipeline more modular and easier to understand. Many of the built-in transforms are composite transforms.

To learn more, see:

* [Composite transforms](/documentation/programming-guide/#composite-transforms)

## Counter (metric)

A metric that reports a single long value and can be incremented. In the Beam model, metrics provide insight into the state of a pipeline, potentially while the pipeline is running.

To learn more, see:

* [Types of metrics](/documentation/programming-guide/#types-of-metrics)

## Cross-language transforms

Portable transforms that can be shared across Beam SDKs. With cross-language transforms, you can use transforms written in any supported SDK language (currently, Java and Python) in a pipeline written in a different SDK language. For example, you could use the Apache Kafka connector from the Java SDK in a Python streaming pipeline. Cross-language transforms make it possible to provide new functionality simultaneously in different SDKs.

To learn more, see:

* [Multi-language pipelines](/documentation/programming-guide/#multi-language-pipelines)

## Deferred execution

A feature of the Beam execution model. Beam operations are deferred, meaning that the result of a given operation may not be available for control flow. Deferred execution allows the Beam API to support parallel processing of data and perform pipeline-level optimizations.

## Distribution (metric)

A metric that reports information about the distribution of reported values. In the Beam model, metrics provide insight into the state of a pipeline, potentially while the pipeline is running.

To learn more, see:

* [Types of metrics](/documentation/programming-guide/#types-of-metrics)

## DoFn

A function object used by ParDo (or some other transform) to process the elements of a PCollection, often producing elements for an output PCollection. A DoFn is a user-defined function, meaning that it contains custom code that defines a data processing task in your pipeline. The Beam system invokes a DoFn one or more times to process some arbitrary bundle of elements, but Beam doesn’t guarantee an exact number of invocations.

To learn more, see:

* [ParDo](/documentation/programming-guide/#pardo)

## Driver

A program that defines your pipeline, including all of the inputs, transforms, and outputs. To use Beam, you need to create a driver program using classes from one of the Beam SDKs. The driver program creates a pipeline and specifies the execution options that tell the pipeline where and how to run. These options include the runner, which determines what backend your pipeline will run on.

To learn more, see:

* [Overview](/documentation/programming-guide/#overview)

## Element

The unit of data in a PCollection. Elements in a PCollection can be of any type, but they must all have the same type. This allows parallel computations to operate uniformly across the entire collection. Some element types have a structure that can be introspected (for example, JSON, Protocol Buffer, Avro, and database records).

To learn more, see:

* [PCollection characteristics](/documentation/programming-guide/#pcollection-characteristics)

## Element-wise

A type of transform that independently processes each element in an input PCollection. Element-wise is similar to the map operation in the [MapReduce](https://en.wikipedia.org/wiki/MapReduce) model. An element-wise transform might output 0, 1, or multiple values for each input element. This is in contrast to aggregation transforms, which compute a single value from multiple input elements. Element-wise operations include Filter, FlatMap, and ParDo.

For a complete list of element-wise transforms, see:

* [Java Transform catalog](/documentation/transforms/java/overview/#element-wise)
* [Python Transform catalog](/documentation/transforms/python/overview/#element-wise)

## Engine

A data-processing system, such as Dataflow, Spark, or Flink. A Beam runner for an engine executes a Beam pipeline on that engine.

## Event time

The time a data event occurs, determined by a timestamp on an element. This is in contrast to processing time, which is when an element is processed in a pipeline. An event could be, for example, a user interaction or a write to an error log. There’s no guarantee that events will appear in a pipeline in order of event time, but windowing and timers let you reason correctly about event time.

To learn more, see:

* [Watermarks and late data](/documentation/programming-guide/#watermarks-and-late-data)
* [Triggers](/documentation/programming-guide/#triggers)

## Expansion Service

A service that enables a pipeline to apply (expand) cross-language transforms defined in other SDKs. For example, by connecting to a Java expansion service, the Python SDK can apply transforms implemented in Java. Currently, SDKs typically start up expansion services as local processes, but in the future Beam may support long-running expansion services. The development of expansion services is part of the ongoing effort to support multi-language pipelines.

## Flatten
One of the core PTransforms. Flatten merges multiple PCollections into a single logical PCollection.

To learn more, see:

* [Flatten](/documentation/programming-guide/#flatten)
* [Flatten (Java)](/documentation/transforms/java/other/flatten/)
* [Flatten (Python)](/documentation/transforms/python/other/flatten/)

## Fn API

An interface that lets a runner invoke SDK-specific user-defined functions. The Fn API, together with the Runner API, supports the ability to mix and match SDKs and runners. Used together, the Fn and Runner APIs let new SDKs run on every runner, and let new runners run pipelines from every SDK.

## Fusion

An optimization that Beam runners can apply before running a pipeline. When one transform outputs a PCollection that’s consumed by another transform, or when two or more transforms take the same PCollection as input, a runner may be able to fuse the transforms together into a single processing unit (a *stage* in Dataflow). The consuming DoFn processes elements as they are emitted by the producing DoFn, rather than waiting for the entire intermediate PCollection to be computed. Fusion can make pipeline execution more efficient by preventing I/O operations.

## Gauge (metric)

A metric that reports the latest value out of reported values. In the Beam model, metrics provide insight into the state of a pipeline, potentially while the pipeline is running. Because metrics are collected from many workers, the gauge value may not be the absolute last value, but it will be one of the latest values produced by one of the workers.

To learn more, see:

* [Types of metrics](/documentation/programming-guide/#types-of-metrics)

## GroupByKey

A PTransform for processing collections of key/value pairs. GroupByKey is a parallel reduction operation, similar to the shuffle of a map/shuffle/reduce algorithm. The input to GroupByKey is a collection of key/value pairs in which multiple pairs have the same key but different values (i.e. a multimap). You can use GroupByKey to collect all of the values associated with each unique key.

To learn more, see:

* [GroupByKey](/documentation/programming-guide/#groupbykey)
* [GroupByKey (Java)](/documentation/transforms/java/aggregation/groupbykey/)
* [GroupByKey (Python)](/documentation/transforms/python/aggregation/groupbykey/)

## I/O connector

A set of PTransforms for working with external data storage systems. When you create a pipeline, you often need to read from or write to external data systems such as files or databases. Beam provides read and write transforms for a number of common data storage types.

To learn more, see:

* [Pipeline I/O](/documentation/programming-guide/#pipeline-io)
* [Built-in I/O Transforms](/documentation/io/built-in/)

## Map

An element-wise PTransform that applies a user-defined function (UDF) to each element in a PCollection. Using Map, you can transform each individual element into a new element, but you can't change the number of elements.

To learn more, see:

* [Map (Python)](/documentation/transforms/python/elementwise/map/)
* [MapElements (Java)](/documentation/transforms/java/elementwise/mapelements/)

## Metrics

Data on the state of a pipeline, potentially while the pipeline is running. You can use the built-in Beam metrics to gain insight into the functioning of your pipeline. For example, you might use Beam metrics to track errors, calls to a backend service, or the number of elements processed. Beam currently supports three types of metric: Counter, Distribution, and Gauge.

To learn more, see:

* [Metrics](/documentation/programming-guide/#metrics)

## Multi-language pipeline

A pipeline that uses cross-language transforms. You can combine transforms written in any supported SDK language (currently, Java and Python) and use them in one multi-language pipeline.

To learn more, see:

* [Multi-language pipelines](/documentation/programming-guide/#multi-language-pipelines)

## ParDo

The lowest-level element-wise PTransform. For each element in an input PCollection, ParDo applies a function and emits zero, one, or multiple elements to an output PCollection. “ParDo” is short for “Parallel Do.” It’s similar to the map operation in a [MapReduce](https://en.wikipedia.org/wiki/MapReduce) algorithm and the reduce operation when following a GroupByKey. ParDo is also comparable to the `apply` method from a DataFrame, or the `UPDATE` keyword from SQL.

To learn more, see:

* [ParDo](/documentation/programming-guide/#pardo)
* [ParDo (Java)](/documentation/transforms/java/elementwise/pardo/)
* [ParDo (Python)](/documentation/transforms/python/elementwise/pardo/)

## Partition

An element-wise PTransform that splits a single PCollection into a fixed number of smaller, disjoint PCollections. Partition requires a user-defined function (UDF) to determine how to split up the elements of the input collection into the resulting output collections. The number of partitions must be determined at graph construction time, meaning that you can’t determine the number of partitions using data calculated by the running pipeline.

To learn more, see:

* [Partition](/documentation/programming-guide/#partition)
* [Partition (Java)](/documentation/transforms/java/elementwise/partition/)
* [Partition (Python)](/documentation/transforms/python/elementwise/partition/)

## PCollection

A potentially distributed, homogeneous dataset or data stream. PCollections represent data in a Beam pipeline, and Beam transforms (PTransforms) use PCollection objects as inputs and outputs. PCollections are intended to be immutable, meaning that once a PCollection is created, you can’t add, remove, or change individual elements. The “P” stands for “parallel.”

To learn more, see:

* [Basics of the Beam model: PCollection](/documentation/basics/#pcollection)
* [Programming guide: PCollections](/documentation/programming-guide/#pcollections)

## Pipe operator (`|`)

Delimits a step in a Python pipeline. For example: `[Final Output PCollection] = ([Initial Input PCollection] | [First Transform] | [Second Transform] | [Third Transform])`. The output of each transform is passed from left to right as input to the next transform. The pipe operator in Python is equivalent to the `apply` method in Java (in other words, the pipe applies a transform to a PCollection), and usage is similar to the pipe operator in shell scripts, which lets you pass the output of one program into the input of another.

To learn more, see:

* [Applying transforms](/documentation/programming-guide/#applying-transforms)

## Pipeline

An encapsulation of your entire data processing task, including reading input data from a source, transforming that data, and writing output data to a sink. You can think of a pipeline as a Beam program that uses PTransforms to process PCollections. (Alternatively, you can think of it as a single, executable composite PTransform with no inputs or outputs.) The transforms in a pipeline can be represented as a directed acyclic graph (DAG). All Beam driver programs must create a pipeline.

To learn more, see:

* [Basics of the Beam model: Pipeline](/documentation/basics/#pipeline)
* [Overview](/documentation/programming-guide/#overview)
* [Creating a pipeline](/documentation/programming-guide/#creating-a-pipeline)
* [Design your pipeline](/documentation/pipelines/design-your-pipeline/)
* [Create your pipeline](/documentation/pipelines/create-your-pipeline/)

## Processing time

The real-world time at which an element is processed at some stage in a pipeline. Processing time is not the same as event time, which is the time at which a data event occurs. Processing time is determined by the clock on the system processing the element. There’s no guarantee that elements will be processed in order of event time.

To learn more, see:

* [Watermarks and late data](/documentation/programming-guide/#watermarks-and-late-data)
* [Triggers](/documentation/programming-guide/#triggers)

## PTransform

A data processing operation, or a step, in your pipeline. A PTransform takes zero or more PCollections as input, applies a processing function to the elements of that PCollection, and produces zero or more output PCollections. Some PTransforms accept user-defined functions that apply custom logic. The “P” stands for “parallel.”

To learn more, see:

* [Basics of the Beam model: PTransform](/documentation/basics/#ptransform)
* [Overview](/documentation/programming-guide/#overview)
* [Transforms](/documentation/programming-guide/#transforms)

## Resource hints

A Beam feature that lets you provide information to a runner about the compute resource requirements of your pipeline. You can use resource hints to define requirements for specific transforms or for an entire pipeline. For example, you could use a resource hint to specify the minimum amount of memory to allocate to workers. The runner is responsible for interpreting resource hints, and runners can ignore unsupported hints.

To learn more, see:

* [Resource hints](/documentation/runtime/resource-hints)

## Runner

A runner runs a pipeline on a specific platform. Most runners are translators or adapters to massively parallel big data processing systems. Other runners exist for local testing and debugging. Among the supported runners are Google Cloud Dataflow, Apache Spark, Apache Samza, Apache Flink, the Interactive Runner, and the Direct Runner.

To learn more, see:

* [Basics of the Beam model: Runner](/documentation/basics/#runner)
* [Choosing a Runner](/documentation/#choosing-a-runner)
* [Beam Capability Matrix](/documentation/runners/capability-matrix/)

## Schema

A language-independent type definition for the elements of a PCollection. The schema for a PCollection defines elements of that PCollection as an ordered list of named fields. Each field has a name, a type, and possibly a set of user options. Schemas provide a way to reason about types across different programming-language APIs. They also let you describe data transformations more succinctly and at a higher level.

To learn more, see:

* [Basics of the Beam model: Schema](/documentation/basics/#schema)
* [Programming guide: Schemas](/documentation/programming-guide/#schemas)
* [Schema Patterns](/documentation/patterns/schema/)

## SchemaTransform

A transform that takes and produces PCollections of Beam Rows with a predefined Schema, i.e.:

```java
SchemaTransform extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {}
```

Mainly used for portable transform use-cases. To learn more, see:

* [Python Multi-Language Guide](sdks/python-custom-multi-language-pipelines-guide.md)

## Session

A time interval for grouping data events. A session is defined by some minimum gap duration between events. For example, a data stream representing user mouse activity may have periods with high concentrations of clicks followed by periods of inactivity. A session can represent such a pattern of activity delimited by inactivity.

To learn more, see:

* [Session windows](/documentation/programming-guide/#session-windows)
* [Analyzing Usage Patterns](/get-started/mobile-gaming-example/#analyzing-usage-patterns)

## Side input

Additional input to a PTransform that is provided in its entirety, rather than element-by-element. Side input is input that you provide in addition to the main input PCollection. A DoFn can access side input each time it processes an element in the PCollection.

To learn more, see:

* [Side inputs](/documentation/programming-guide/#side-inputs)
* [Side input patterns](/documentation/patterns/side-inputs/)

## Sink

A transform that writes to an external data storage system, like a file or database.

To learn more, see:

* [Developing new I/O connectors](/documentation/io/developing-io-overview/)
* [Pipeline I/O](/documentation/programming-guide/#pipeline-io)
* [Built-in I/O transforms](/documentation/io/built-in/)

## Source

A transform that reads from an external storage system. A pipeline typically reads input data from a source. The source has a type, which may be different from the sink type, so you can change the format of data as it moves through your pipeline.

To learn more, see:

* [Developing new I/O connectors](/documentation/io/developing-io-overview/)
* [Pipeline I/O](/documentation/programming-guide/#pipeline-io)
* [Built-in I/O transforms](/documentation/io/built-in/)

## Splittable DoFn

A generalization of DoFn that makes it easier to create complex, modular I/O connectors. A Splittable DoFn (SDF) can process elements in a non-monolithic way, meaning that the processing can be decomposed into smaller tasks. With SDF, you can check-point the processing of an element, and you can split the remaining work to yield additional parallelism. SDF is recommended for building new I/O connectors.

To learn more, see:

* [Basics of the Beam model: Splittable DoFn](/documentation/basics/#splittable-dofn)
* [Programming guide: Splittable DoFns](/documentation/programming-guide/#splittable-dofns)
* [Splittable DoFn in Apache Beam is Ready to Use](/blog/splittable-do-fn-is-available/)

## Stage

The unit of fused transforms in a pipeline. Runners can perform fusion optimization to make pipeline execution more efficient. In Dataflow, the pipeline is conceptualized as a graph of fused stages.

## State

Persistent values that a PTransform can access. The state API lets you augment element-wise operations (for example, ParDo or Map) with mutable state. Using the state API, you can read from, and write to, state as you process each element of a PCollection. You can use the state API together with the timer API to create processing tasks that give you fine-grained control over the workflow. State is always local to a key and window.

To learn more, see:

* [Basics of the Beam model: State and timers](/documentation/basics/#state-and-timers)
* [Programming guide: State and Timers](/documentation/programming-guide/#state-and-timers)
* [Stateful processing with Apache Beam](/blog/stateful-processing/)

## Streaming

A data processing paradigm for working with infinite, or unbounded, datasets. Reading from a streaming data source, such as Pub/Sub or Kafka, creates an unbounded PCollection. An unbounded PCollection must be processed using a job that runs continuously, because the entire collection can never be available for processing at any one time.

To learn more, see:

* [Size and boundedness](/documentation/programming-guide/#size-and-boundedness)
* [Python Streaming Pipelines](/documentation/sdks/python-streaming/)

# Timer

A Beam feature that enables delayed processing of data stored using the state API. The timer API lets you set timers to call back at either an event-time or a processing-time timestamp. You can use the timer API together with the state API to create processing tasks that give you fine-grained control over the workflow.

To learn more, see:

* [Basics of the Beam model: State and timers](/documentation/basics/#state-and-timers)
* [Programming guide: State and Timers](/documentation/programming-guide/#state-and-timers)
* [Stateful processing with Apache Beam](/blog/stateful-processing/)
* [Timely (and Stateful) Processing with Apache Beam](/blog/timely-processing/)

## Timestamp

A point in event time associated with an element in a PCollection and used to assign a window to the element. The source that creates the PCollection assigns each element an initial timestamp, often corresponding to when the element was read or added. But you can also manually assign timestamps. This can be useful if elements have an inherent timestamp, but the timestamp is somewhere in the structure of the element itself (for example, a time field in a server log entry).

To learn more, see:

* [Basics of the Beam model: Timestamp](/documentation/basics/#timestamp)
* [Element timestamps](/documentation/programming-guide/#element-timestamps)
* [Adding timestamps to a PCollection’s elements](/documentation/programming-guide/#adding-timestamps-to-a-pcollections-elements)

## Transform

See PTransform.

## Trigger

Determines when to emit aggregated result data from a window. You can use triggers to refine the windowing strategy for your pipeline. If you use the default windowing configuration and default trigger, Beam outputs an aggregated result when it estimates that all data for a window has arrived, and it discards all subsequent data for that window. But you can also use triggers to emit early results, before all the data in a given window has arrived, or to process late data by triggering after the event time watermark passes the end of the window.

To learn more, see:

* [Basics of the Beam model: Trigger](/documentation/basics/#trigger)
* [Programming guide: Triggers](/documentation/programming-guide/#triggers)

## Unbounded data

A dataset that grows over time, with elements processed as they arrive. A PCollection can be bounded or unbounded, depending on the source of the data that it represents. Reading from a streaming or continuously-updating data source, such as Pub/Sub or Kafka, typically creates an unbounded PCollection.

To learn more, see:

* [Size and boundedness](/documentation/programming-guide/#size-and-boundedness)

## User-defined function

Custom logic that a PTransform applies to your data. Some PTransforms accept a user-defined function (UDF) as a way to configure the transform. For example, ParDo expects user code in the form of a DoFn object. Each language SDK has its own idiomatic way of expressing user-defined functions, but there are some common requirements, like serializability and thread compatibility.

To learn more, see:

* [Basics of the Beam model: User-Defined Functions (UDFs)](/documentation/basics/#user-defined-functions-udfs)
* [ParDo](/documentation/programming-guide/#pardo)
* [Requirements for writing user code for Beam transforms](/documentation/programming-guide/#requirements-for-writing-user-code-for-beam-transforms)

## Watermark

An estimate on the lower bound of the timestamps that will be seen (in the future) at this point of the pipeline. Watermarks provide a way to estimate the completeness of input data. Every PCollection has an associated watermark. Once the watermark progresses past the end of a window, any element that arrives with a timestamp in that window is considered late data.

To learn more, see:

* [Basics of the Beam model: Watermark](/documentation/basics/#watermark)
* [Programming guide: Watermarks and late data](/documentation/programming-guide/#watermarks-and-late-data)

## Windowing

Partitioning a PCollection into bounded subsets grouped by the timestamps of individual elements. In the Beam model, any PCollection – including unbounded PCollections – can be subdivided into logical windows. Each element in a PCollection is assigned to one or more windows according to the PCollection's windowing function, and each individual window contains a finite number of elements. Transforms that aggregate multiple elements, such as GroupByKey and Combine, work implicitly on a per-window basis.

To learn more, see:

* [Basics of the Beam model: Window](/documentation/basics/#window)
* [Programming guide: Windowing](/documentation/programming-guide/#windowing)

## Worker

A container, process, or virtual machine (VM) that handles some part of the parallel processing of a pipeline. Each worker node has its own independent copy of state. A Beam runner might serialize elements between machines for communication purposes and for other reasons such as persistence.

To learn more, see:

* [Execution model](/documentation/runtime/model/)
