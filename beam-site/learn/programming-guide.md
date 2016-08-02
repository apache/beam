---
layout: default
title: "Beam Programming Guide"
permalink: /learn/programming-guide/
redirect_from: /docs/learn/programming-guide/
---

# Apache Beam Programming Guide

The **Beam Programming Guide** is intended for Beam users who want to use the Beam SDKs to create data processing pipelines. It provides guidance for using the Beam SDK classes to build and test your pipeline. It is not intended as an exhaustive reference, but as a language-agnostic, high-level guide to programmatically building your Beam pipeline. As the programming guide is filled out, the text will include code samples in multiple languages to help illustrate how to implement Beam concepts in your programs.

## Contents

* [Overview](#overview)
* [Creating the Pipeline](#pipeline)
* [Working with PCollections](#pcollection)
  * [Creating a PCollection](#pccreate)
  * [PCollection Characteristics](#pccharacteristics)
    * [Element Type](#pcelementtype)
    * [Immutability](#pcimmutability)
    * [Random Access](#pcrandomaccess)
    * [Size and Boundedness](#pcsizebound)
    * [Element Timestamps](#pctimestamps)
* [Applying Transforms](#transforms)
  * [Using ParDo](#transforms-pardo)
  * [Using GroupByKey](#transforms-gbk)
  * [Using Combine](#transforms-combine)
  * [General Requirements for Writing User Code for Beam Transforms](#transforms-usercodereqs)
  * [Side Inputs and Side Outputs](#transforms-sideio)
* [I/O](#io)
* [Running the Pipeline](#running)
* [Data Encoding and Type Safety](#coders)
* [Working with Windowing](#windowing)
* [Working with Triggers](#triggers)

## <a name="overview"></a>Overview

To use Beam, you need to first create a driver program using the classes in one of the Beam SDKs. Your driver program *defines* your pipeline, including all of the inputs, transforms, and outputs; it also sets execution options for your pipeline (typically passed in using command-line options). These include the Pipeline Runner, which, in turn, determines what back-end your pipeline will run on.

The Beam SDKs provide a number of abstractions that simplify the mechanics of large-scale distributed data processing. The same Beam abstractions work with both batch and streaming data sources. When you create your Beam pipeline, you can think about your data processing task in terms of these abstractions. They include:

* `Pipeline`: A `Pipeline` encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data. All Beam driver programs must create a `Pipeline`. When you create the `Pipeline`, you must also specify the execution options that tell the `Pipeline` where and how to run.

* `PCollection`: A `PCollection` represents a distributed data set that your Beam pipeline operates on. The data set can be *bounded*, meaning it comes from a fixed source like a file, or *unbounded*, meaning it comes from a continuously updating source via a subscription or other mechanism. Your pipeline typically creates an initial `PCollection` by reading data from an external data source, but you can also create a `PCollection` from in-memory data within your driver program. From there, `PCollection`s are the inputs and outputs for each step in your pipeline.

* `Transform`: A `Transform` represents a data processing operation, or a step, in your pipeline. Every `Transform` takes one or more `PCollection` objects as input, perfroms a processing function that you provide on the elements of that `PCollection`, and produces one or more output `PCollection` objects. 

* I/O `Source` and `Sink`: Beam provides `Source` and `Sink` APIs to represent reading and writing data, respectively. `Source` encapsulates the code necessary to read data into your Beam pipeline from some external source, such as cloud file storage or a subscription to a streaming data source. `Sink` likewise encapsulates the code necessary to write the elements of a `PCollection` to an external data sink.

A typical Beam driver program works as follows:

* Create a `Pipeline` object and set the pipeline execution options, including the Pipeline Runner.
* Create an initial `PCollection` for pipeline data, either using the `Source` API to read data from an external source, or using a `Create` transform to build a `PCollection` from in-memory data.
* Apply **Transforms** to each `PCollection`. Transforms can change, filter, group, analyze, or otherwise process the elements in a `PCollection`. A transform creates a new output `PCollection` *without consuming the input collection*. A typical pipeline applies subsequent transforms to the each new output `PCollection` in turn until processing is complete.
* Output the final, transformed `PCollection`(s), typically using the `Sink` API to write data to an external source.
* **Run** the pipeline using the designated Pipeline Runner.

When you run your Beam driver program, the Pipeline Runner that you designate constructs a **workflow graph** of your pipeline based on the `PCollection` objects you've created and transforms that you've applied. That graph is then executed using the appropriate distributed processing back-end, becoming an asynchronous "job" (or equivalent) on that back-end.

## <a name="pipeline"></a>Creating the Pipeline

The `Pipeline` abstraction encapsulates all the data and steps in your data processing task. Your Beam driver program typically starts by constructing a [Pipeline](https://github.com/apache/incubator-beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/Pipeline.java) object, and then using that object as the basis for creating the pipeline's data sets as `PCollection`s and its operations as `Transform`s.

To use Beam, your driver program must first create an instance of the Beam SDK class `Pipeline` (typically in the `main()` function). When you create your `Pipeline`, you'll also need to set some **configuration options**. You can set your pipeline's configuration options programatically, but it's often easier to set the options ahead of time (or read them from the command line) and pass them to the `Pipeline` object when you create the object.

The pipeline configuration options determine, among other things, the `PipelineRunner` that determines where the pipeline gets executed: locally, or using a distributed back-end of your choice. Depending on where your pipeline gets executed and what your specifed Runner requires, the options can also help you specify other aspects of execution.

To set your pipeline's configuration options and create the pipeline, create an object of type [PipelineOptions](https://github.com/apache/incubator-beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/options/PipelineOptions.java) and pass it to `Pipeline.Create()`. The most common way to do this is by parsing arguments from the command-line:

```java
public static void main(String[] args) {
   // Will parse the arguments passed into the application and construct a PipelineOptions
   // Note that --help will print registered options, and --help=PipelineOptionsClassName
   // will print out usage for the specific class.
   PipelineOptions options =
       PipelineOptionsFactory.fromArgs(args).create();

   Pipeline p = Pipeline.create(options);
```

The Beam SDKs contain various subclasses of `PipelineOptions` that correspond to different Runners. For example, `DirectPipelineOptions` contains options for the Direct (local) pipeline runner, while `DataflowPipelineOptions` contains options for using the runner for Google Cloud Dataflow. You can also define your own custom `PipelineOptions` by creating an interface that extends the Beam SDKs' `PipelineOptions` class.

## <a name="pcollection"></a>Working with PCollections

The [PCollection](https://github.com/apache/incubator-beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/values/PCollection.java) abstraction represents a potentially distributed, multi-element data set. You can think of a `PCollection` as "pipeline" data; Beam transforms use `PCollection` objects as inputs and outputs. As such, if you want to work with data in your pipeline, it must be in the form of a `PCollection`.

After you've created your `Pipeline`, you'll need to begin by creating at least one `PCollection` in some form. The `PCollection` you create serves as the input for the first operation in your pipeline.

### <a name="pccreate"></a>Creating a PCollection

You create a `PCollection` by either reading data from an external source using Beam's [Source API](#io), or you can create a `PCollection` of data stored in an in-memory collection class in your driver program. The former is typically how a production pipeline would ingest data; Beam's Source APIs contain adapters to help you read from external sources like large cloud-based files, databases, or subscription services. The latter is primarily useful for testing and debugging purposes.

#### Reading from an External Source

To read from an external source, you use one of the [Beam-provided I/O adapters](#io). The adapters vary in their exact usage, but all of them from some external data source and return a `PCollection` whose elements represent the data records in that source. 

Each data source adapter has a `Read` transform; to read, you must apply that transform to the `Pipeline` object itself. `TextIO.Read`, for example, reads from an external text file and returns a `PCollection` whose elements are of type `String`; each `String` represents one line from the text file. Here's how you would apply `TextIO.Read` to your `Pipeline` to create a `PCollection`:

```java
public static void main(String[] args) {
    // Create the pipeline.
    PipelineOptions options = 
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    PCollection<String> lines = p.apply(
      TextIO.Read.named("ReadMyFile").from("gs://some/inputData.txt"));
}
```

See the [section on I/O](#io) to learn more about how to read from the various data sources supported by the Beam SDK.

#### Creating a PCollection from In-Memory Data

To create a `PCollection` from an in-memory Java `Collection`, you use the Beam-provided `Create` transform. Much like a data adapter's `Read`, you apply `Create` sirectly to your `Pipeline` object itself. 

As parameters, `Create` accepts the Java `Collection` and a `Coder` object. The `Coder` specifies how the elements in the `Collection` should be [encoded](#pcelementtype).

The following example code shows how to create a `PCollection` from an in-memory Java `List`:

```java
public static void main(String[] args) {
    // Create a Java Collection, in this case a List of Strings.
    static final List<String> LINES = Arrays.asList(
      "To be, or not to be: that is the question: ",
      "Whether 'tis nobler in the mind to suffer ",
      "The slings and arrows of outrageous fortune, ",
      "Or to take arms against a sea of troubles, ");

    // Create the pipeline.
    PipelineOptions options = 
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // Apply Create, passing the list and the coder, to create the PCollection.
    p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
}
```
### <a name="pccharacteristics">PCollection Characteristics

A `PCollection` is owned by the specific `Pipeline` object for which it is created; multiple pipelines cannot share a `PCollection`. In some respects, a `PCollection` functions like a collection class. However, a `PCollection` can differ in a few key ways:

#### <a name="pcelementtype"></a>Element Type

The elements of a `PCollection` may be of any type, but must all be of the same type. However, to support distributed processing, Beam needs to be able to encode each individual element as a byte string (so elements can be passed around to distributed workers). The Beam SDKs provide a data encoding mechanism that includes built-in encoding for commonly-used types as well as support for specifying custom encodings as needed.

#### <a name="pcimmutability"></a>Immutability

A `PCollection` is immutable. Once created, you cannot add, remove, or change individual elements. A Beam Transform might process each element of a `PCollection` and generate new pipeline data (as a new `PCollection`), *but it does not consume or modify the original input collection*.

#### <a name="pcrandomaccess"></a>Random Access

A `PCollection` does not support random access to individual elements. Instead, Beam Transforms consider every element in a `PCollection` individually.

#### <a name="pcsizebound"></a>Size and Boundedness

A `PCollection` is a large, immutable "bag" of elements. There is no upper limit on how many elements a `PCollection` can contain; any given `PCollection` might fit in memory on a single machine, or it might represent a very large distributed data set backed by a persistent data store.

A `PCollection` can be either **bounded** or **unbounded** in size. A **bounded** `PCollection` represents a data set of a known, fixed size, while an **unbounded** `PCollection` represents a data set of unlimited size. Whether a `PCollection` is bounded or unbounded depends on the source of the data set that it represents. Reading from a batch data source, such as a file or a database, creates a bounded `PCollection`. Reading from a streaming or continously-updating data source, such as Pub/Sub or Kafka, creates an unbounded `PCollection` (unless you explicitly tell it not to).

The bounded (or unbounded) nature The bounded (or unbounded) nature of your `PCollection` affects how Beam processes your data. A bounded `PCollection` can be processed using a batch job, which might read the entire data set once, and perform processing in a job of finite length. An unbounded `PCollection` must be processed using a streaming job that runs continuously, as the entire collection can never be available for processing at any one time.

When performing an operation that groups elements in an unbounded `PCollection`, Beam requires a concept called **Windowing** to divide a continuously updating data set into logical windows of finite size.  Beam processes each window as a bundle, and processing continues as the data set is generated. These logical windows are determined by some characteristic associated with a data element, such as a **timestamp**.

#### <a name="pctimestamps"></a>Element Timestamps

Each element in a `PCollection` has an associated intrinsic **timestamp**. The timestamp for each element is initially assigned by the [Source](#io) that creates the `PCollection`. Sources that create an unbounded `PCollection` often assign each new element a timestamp that corresponds to when the element was read or added.

> **Note**: Sources that create a bounded `PCollection` for a fixed data set also automatically assign timestamps, but the most common behavior is to assign every element the same timestamp (`Long.MIN_VALUE`).

Timestamps are useful for a `PCollection` that contains elements with an inherent notion of time. If your pipeline is reading a stream of events, like Tweets or other social media messages, each element might use the time the event was posted as the element timestamp.

You can manually assign timestamps to the elements of a `PCollection` if the source doesn't do it for you. You'll want to do this if the elements have an inherent timestamp, but the timestamp is somewhere in the structure of the element itself (such as a "time" field in a server log entry). Beam has [Transforms](#transform) that take a `PCollection` as input and output an identical `PCollection` with timestamps attached; see [Assigning Timestamps](#windowing) for more information on how to do so.

## <a name="transforms"></a>Applying Transforms

In the Beam SDKs, **transforms** are the operations in your pipeline. A transform takes a `PCollection` (or more than one `PCollection`) as input, performs an operation that you specify on each element in that collection, and produces a new output `PCollection`. To invoke a transform, you must **apply** it to the input `PCollection`.

In Beam SDK for Java, each transform has a generic `apply` method. In the Beam SDK for Python, you use the pipe operator (`|`) to apply a transform. Invoking multiple Beam transforms is similar to *method chaining*, but with one slight difference: You apply the transform to the input `PCollection`, passing the transform itself as an argument, and the operation returns the output `PCollection`. This takes the general form:

```java
[Output PCollection] = [Input PCollection].apply([Transform])
```

Because Beam uses a generic `apply` method for `PCollection`, you can both chain transforms sequentially and also apply transforms that contain other transforms nested within (called **composite transforms** in the Beam SDKs).

How you apply your pipeline's transforms determines the structure of your pipeline. The best way to think of your pipeline is as a directed acyclic graph, where the nodes are `PCollection`s and the edges are transforms. For example, you can chain transforms to create a sequential pipeline, like this one:

```java
[Final Output PCollection] = [Initial Input PCollection].apply([First Transform])
							.apply([Second Transform])
							.apply([Third Transform])
```

The resulting workflow graph of the above pipeline looks like this:

[Sequential Graph Graphic]

However, note that a transform *does not consume or otherwise alter* the input collection--remember that a `PCollection` is immutable by definition. This means that you can apply multiple transforms to the same input `PCollection` to create a branching pipeline, like so:

```java
[Output PCollection 1] = [Input PCollection].apply([Transform 1])
[Output PCollection 2] = [Input PCollection].apply([Transform 2])
```

The resulting workflow graph from the branching pipeline abouve looks like this:

[Branching Graph Graphic]

You can also build your own [composite transforms](#transforms-composite) that nest multiple sub-steps inside a single, larger transform. Composite transforms are particularly useful for building a reusable sequence of simple steps that get used in a lot of different places.

### Transforms in the Beam SDK

The transforms in the Beam SDKs provide a generic **processing framework**, where you provide processing logic in the form of a function object (colloquially referred to as "user code"). The user code gets applied to the elements of the input `PCollection`. Instances of your user code might then be executed in parallel by many different workers across a cluster, depending on the pipeline runner and back-end that you choose to execute your Beam pipeline. The user code running on each worker generates the output elements that are ultimately added to the final output `PCollection` that the transform produces.

### Core Beam Transforms

Beam provides the following transforms, each of which represents a different processing paradigm:

* `ParDo`
* `GroupByKey`
* `Combine`
* `Flatten`

#### <a name="transforms-pardo"></a>ParDo

`ParDo` is a Beam transform for generic parallel processing. The `ParDo` processing paradigm is similar to the "Map" phase of a Map/Shuffle/Reduce-style algorithm: a `ParDo` transform considers each element in the input `PCollection`, performs some processing function (your user code) on that element, and emits zero, one, or multiple elements to an output `PCollection`.

`ParDo` is useful for a variety of common data processing operations, including:

* **Filtering a data set.** You can use `ParDo` to consider each element in a `PCollection` and either output that element to a new collection, or discard it.
* **Formatting or type-converting each element in a data set.** If your input `PCollection` contains elements that are of a different type or format than you want, you can use `ParDo` to perform a conversion on each element and output the result to a new `PCollection`.
* **Extracting parts of each element in a data set.** If you have a `PCollection` of records with multiple fields, for example, you can use a `ParDo` to parse out just the fields you want to consider into a new `PCollection`.
* **Performing computations on each element in a data set.** You can use `ParDo` to perform simple or complex computations on every element, or certain elements, of a `PCollection` and output the results as a new `PCollection`.

In such roles, `ParDo` is a common intermediate step in a pipeline. You might use it to extract certain fields from a set of raw input records, or convert raw input into a different format; you might also use `ParDo` to convert processed data into a format suitable for output, like database table rows or printable strings.

When you apply a `ParDo` transform, you'll need to provide user code in the form of a `DoFn` object. `DoFn` is a Beam SDK class that defines a distribured processing function.

> When you create a subclass of `DoFn`, note that your subclass should adhere to the [General Requirements for Writing User Code for Beam Transforms](#transforms-usercodereqs).

##### Applying ParDo

Like all Beam transforms, you apply `ParDo` by calling the `apply` method on the input `PCollection` and passing `ParDo` as an argument, as shown in the following example code:

```java
// The input PCollection of Strings.
PCollection<String> words = ...;

// The DoFn to perform on each element in the input PCollection.
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }

// Apply a ParDo to the PCollection "words" to compute lengths for each word.
PCollection<Integer> wordLengths = words.apply(
    ParDo
    .of(new ComputeWordLengthFn()));        // The DoFn to perform on each element, which
                                            // we define above.
```

In the example, our input `PCollection` contains `String` values. We apply a `ParDo` transform that specifies a function (`ComputeWordLengthFn`) to compute the length of each string, and outputs the result to a new `PCollection` of `Integer` values that stores the length of each word.

##### Creating a DoFn

The `DoFn` object that you pass to `ParDo` contains the processing logic that gets applied to the elements in the input collection. When you use Beam, often the most important pieces of code you'll write are these `DoFn`s--they're what define your pipeline's exact data processing tasks.

> **Note:** When you create your `DoFn`, be mindful of the [General Requirements for Writing User Code for Beam Transforms](#transforms-usercodereqs) and ensure that your code follows them.

A `DoFn` processes one element at a time from the input `PCollection`. When you create a subclass of `DoFn`, you'll need to provide type paraemters that match the types of the input and output elements. If your `DoFn` processes incoming `String` elements and produces `Integer` elements for the output collection (like our previous example, `ComputeWordLengthFn`), your class declaration would look like this:

```java
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }
```

Inside your `DoFn` subclass, you'll need to override the method `processElement`, where you provide the actual processing logic. You don't need to manually extract the elements from the input collection; the Beam SDKs handle that for you. Your override of `processElement` should accept an object of type `ProcessContext`. The `ProcessContext` object gives you access to an input element and a method for emitting an output element:

```java
static class ComputeWordLengthFn extends DoFn<String, Integer> {
  @Override
  public void processElement(ProcessContext c) {
    // Get the input element from ProcessContext.
    String word = c.element();
    // Use ProcessContext.output to emit the output element.
    c.output(word.length());
  }
}
```

> **Note:** If the elements in your input `PCollection` are key/value pairs, you can access the key or value by using `ProcessContext.element().getKey()` or `ProcessContext.element().getValue()`, respectively.

A given `DoFn` instance generally gets invoked one or more times to process some arbitrary bundle of elements. However, Beam doesn't guarantee an exact number of invocations; it may be invoked multiple times on a given worker node to account for failures and retries. As such, you can cache information across multiple calls to `processElement`, but if you do so, make sure the implementation **does not depend on the number of invocations**.

When you override `processElement`, you'll need to meet some immutability requirements to ensure that Beam and the processing back-end can safely serialize and cache the values in your pipeline. Your method should meet the following requirements:

* You should not in any way modify an element returned by `ProcessContext.element()` or `ProcessContext.sideInput()` (the incoming elements from the input collection).
* Once you output a value using `ProcessContext.output()` or `ProcessContext.sideOutput()`, you should not modify that value in any way.

##### Lightweight DoFns and Other Abstractions

If your function is relatively straightforward, you can simply your use of `ParDo` by providing a lightweight `DoFn` in-line. In Java, you can specify your `DoFn` as an anonymous inner class instance, and in Python you can use a `Callable`.

Here's the previous example, `ParDo` with `ComputeLengthWordsFn`, with the `DoFn` specified as an anonymous inner class instance:

```java
// The input PCollection.
PCollection<String> words = ...;

// Apply a ParDo with an anonymous DoFn to the PCollection words.
// Save the result as the PCollection wordLengths.
PCollection<Integer> wordLengths = words.apply(
  ParDo
    .named("ComputeWordLengths")            // the transform name
    .of(new DoFn<String, Integer>() {       // a DoFn as an anonymous inner class instance
      @Override
      public void processElement(ProcessContext c) {
        c.output(c.element().length());
      }
    }));
```

If your `ParDo` performs a one-to-one mapping of input elements to output elements--that is, for each input element, it applies a function that produces *exactly one* output element, you can use the higher-level `MapElements` transform. `MapElements` can accept an anonymous Java 8 lambda function for additional brevity.

Here's the previous example using `MapElements`:

```java
// The input PCollection.
PCollection&lt;String&gt; words = ...;

// Apply a MapElements with an anonymous lambda function to the PCollection words.
// Save the result as the PCollection wordLengths.
PCollection&lt;Integer&gt; wordLengths = words.apply(
  MapElements.via((String word) -&gt; word.length())
      .withOutputType(new TypeDescriptor&lt;Integer&gt;() {});
```

> **Note:** You can use Java 8 lambda functions with several other Beam transforms, including `Filter`, `FlatMapElements`, and `Partition`.

#### <a name="transforms-gbk"></a>Using GroupByKey

`GroupByKey` is a Beam transform for processing collections of key/value pairs. It's a parallel reduction operation, analagous to the Shuffle phase of a Map/Shuffle/Reduce-style algorithm. The input to `GroupByKey` is a collection of key/value pairs that represents a *multimap*, where the collection contains multiple pairs that have the same key, but different values. Given such a collection, you use `GroupByKey` to collect all of the values associated with each unique key.

`GroupByKey` is a good way to aggregate data that has something in common. For example, if you have a collection that stores records of customer orders, you might want to group together all the orders from the same postal code (wherein the "key" of the key/value pair is the postal code field, and the "value" is the remainder of the record).

Let's examine the mechanics of `GroupByKey` with a simple xample case, where our data set consists of words from a text file and the line number on which they appear. We want to group together all the line numbers (values) that share the same word (key), letting us see all the places in the text where a particular word appears.

Our input is a `PCollection` of key/value pairs where each word is a key, and the value is a line number in the file where the word appears. Here's a list of the key/value pairs in the input collection:

```
cat, 1
dog, 5
and, 1
jump, 3
tree, 2
cat, 5
dog, 2
and, 2
cat, 9
and, 6
...
```

`GroupByKey` gathers up all the values with the same key and outputs a new pair consisting of the unique key and a collection of all of the values that were associated with that key in the input collection. If we apply `GroupByKey` to our input collection above, the output collection would look like this:

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
...
```

Thus, `GroupByKey` represents a transform from a multimap (multiple keys to individual values) to a uni-map (unique keys to collections of values).

> **A Note on Key/Value Pairs:** Beam represents key/value pairs slightly differently depending on the language and SDK you're using. In the Beam SDK for Java, you represent a key/value pair with an object of type `KV<K, V>`. In Python, you represent key/value pairs with 2-tuples.
     

#### <a name="transforms-combine"></a>Using Combine

#### <a name="transforms-usercodereqs"></a>General Requirements for Writing User Code for Beam Transforms

When you build user code for a Beam transform, you should keep in mind the distributed nature of execution. For example, there might be many copies of your function running on a lot of different machines in parallel, and those copies function independently, without communicating or sharing state with any of the other copies. Depending on the Pipeline Runner and processing back-end you choose for your pipeline, each copy of your user code function may be retried or run multiple times. As such, you should be cautious about including things like state dependency in your user code.

In general, your user code must fulfill at least these requirements:

* Your function object must be **serializable**.
* Your function object must be **thread-compatible**, and be aware that *the Beam SDKs are not thread-safe*.

In addition, it's recommended that you make your function object **idempotent**.

> **Note:** These requirements apply to subclasses of `DoFn` (a function object used with the [ParDo](#transforms-pardo) transform), `CombineFn` (a function object used with the [Combine](#transforms-combine) transform), and `WindowFn` (a function object used with the [Window](#windowing) transform).

##### Serializability

Any function object you provide to a transform must be **fully serializable**. This is because a copy of the function needs to be serialized and transmitted to a remote worker in your processing cluster. The base classes for user code, such as `DoFn`, `CombineFn`, and `WindowFn`, already implement `Serializable`; however, your subclass must not add any non-serializable members.

Some other serializability factors you should keep in mind are:

* Transient fields in your function object are *not* transmitted to worker instances, because they are not automatically serialized.
* Avoid loading a field with a large amount of data before serialization.
* Individual instances of your function object cannot share data.
* Mutating a function object after it gets applied will have no effect.
* Take care when declaring your function object inline by using an anonymous inner class instance. In a non-static context, your inner class instance will implicitly contain a pointer to the enclosing class and that class' state. That enclosing class will also be serialized, and thus the same considerations that apply to the function object itself also apply to this outer class.

##### Thread-Compatibility

Your function object should be thread-compatible. Each instance of your function object is accessed by a single thread on a worker instance, unless you explicitly create your own threads. Note, however, that **the Beam SDKs are not thread-safe**. If you create your own threads in your user code, you must provide your own synchronization. Note that static members in your function object are not passed to worker instances and that multiple instances of your function may be accessed from different threads.

##### Idempotence

It's recommended that you make your function object idempotent--that is, that it can be repeated or retried as often as necessary without causing unintended side effects. The Beam model provides no guarantees as to the number of times your user code might be invoked or retried; as such, keeping your function object idempotent keeps your pipeline's output deterministic, and your transforms' behavior more predictable and easier to debug.

<a name="io"></a>
<a name="running"></a>
<a name="transforms-composite"></a>
<a name="transforms-sideio"></a>
<a name="coders"></a>
<a name="windowing"></a>
<a name="triggers"></a>

> **Note:** This guide is still in progress. There is an open issue to finish the guide ([BEAM-193](https://issues.apache.org/jira/browse/BEAM-193))
