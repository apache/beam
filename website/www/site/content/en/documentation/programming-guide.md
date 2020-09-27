---
title: "Beam Programming Guide"
aliases:
  - /learn/programming-guide/
  - /docs/learn/programming-guide/
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

# Apache Beam Programming Guide

The **Beam Programming Guide** is intended for Beam users who want to use the
Beam SDKs to create data processing pipelines. It provides guidance for using
the Beam SDK classes to build and test your pipeline. It is not intended as an
exhaustive reference, but as a language-agnostic, high-level guide to
programmatically building your Beam pipeline. As the programming guide is filled
out, the text will include code samples in multiple languages to help illustrate
how to implement Beam concepts in your pipelines.

{{< language-switcher java py >}}

{{< paragraph class="language-py" >}}
The Python SDK supports Python 2.7, 3.5, 3.6, and 3.7. New Python SDK releases will stop supporting Python 2.7 in 2020 ([BEAM-8371](https://issues.apache.org/jira/browse/BEAM-8371)). For best results, use Beam with Python 3.
{{< /paragraph >}}

## 1. Overview {#overview}

To use Beam, you need to first create a driver program using the classes in one
of the Beam SDKs. Your driver program *defines* your pipeline, including all of
the inputs, transforms, and outputs; it also sets execution options for your
pipeline (typically passed in using command-line options). These include the
Pipeline Runner, which, in turn, determines what back-end your pipeline will run
on.

The Beam SDKs provide a number of abstractions that simplify the mechanics of
large-scale distributed data processing. The same Beam abstractions work with
both batch and streaming data sources. When you create your Beam pipeline, you
can think about your data processing task in terms of these abstractions. They
include:

* `Pipeline`: A `Pipeline` encapsulates your entire data processing task, from
  start to finish. This includes reading input data, transforming that data, and
  writing output data. All Beam driver programs must create a `Pipeline`. When
  you create the `Pipeline`, you must also specify the execution options that
  tell the `Pipeline` where and how to run.

* `PCollection`: A `PCollection` represents a distributed data set that your
  Beam pipeline operates on. The data set can be *bounded*, meaning it comes
  from a fixed source like a file, or *unbounded*, meaning it comes from a
  continuously updating source via a subscription or other mechanism. Your
  pipeline typically creates an initial `PCollection` by reading data from an
  external data source, but you can also create a `PCollection` from in-memory
  data within your driver program. From there, `PCollection`s are the inputs and
  outputs for each step in your pipeline.

* `PTransform`: A `PTransform` represents a data processing operation, or a step,
  in your pipeline. Every `PTransform` takes one or more `PCollection` objects as
  input, performs a processing function that you provide on the elements of that
  `PCollection`, and produces zero or more output `PCollection` objects.

* I/O transforms: Beam comes with a number of "IOs" - library `PTransform`s that
  read or write data to various external storage systems.

A typical Beam driver program works as follows:

* **Create** a `Pipeline` object and set the pipeline execution options, including
  the Pipeline Runner.
* Create an initial `PCollection` for pipeline data, either using the IOs
  to read data from an external storage system, or using a `Create` transform to
  build a `PCollection` from in-memory data.
* **Apply** `PTransform`s to each `PCollection`. Transforms can change, filter,
  group, analyze, or otherwise process the elements in a `PCollection`. A
  transform creates a new output `PCollection` *without modifying the input
  collection*. A typical pipeline applies subsequent transforms to each new
  output `PCollection` in turn until processing is complete. However, note that
  a pipeline does not have to be a single straight line of transforms applied
  one after another: think of `PCollection`s as variables and `PTransform`s as
  functions applied to these variables: the shape of the pipeline can be an
  arbitrarily complex processing graph.
* Use IOs to write the final, transformed `PCollection`(s) to an external source.
* **Run** the pipeline using the designated Pipeline Runner.

When you run your Beam driver program, the Pipeline Runner that you designate
constructs a **workflow graph** of your pipeline based on the `PCollection`
objects you've created and transforms that you've applied. That graph is then
executed using the appropriate distributed processing back-end, becoming an
asynchronous "job" (or equivalent) on that back-end.

## 2. Creating a pipeline {#creating-a-pipeline}

The `Pipeline` abstraction encapsulates all the data and steps in your data
processing task. Your Beam driver program typically starts by constructing a
<span class="language-java">[Pipeline](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/Pipeline.html)</span>
<span class="language-py">[Pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/pipeline.py)</span>
object, and then using that object as the basis for creating the pipeline's data
sets as `PCollection`s and its operations as `Transform`s.

To use Beam, your driver program must first create an instance of the Beam SDK
class `Pipeline` (typically in the `main()` function). When you create your
`Pipeline`, you'll also need to set some **configuration options**. You can set
your pipeline's configuration options programmatically, but it's often easier to
set the options ahead of time (or read them from the command line) and pass them
to the `Pipeline` object when you create the object.

{{< highlight java >}}
// Start by defining the options for the pipeline.
PipelineOptions options = PipelineOptionsFactory.create();

// Then create the pipeline.
Pipeline p = Pipeline.create(options);
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" pipelines_constructing_creating >}}
{{< /highlight >}}

{{< highlight go >}}
// In order to start creating the pipeline for execution, a Pipeline object and a Scope object are needed.
p, s := beam.NewPipelineWithRoot()
{{< /highlight >}}

### 2.1. Configuring pipeline options {#configuring-pipeline-options}

Use the pipeline options to configure different aspects of your pipeline, such
as the pipeline runner that will execute your pipeline and any runner-specific
configuration required by the chosen runner. Your pipeline options will
potentially include information such as your project ID or a location for
storing files.

When you run the pipeline on a runner of your choice, a copy of the
PipelineOptions will be available to your code. For example, if you add a PipelineOptions parameter
to a DoFn's `@ProcessElement` method, it will be populated by the system.

#### 2.1.1. Setting PipelineOptions from command-line arguments {#pipeline-options-cli}

While you can configure your pipeline by creating a `PipelineOptions` object and
setting the fields directly, the Beam SDKs include a command-line parser that
you can use to set fields in `PipelineOptions` using command-line arguments.

To read options from the command-line, construct your `PipelineOptions` object
as demonstrated in the following example code:

{{< highlight java >}}
PipelineOptions options =
    PipelineOptionsFactory.fromArgs(args).withValidation().create();
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" pipelines_constructing_creating >}}
{{< /highlight >}}

{{< highlight go >}}
// If beamx or Go flags are used, flags must be parsed first.
flag.Parse()
{{< /highlight >}}

This interprets command-line arguments that follow the format:

```
--<option>=<value>
```

> **Note:** Appending the method `.withValidation` will check for required
> command-line arguments and validate argument values.

Building your `PipelineOptions` this way lets you specify any of the options as
a command-line argument.

> **Note:** The [WordCount example pipeline](/get-started/wordcount-example)
> demonstrates how to set pipeline options at runtime by using command-line
> options.

#### 2.1.2. Creating custom options {#creating-custom-options}

You can add your own custom options in addition to the standard
`PipelineOptions`. To add your own options, define an interface with getter and
setter methods for each option, as in the following example for
adding `input` and `output` custom options:

{{< highlight java >}}
public interface MyOptions extends PipelineOptions {
    String getInput();
    void setInput(String input);

    String getOutput();
    void setOutput(String output);
}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" pipeline_options_define_custom >}}
{{< /highlight >}}

{{< highlight go >}}
var (
  input = flag.String("input", "", "")
  output = flag.String("output", "", "")
)
{{< /highlight >}}

You can also specify a description, which appears when a user passes `--help` as
a command-line argument, and a default value.

You set the description and default value using annotations, as follows:

{{< highlight java >}}
public interface MyOptions extends PipelineOptions {
    @Description("Input for the pipeline")
    @Default.String("gs://my-bucket/input")
    String getInput();
    void setInput(String input);

    @Description("Output for the pipeline")
    @Default.String("gs://my-bucket/output")
    String getOutput();
    void setOutput(String output);
}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" pipeline_options_define_custom_with_help_and_default >}}
{{< /highlight >}}

{{< highlight go >}}
var (
  input = flag.String("input", "gs://my-bucket/input", "Input for the pipeline")
  output = flag.String("output", "gs://my-bucket/output", "Output for the pipeline")
)
{{< /highlight >}}

{{< paragraph class="language-java" >}}
It's recommended that you register your interface with `PipelineOptionsFactory`
and then pass the interface when creating the `PipelineOptions` object. When you
register your interface with `PipelineOptionsFactory`, the `--help` can find
your custom options interface and add it to the output of the `--help` command.
`PipelineOptionsFactory` will also validate that your custom options are
compatible with all other registered options.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
The following example code shows how to register your custom options interface
with `PipelineOptionsFactory`:
{{< /paragraph >}}

{{< highlight java >}}
PipelineOptionsFactory.register(MyOptions.class);
MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                .withValidation()
                                                .as(MyOptions.class);
{{< /highlight >}}

Now your pipeline can accept `--input=value` and `--output=value` as command-line arguments.

## 3. PCollections {#pcollections}

The <span class="language-java">[PCollection](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/values/PCollection.html)</span>
<span class="language-py">`PCollection`</span> abstraction represents a
potentially distributed, multi-element data set. You can think of a
`PCollection` as "pipeline" data; Beam transforms use `PCollection` objects as
inputs and outputs. As such, if you want to work with data in your pipeline, it
must be in the form of a `PCollection`.

After you've created your `Pipeline`, you'll need to begin by creating at least
one `PCollection` in some form. The `PCollection` you create serves as the input
for the first operation in your pipeline.

### 3.1. Creating a PCollection {#creating-a-pcollection}

You create a `PCollection` by either reading data from an external source using
Beam's [Source API](#pipeline-io), or you can create a `PCollection` of data
stored in an in-memory collection class in your driver program. The former is
typically how a production pipeline would ingest data; Beam's Source APIs
contain adapters to help you read from external sources like large cloud-based
files, databases, or subscription services. The latter is primarily useful for
testing and debugging purposes.

#### 3.1.1. Reading from an external source {#reading-external-source}

To read from an external source, you use one of the [Beam-provided I/O
adapters](#pipeline-io). The adapters vary in their exact usage, but all of them
read from some external data source and return a `PCollection` whose elements
represent the data records in that source.

Each data source adapter has a `Read` transform; to read, you must apply that
transform to the `Pipeline` object itself.
<span class="language-java">`TextIO.Read`</span>
<span class="language-py">`io.TextFileSource`</span>, for example, reads from an
external text file and returns a `PCollection` whose elements are of type
`String`, each `String` represents one line from the text file. Here's how you
would apply <span class="language-java">`TextIO.Read`</span>
<span class="language-py">`io.TextFileSource`</span> to your `Pipeline` to create
a `PCollection`:

{{< highlight java >}}
public static void main(String[] args) {
    // Create the pipeline.
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // Create the PCollection 'lines' by applying a 'Read' transform.
    PCollection<String> lines = p.apply(
      "ReadMyFile", TextIO.read().from("gs://some/inputData.txt"));
}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" pipelines_constructing_reading >}}
{{< /highlight >}}

{{< highlight go >}}
lines := textio.Read(s, "gs://some/inputData.txt")
{{< /highlight >}}

See the [section on I/O](#pipeline-io) to learn more about how to read from the
various data sources supported by the Beam SDK.

#### 3.1.2. Creating a PCollection from in-memory data {#creating-pcollection-in-memory}

{{< paragraph class="language-java" >}}
To create a `PCollection` from an in-memory Java `Collection`, you use the
Beam-provided `Create` transform. Much like a data adapter's `Read`, you apply
`Create` directly to your `Pipeline` object itself.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
As parameters, `Create` accepts the Java `Collection` and a `Coder` object. The
`Coder` specifies how the elements in the `Collection` should be
[encoded](#element-type).
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
To create a `PCollection` from an in-memory `list`, you use the Beam-provided
`Create` transform. Apply this transform directly to your `Pipeline` object
itself.
{{< /paragraph >}}

The following example code shows how to create a `PCollection` from an in-memory
<span class="language-java">`List`</span><span class="language-py">`list`</span>:

{{< highlight java >}}
public static void main(String[] args) {
    // Create a Java Collection, in this case a List of Strings.
    final List<String> LINES = Arrays.asList(
      "To be, or not to be: that is the question: ",
      "Whether 'tis nobler in the mind to suffer ",
      "The slings and arrows of outrageous fortune, ",
      "Or to take arms against a sea of troubles, ");

    // Create the pipeline.
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // Apply Create, passing the list and the coder, to create the PCollection.
    p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_pcollection >}}
{{< /highlight >}}

### 3.2. PCollection characteristics {#pcollection-characteristics}

A `PCollection` is owned by the specific `Pipeline` object for which it is
created; multiple pipelines cannot share a `PCollection`. <span language="java">
In some respects, a `PCollection` functions like a `Collection` class. However,
a `PCollection` can differ in a few key ways:</span>

#### 3.2.1. Element type {#element-type}

The elements of a `PCollection` may be of any type, but must all be of the same
type. However, to support distributed processing, Beam needs to be able to
encode each individual element as a byte string (so elements can be passed
around to distributed workers). The Beam SDKs provide a data encoding mechanism
that includes built-in encoding for commonly-used types as well as support for
specifying custom encodings as needed.

#### 3.2.2. Element schema {#element-schema}

In many cases, the element type in a `PCollection` has a structure that can introspected.
Examples are JSON, Protocol Buffer, Avro, and database records. Schemas provide a way to
express types as a set of named fields, allowing for more-expressive aggregations.

#### 3.2.3. Immutability {#immutability}

A `PCollection` is immutable. Once created, you cannot add, remove, or change
individual elements. A Beam Transform might process each element of a
`PCollection` and generate new pipeline data (as a new `PCollection`), *but it
does not consume or modify the original input collection*.

#### 3.2.4. Random access {#random-access}

A `PCollection` does not support random access to individual elements. Instead,
Beam Transforms consider every element in a `PCollection` individually.

#### 3.2.5. Size and boundedness {#size-and-boundedness}

A `PCollection` is a large, immutable "bag" of elements. There is no upper limit
on how many elements a `PCollection` can contain; any given `PCollection` might
fit in memory on a single machine, or it might represent a very large
distributed data set backed by a persistent data store.

A `PCollection` can be either **bounded** or **unbounded** in size. A
**bounded** `PCollection` represents a data set of a known, fixed size, while an
**unbounded** `PCollection` represents a data set of unlimited size. Whether a
`PCollection` is bounded or unbounded depends on the source of the data set that
it represents. Reading from a batch data source, such as a file or a database,
creates a bounded `PCollection`. Reading from a streaming or
continuously-updating data source, such as Pub/Sub or Kafka, creates an unbounded
`PCollection` (unless you explicitly tell it not to).

The bounded (or unbounded) nature of your `PCollection` affects how Beam
processes your data. A bounded `PCollection` can be processed using a batch job,
which might read the entire data set once, and perform processing in a job of
finite length. An unbounded `PCollection` must be processed using a streaming
job that runs continuously, as the entire collection can never be available for
processing at any one time.

Beam uses [windowing](#windowing) to divide a continuously updating unbounded
`PCollection` into logical windows of finite size. These logical windows are
determined by some characteristic associated with a data element, such as a
**timestamp**. Aggregation transforms (such as `GroupByKey` and `Combine`) work
on a per-window basis — as the data set is generated, they process each
`PCollection` as a succession of these finite windows.


#### 3.2.6. Element timestamps {#element-timestamps}

Each element in a `PCollection` has an associated intrinsic **timestamp**. The
timestamp for each element is initially assigned by the [Source](#pipeline-io)
that creates the `PCollection`. Sources that create an unbounded `PCollection`
often assign each new element a timestamp that corresponds to when the element
was read or added.

> **Note**: Sources that create a bounded `PCollection` for a fixed data set
> also automatically assign timestamps, but the most common behavior is to
> assign every element the same timestamp (`Long.MIN_VALUE`).

Timestamps are useful for a `PCollection` that contains elements with an
inherent notion of time. If your pipeline is reading a stream of events, like
Tweets or other social media messages, each element might use the time the event
was posted as the element timestamp.

You can manually assign timestamps to the elements of a `PCollection` if the
source doesn't do it for you. You'll want to do this if the elements have an
inherent timestamp, but the timestamp is somewhere in the structure of the
element itself (such as a "time" field in a server log entry). Beam has
[Transforms](#transforms) that take a `PCollection` as input and output an
identical `PCollection` with timestamps attached; see [Adding
Timestamps](#adding-timestamps-to-a-pcollections-elements) for more information
about how to do so.

## 4. Transforms {#transforms}

Transforms are the operations in your pipeline, and provide a generic
processing framework. You provide processing logic in the form of a function
object (colloquially referred to as "user code"), and your user code is applied
to each element of an input `PCollection` (or more than one `PCollection`).
Depending on the pipeline runner and back-end that you choose, many different
workers across a cluster may execute instances of your user code in parallel.
The user code running on each worker generates the output elements that are
ultimately added to the final output `PCollection` that the transform produces.

The Beam SDKs contain a number of different transforms that you can apply to
your pipeline's `PCollection`s. These include general-purpose core transforms,
such as [ParDo](#pardo) or [Combine](#combine). There are also pre-written
[composite transforms](#composite-transforms) included in the SDKs, which
combine one or more of the core transforms in a useful processing pattern, such
as counting or combining elements in a collection. You can also define your own
more complex composite transforms to fit your pipeline's exact use case.

### 4.1. Applying transforms {#applying-transforms}

To invoke a transform, you must **apply** it to the input `PCollection`. Each
transform in the Beam SDKs has a generic `apply` method <span class="language-py">(or pipe operator `|`)</span>.
Invoking multiple Beam transforms is similar to *method chaining*, but with one
slight difference: You apply the transform to the input `PCollection`, passing
the transform itself as an argument, and the operation returns the output
`PCollection`. This takes the general form:

{{< highlight java >}}
[Output PCollection] = [Input PCollection].apply([Transform])
{{< /highlight >}}

{{< highlight py >}}
[Output PCollection] = [Input PCollection] | [Transform]
{{< /highlight >}}

Because Beam uses a generic `apply` method for `PCollection`, you can both chain
transforms sequentially and also apply transforms that contain other transforms
nested within (called [composite transforms](#composite-transforms) in the Beam
SDKs).

How you apply your pipeline's transforms determines the structure of your
pipeline. The best way to think of your pipeline is as a directed acyclic graph,
where `PTransform` nodes are subroutines that accept `PCollection` nodes as
inputs and emit `PCollection` nodes as outputs. For example, you can chain
together transforms to create a pipeline that successively modifies input data:

{{< highlight java >}}
[Final Output PCollection] = [Initial Input PCollection].apply([First Transform])
.apply([Second Transform])
.apply([Third Transform])
{{< /highlight >}}

{{< highlight py >}}
[Final Output PCollection] = ([Initial Input PCollection] | [First Transform]
              | [Second Transform]
              | [Third Transform])
{{< /highlight >}}

The graph of this pipeline looks like the following:

![This linear pipeline starts with one input collection, sequentially applies
  three transforms, and ends with one output collection.](/images/design-your-pipeline-linear.svg)

*Figure 1: A linear pipeline with three sequential transforms.*

However, note that a transform *does not consume or otherwise alter* the input
collection — remember that a `PCollection` is immutable by definition. This means
that you can apply multiple transforms to the same input `PCollection` to create
a branching pipeline, like so:

{{< highlight java >}}
[PCollection of database table rows] = [Database Table Reader].apply([Read Transform])
[PCollection of 'A' names] = [PCollection of database table rows].apply([Transform A])
[PCollection of 'B' names] = [PCollection of database table rows].apply([Transform B])
{{< /highlight >}}

{{< highlight py >}}
[PCollection of database table rows] = [Database Table Reader] | [Read Transform]
[PCollection of 'A' names] = [PCollection of database table rows] | [Transform A]
[PCollection of 'B' names] = [PCollection of database table rows] | [Transform B]
{{< /highlight >}}

The graph of this branching pipeline looks like the following:

![This pipeline applies two transforms to a single input collection. Each
  transform produces an output collection.](/images/design-your-pipeline-multiple-pcollections.svg)

*Figure 2: A branching pipeline. Two transforms are applied to a single
PCollection of database table rows.*

You can also build your own [composite transforms](#composite-transforms) that
nest multiple transforms inside a single, larger transform. Composite transforms
are particularly useful for building a reusable sequence of simple steps that
get used in a lot of different places.

### 4.2. Core Beam transforms {#core-beam-transforms}

Beam provides the following core transforms, each of which represents a different
processing paradigm:

* `ParDo`
* `GroupByKey`
* `CoGroupByKey`
* `Combine`
* `Flatten`
* `Partition`

#### 4.2.1. ParDo {#pardo}

`ParDo` is a Beam transform for generic parallel processing. The `ParDo`
processing paradigm is similar to the "Map" phase of a
[Map/Shuffle/Reduce](https://en.wikipedia.org/wiki/MapReduce)-style
algorithm: a `ParDo` transform considers each element in the input
`PCollection`, performs some processing function (your user code) on that
element, and emits zero, one, or multiple elements to an output `PCollection`.

`ParDo` is useful for a variety of common data processing operations, including:

* **Filtering a data set.** You can use `ParDo` to consider each element in a
  `PCollection` and either output that element to a new collection or discard
  it.
* **Formatting or type-converting each element in a data set.** If your input
  `PCollection` contains elements that are of a different type or format than
  you want, you can use `ParDo` to perform a conversion on each element and
  output the result to a new `PCollection`.
* **Extracting parts of each element in a data set.** If you have a
  `PCollection` of records with multiple fields, for example, you can use a
  `ParDo` to parse out just the fields you want to consider into a new
  `PCollection`.
* **Performing computations on each element in a data set.** You can use `ParDo`
  to perform simple or complex computations on every element, or certain
  elements, of a `PCollection` and output the results as a new `PCollection`.

In such roles, `ParDo` is a common intermediate step in a pipeline. You might
use it to extract certain fields from a set of raw input records, or convert raw
input into a different format; you might also use `ParDo` to convert processed
data into a format suitable for output, like database table rows or printable
strings.

When you apply a `ParDo` transform, you'll need to provide user code in the form
of a `DoFn` object. `DoFn` is a Beam SDK class that defines a distributed
processing function.

> When you create a subclass of `DoFn`, note that your subclass should adhere to
> the [Requirements for writing user code for Beam transforms](#requirements-for-writing-user-code-for-beam-transforms).

##### 4.2.1.1. Applying ParDo {#applying-pardo}

Like all Beam transforms, you apply `ParDo` by calling the `apply` method on the
input `PCollection` and passing `ParDo` as an argument, as shown in the
following example code:

{{< highlight java >}}
// The input PCollection of Strings.
PCollection<String> words = ...;

// The DoFn to perform on each element in the input PCollection.
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }

// Apply a ParDo to the PCollection "words" to compute lengths for each word.
PCollection<Integer> wordLengths = words.apply(
    ParDo
    .of(new ComputeWordLengthFn()));        // The DoFn to perform on each element, which
                                            // we define above.
{{< /highlight >}}

{{< highlight py >}}
# The input PCollection of Strings.
words = ...

# The DoFn to perform on each element in the input PCollection.
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_pardo >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_apply >}}
{{< /highlight >}}

{{< highlight go >}}
// words is the input PCollection of strings
var words beam.PCollection = ...

func computeWordLengthFn(word string) int {
      return len(word)
}

wordLengths := beam.ParDo(s, computeWordLengthFn, words)
{{< /highlight >}}

In the example, our input `PCollection` contains `String` values. We apply a
`ParDo` transform that specifies a function (`ComputeWordLengthFn`) to compute
the length of each string, and outputs the result to a new `PCollection` of
`Integer` values that stores the length of each word.

##### 4.2.1.2. Creating a DoFn

The `DoFn` object that you pass to `ParDo` contains the processing logic that
gets applied to the elements in the input collection. When you use Beam, often
the most important pieces of code you'll write are these `DoFn`s - they're what
define your pipeline's exact data processing tasks.

> **Note:** When you create your `DoFn`, be mindful of the [Requirements
> for writing user code for Beam transforms](#requirements-for-writing-user-code-for-beam-transforms)
> and ensure that your code follows them.

{{< paragraph class="language-java" >}}
A `DoFn` processes one element at a time from the input `PCollection`. When you
create a subclass of `DoFn`, you'll need to provide type parameters that match
the types of the input and output elements. If your `DoFn` processes incoming
`String` elements and produces `Integer` elements for the output collection
(like our previous example, `ComputeWordLengthFn`), your class declaration would
look like this:
{{< /paragraph >}}

{{< highlight java >}}
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Inside your `DoFn` subclass, you'll write a method annotated with
`@ProcessElement` where you provide the actual processing logic. You don't need
to manually extract the elements from the input collection; the Beam SDKs handle
that for you. Your `@ProcessElement` method should accept a parameter tagged with
`@Element`, which will be populated with the input element. In order to output
elements, the method can also take a parameter of type `OutputReceiver` which
provides a method for emitting elements. The parameter types must match the input
and output types of your `DoFn` or the framework will raise an error. Note: `@Element` and
`OutputReceiver` were introduced in Beam 2.5.0; if using an earlier release of Beam, a
`ProcessContext` parameter should be used instead.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
Inside your `DoFn` subclass, you'll write a method `process` where you provide
the actual processing logic. You don't need to manually extract the elements
from the input collection; the Beam SDKs handle that for you. Your `process` method
should accept an argument `element`, which is the input element, and return an
iterable with its output values. You can accomplish this by emitting individual
elements with `yield` statements. You can also use a `return` statement
with an iterable, like a list or a generator.
{{< /paragraph >}}

{{< highlight java >}}
static class ComputeWordLengthFn extends DoFn<String, Integer> {
  @ProcessElement
  public void processElement(@Element String word, OutputReceiver<Integer> out) {
    // Use OutputReceiver.output to emit the output element.
    out.output(word.length());
  }
}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_pardo >}}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
> **Note:** If the elements in your input `PCollection` are key/value pairs, you
> can access the key or value by using `element.getKey()` or
> `element.getValue()`, respectively.
{{< /paragraph >}}

A given `DoFn` instance generally gets invoked one or more times to process some
arbitrary bundle of elements. However, Beam doesn't guarantee an exact number of
invocations; it may be invoked multiple times on a given worker node to account
for failures and retries. As such, you can cache information across multiple
calls to your processing method, but if you do so, make sure the implementation
**does not depend on the number of invocations**.

In your processing method, you'll also need to meet some immutability
requirements to ensure that Beam and the processing back-end can safely
serialize and cache the values in your pipeline. Your method should meet the
following requirements:

{{< paragraph class="language-java" >}}
* You should not in any way modify an element returned by
  the `@Element` annotation or `ProcessContext.sideInput()` (the incoming
  elements from the input collection).
* Once you output a value using `OutputReceiver.output()` you should not modify
  that value in any way.
{{< /paragraph >}}

{{< paragraph class="language-python" >}}
* You should not in any way modify the `element` argument provided to the
  `process` method, or any side inputs.
* Once you output a value using `yield` or `return`, you should not modify
  that value in any way.
{{< /paragraph >}}

##### 4.2.1.3. Lightweight DoFns and other abstractions {#lightweight-dofns}

If your function is relatively straightforward, you can simplify your use of
`ParDo` by providing a lightweight `DoFn` in-line, as
<span class="language-java">an anonymous inner class instance</span>
<span class="language-py">a lambda function</span>.

Here's the previous example, `ParDo` with `ComputeLengthWordsFn`, with the
`DoFn` specified as
<span class="language-java">an anonymous inner class instance</span>
<span class="language-py">a lambda function</span>:

{{< highlight java >}}
// The input PCollection.
PCollection<String> words = ...;

// Apply a ParDo with an anonymous DoFn to the PCollection words.
// Save the result as the PCollection wordLengths.
PCollection<Integer> wordLengths = words.apply(
  "ComputeWordLengths",                     // the transform name
  ParDo.of(new DoFn<String, Integer>() {    // a DoFn as an anonymous inner class instance
      @ProcessElement
      public void processElement(@Element String word, OutputReceiver<Integer> out) {
        out.output(word.length());
      }
    }));
{{< /highlight >}}

{{< highlight py >}}
# The input PCollection of strings.
words = ...

# Apply a lambda function to the PCollection words.
# Save the result as the PCollection word_lengths.
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_using_flatmap >}}
{{< /highlight >}}

{{< highlight go >}}
// words is the input PCollection of strings
var words beam.PCollection = ...

lengths := beam.ParDo(s, func (word string) int {
      return len(word)
}, words)
{{< /highlight >}}

If your `ParDo` performs a one-to-one mapping of input elements to output
elements--that is, for each input element, it applies a function that produces
*exactly one* output element, you can use the higher-level
<span class="language-java">`MapElements`</span><span class="language-py">`Map`</span>
transform. <span class="language-java">`MapElements` can accept an anonymous
Java 8 lambda function for additional brevity.</span>

Here's the previous example using <span class="language-java">`MapElements`</span>
<span class="language-py">`Map`</span>:

{{< highlight java >}}
// The input PCollection.
PCollection<String> words = ...;

// Apply a MapElements with an anonymous lambda function to the PCollection words.
// Save the result as the PCollection wordLengths.
PCollection<Integer> wordLengths = words.apply(
  MapElements.into(TypeDescriptors.integers())
             .via((String word) -> word.length()));
{{< /highlight >}}

{{< highlight py >}}
# The input PCollection of string.
words = ...

# Apply a Map with a lambda function to the PCollection words.
# Save the result as the PCollection word_lengths.
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_using_map >}}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
> **Note:** You can use Java 8 lambda functions with several other Beam
> transforms, including `Filter`, `FlatMapElements`, and `Partition`.
{{< /paragraph >}}

##### 4.2.1.4. DoFn lifecycle {#dofn}
Here is a sequence diagram that shows the lifecycle of the DoFn during
 the execution of the ParDo transform. The comments give useful
 information to pipeline developers such as the constraints that
 apply to the objects or particular cases such as failover or
 instance reuse. They also give instantiation use cases.

<!-- The source for the sequence diagram can be found in the the SVG resource. -->
![This is a sequence diagram that shows the lifecycle of the DoFn](/images/dofn-sequence-diagram.svg)

#### 4.2.2. GroupByKey {#groupbykey}

`GroupByKey` is a Beam transform for processing collections of key/value pairs.
It's a parallel reduction operation, analogous to the Shuffle phase of a
Map/Shuffle/Reduce-style algorithm. The input to `GroupByKey` is a collection of
key/value pairs that represents a *multimap*, where the collection contains
multiple pairs that have the same key, but different values. Given such a
collection, you use `GroupByKey` to collect all of the values associated with
each unique key.

`GroupByKey` is a good way to aggregate data that has something in common. For
example, if you have a collection that stores records of customer orders, you
might want to group together all the orders from the same postal code (wherein
the "key" of the key/value pair is the postal code field, and the "value" is the
remainder of the record).

Let's examine the mechanics of `GroupByKey` with a simple example case, where
our data set consists of words from a text file and the line number on which
they appear. We want to group together all the line numbers (values) that share
the same word (key), letting us see all the places in the text where a
particular word appears.

Our input is a `PCollection` of key/value pairs where each word is a key, and
the value is a line number in the file where the word appears. Here's a list of
the key/value pairs in the input collection:

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

`GroupByKey` gathers up all the values with the same key and outputs a new pair
consisting of the unique key and a collection of all of the values that were
associated with that key in the input collection. If we apply `GroupByKey` to
our input collection above, the output collection would look like this:

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
...
```

Thus, `GroupByKey` represents a transform from a multimap (multiple keys to
individual values) to a uni-map (unique keys to collections of values).

##### 4.2.2.1 GroupByKey and unbounded PCollections {#groupbykey-and-unbounded-pcollections}

If you are using unbounded `PCollection`s, you must use either [non-global
windowing](#setting-your-pcollections-windowing-function) or an
[aggregation trigger](#triggers) in order to perform a `GroupByKey` or
[CoGroupByKey](#cogroupbykey). This is because a bounded `GroupByKey` or
`CoGroupByKey` must wait for all the data with a certain key to be collected,
but with unbounded collections, the data is unlimited. Windowing and/or triggers
allow grouping to operate on logical, finite bundles of data within the
unbounded data streams.

If you do apply `GroupByKey` or `CoGroupByKey` to a group of unbounded
`PCollection`s without setting either a non-global windowing strategy, a trigger
strategy, or both for each collection, Beam generates an IllegalStateException
error at pipeline construction time.

When using `GroupByKey` or `CoGroupByKey` to group `PCollection`s that have a
[windowing strategy](#windowing) applied, all of the `PCollection`s you want to
group *must use the same windowing strategy* and window sizing. For example, all
of the collections you are merging must use (hypothetically) identical 5-minute
fixed windows, or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use `GroupByKey` or `CoGroupByKey` to merge
`PCollection`s with incompatible windows, Beam generates an
IllegalStateException error at pipeline construction time.

#### 4.2.3. CoGroupByKey {#cogroupbykey}

`CoGroupByKey` performs a relational join of two or more key/value
`PCollection`s that have the same key type.
[Design Your Pipeline](/documentation/pipelines/design-your-pipeline/#multiple-sources)
shows an example pipeline that uses a join.

Consider using `CoGroupByKey` if you have multiple data sets that provide
information about related things. For example, let's say you have two different
files with user data: one file has names and email addresses; the other file
has names and phone numbers. You can join those two data sets, using the user
name as a common key and the other data as the associated values. After the
join, you have one data set that contains all of the information (email
addresses and phone numbers) associated with each name.

If you are using unbounded `PCollection`s, you must use either [non-global
windowing](#setting-your-pcollections-windowing-function) or an
[aggregation trigger](#triggers) in order to perform a `CoGroupByKey`. See
[GroupByKey and unbounded PCollections](#groupbykey-and-unbounded-pcollections)
for more details.

<span class="language-java">
In the Beam SDK for Java, `CoGroupByKey` accepts a tuple of keyed
`PCollection`s (`PCollection<KV<K, V>>`) as input. For type safety, the SDK
requires you to pass each `PCollection` as part of a `KeyedPCollectionTuple`.
You must declare a `TupleTag` for each input `PCollection` in the
`KeyedPCollectionTuple` that you want to pass to `CoGroupByKey`. As output,
`CoGroupByKey` returns a `PCollection<KV<K, CoGbkResult>>`, which groups values
from all the input `PCollection`s by their common keys. Each key (all of type
`K`) will have a different `CoGbkResult`, which is a map from `TupleTag<T>` to
`Iterable<T>`. You can access a specific collection in an `CoGbkResult` object
by using the `TupleTag` that you supplied with the initial collection.
</span>
<span class="language-py">
In the Beam SDK for Python, `CoGroupByKey` accepts a dictionary of keyed
`PCollection`s as input. As output, `CoGroupByKey` creates a single output
`PCollection` that contains one key/value tuple for each key in the input
`PCollection`s. Each key's value is a dictionary that maps each tag to an
iterable of the values under they key in the corresponding `PCollection`.
</span>

The following conceptual examples use two input collections to show the mechanics of
`CoGroupByKey`.

<span class="language-java">
The first set of data has a `TupleTag<String>` called `emailsTag` and contains names
and email addresses. The second set of data has a `TupleTag<String>` called
`phonesTag` and contains names and phone numbers.
</span>
<span class="language-py">
The first set of data contains names and email addresses. The second set of
data contains names and phone numbers.
</span>

{{< highlight java >}}
{{< code_sample "examples/java/src/test/java/org/apache/beam/examples/snippets/SnippetsTest.java" CoGroupByKeyTupleInputs >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_group_by_key_cogroupbykey_tuple_inputs >}}
{{< /highlight >}}

After `CoGroupByKey`, the resulting data contains all data associated with each
unique key from any of the input collections.

{{< highlight java >}}
{{< code_sample "examples/java/src/test/java/org/apache/beam/examples/snippets/SnippetsTest.java" CoGroupByKeyTupleOutputs >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_group_by_key_cogroupbykey_tuple_outputs >}}
{{< /highlight >}}

The following code example joins the two `PCollection`s with `CoGroupByKey`,
followed by a `ParDo` to consume the result. Then, the code uses tags to look up
and format data from each collection.

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" CoGroupByKeyTuple >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_group_by_key_cogroupbykey_tuple >}}
{{< /highlight >}}

The formatted data looks like this:

{{< highlight java >}}
{{< code_sample "examples/java/src/test/java/org/apache/beam/examples/snippets/SnippetsTest.java" CoGroupByKeyTupleFormattedOutputs >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_group_by_key_cogroupbykey_tuple_formatted_outputs >}}
{{< /highlight >}}

#### 4.2.4. Combine {#combine}

<span class="language-java">[`Combine`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/transforms/Combine.html)</span>
<span class="language-py">[`Combine`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)</span>
is a Beam transform for combining collections of elements or values in your
data. `Combine` has variants that work on entire `PCollection`s, and some that
combine the values for each key in `PCollection`s of key/value pairs.

When you apply a `Combine` transform, you must provide the function that
contains the logic for combining the elements or values. The combining function
should be commutative and associative, as the function is not necessarily
invoked exactly once on all values with a given key. Because the input data
(including the value collection) may be distributed across multiple workers, the
combining function might be called multiple times to perform partial combining
on subsets of the value collection. The Beam SDK also provides some pre-built
combine functions for common numeric combination operations such as sum, min,
and max.

Simple combine operations, such as sums, can usually be implemented as a simple
function. More complex combination operations might require you to create a
subclass of `CombineFn` that has an accumulation type distinct from the
input/output type.

##### 4.2.4.1. Simple combinations using simple functions {#simple-combines}

The following example code shows a simple combine function.

{{< highlight java >}}
// Sum a collection of Integer values. The function SumInts implements the interface SerializableFunction.
public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
  @Override
  public Integer apply(Iterable<Integer> input) {
    int sum = 0;
    for (int item : input) {
      sum += item;
    }
    return sum;
  }
}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" combine_bounded_sum >}}
{{< /highlight >}}

##### 4.2.4.2. Advanced combinations using CombineFn {#advanced-combines}

For more complex combine functions, you can define a subclass of `CombineFn`.
You should use `CombineFn` if the combine function requires a more sophisticated
accumulator, must perform additional pre- or post-processing, might change the
output type, or takes the key into account.

A general combining operation consists of four operations. When you create a
subclass of `CombineFn`, you must provide four operations by overriding the
corresponding methods:

1. **Create Accumulator** creates a new "local" accumulator. In the example
   case, taking a mean average, a local accumulator tracks the running sum of
   values (the numerator value for our final average division) and the number of
   values summed so far (the denominator value). It may be called any number of
   times in a distributed fashion.

2. **Add Input** adds an input element to an accumulator, returning the
   accumulator value. In our example, it would update the sum and increment the
   count. It may also be invoked in parallel.

3. **Merge Accumulators** merges several accumulators into a single accumulator;
   this is how data in multiple accumulators is combined before the final
   calculation. In the case of the mean average computation, the accumulators
   representing each portion of the division are merged together. It may be
   called again on its outputs any number of times.

4. **Extract Output** performs the final computation. In the case of computing a
   mean average, this means dividing the combined sum of all the values by the
   number of values summed. It is called once on the final, merged accumulator.

The following example code shows how to define a `CombineFn` that computes a
mean average:

{{< highlight java >}}
public class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
  public static class Accum {
    int sum = 0;
    int count = 0;
  }

  @Override
  public Accum createAccumulator() { return new Accum(); }

  @Override
  public Accum addInput(Accum accum, Integer input) {
      accum.sum += input;
      accum.count++;
      return accum;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accums) {
    Accum merged = createAccumulator();
    for (Accum accum : accums) {
      merged.sum += accum.sum;
      merged.count += accum.count;
    }
    return merged;
  }

  @Override
  public Double extractOutput(Accum accum) {
    return ((double) accum.sum) / accum.count;
  }
}
{{< /highlight >}}

{{< highlight py >}}
pc = ...
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" combine_custom_average_define >}}
{{< /highlight >}}

##### 4.2.4.3. Combining a PCollection into a single value {#combining-pcollection}

Use the global combine to transform all of the elements in a given `PCollection`
into a single value, represented in your pipeline as a new `PCollection`
containing one element. The following example code shows how to apply the Beam
provided sum combine function to produce a single sum value for a `PCollection`
of integers.

{{< highlight java >}}
// Sum.SumIntegerFn() combines the elements in the input PCollection. The resulting PCollection, called sum,
// contains one value: the sum of all the elements in the input PCollection.
PCollection<Integer> pc = ...;
PCollection<Integer> sum = pc.apply(
   Combine.globally(new Sum.SumIntegerFn()));
{{< /highlight >}}

{{< highlight py >}}
# sum combines the elements in the input PCollection.
# The resulting PCollection, called result, contains one value: the sum of all
# the elements in the input PCollection.
pc = ...
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" combine_custom_average_execute >}}
{{< /highlight >}}

##### 4.2.4.4. Combine and global windowing {#combine-global-windowing}

If your input `PCollection` uses the default global windowing, the default
behavior is to return a `PCollection` containing one item. That item's value
comes from the accumulator in the combine function that you specified when
applying `Combine`. For example, the Beam provided sum combine function returns
a zero value (the sum of an empty input), while the min combine function returns
a maximal or infinite value.

To have `Combine` instead return an empty `PCollection` if the input is empty,
specify `.withoutDefaults` when you apply your `Combine` transform, as in the
following code example:

{{< highlight java >}}
PCollection<Integer> pc = ...;
PCollection<Integer> sum = pc.apply(
  Combine.globally(new Sum.SumIntegerFn()).withoutDefaults());
{{< /highlight >}}

{{< highlight py >}}
pc = ...
sum = pc | beam.CombineGlobally(sum).without_defaults()
{{< /highlight >}}

##### 4.2.4.5. Combine and non-global windowing {#combine-non-global-windowing}

If your `PCollection` uses any non-global windowing function, Beam does not
provide the default behavior. You must specify one of the following options when
applying `Combine`:

* Specify `.withoutDefaults`, where windows that are empty in the input
  `PCollection` will likewise be empty in the output collection.
* Specify `.asSingletonView`, in which the output is immediately converted to a
  `PCollectionView`, which will provide a default value for each empty window
  when used as a side input. You'll generally only need to use this option if
  the result of your pipeline's `Combine` is to be used as a side input later in
  the pipeline.

##### 4.2.4.6. Combining values in a keyed PCollection {#combining-values-in-a-keyed-pcollection}

After creating a keyed PCollection (for example, by using a `GroupByKey`
transform), a common pattern is to combine the collection of values associated
with each key into a single, merged value. Drawing on the previous example from
`GroupByKey`, a key-grouped `PCollection` called `groupedWords` looks like this:
```
  cat, [1,5,9]
  dog, [5,2]
  and, [1,2,6]
  jump, [3]
  tree, [2]
  ...
```

In the above `PCollection`, each element has a string key (for example, "cat")
and an iterable of integers for its value (in the first element, containing [1,
5, 9]). If our pipeline's next processing step combines the values (rather than
considering them individually), you can combine the iterable of integers to
create a single, merged value to be paired with each key. This pattern of a
`GroupByKey` followed by merging the collection of values is equivalent to
Beam's Combine PerKey transform. The combine function you supply to Combine
PerKey must be an associative reduction function or a subclass of `CombineFn`.

{{< highlight java >}}
// PCollection is grouped by key and the Double values associated with each key are combined into a Double.
PCollection<KV<String, Double>> salesRecords = ...;
PCollection<KV<String, Double>> totalSalesPerPerson =
  salesRecords.apply(Combine.<String, Double, Double>perKey(
    new Sum.SumDoubleFn()));

// The combined value is of a different type than the original collection of values per key. PCollection has
// keys of type String and values of type Integer, and the combined value is a Double.
PCollection<KV<String, Integer>> playerAccuracy = ...;
PCollection<KV<String, Double>> avgAccuracyPerPlayer =
  playerAccuracy.apply(Combine.<String, Integer, Double>perKey(
    new MeanInts())));
{{< /highlight >}}

{{< highlight py >}}
# PCollection is grouped by key and the numeric values associated with each key
# are averaged into a float.
player_accuracies = ...
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" combine_per_key >}}
{{< /highlight >}}

#### 4.2.5. Flatten {#flatten}

<span class="language-java">[`Flatten`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/transforms/Flatten.html)</span>
<span class="language-py">[`Flatten`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)</span>
is a Beam transform for `PCollection` objects that store the same data type.
`Flatten` merges multiple `PCollection` objects into a single logical
`PCollection`.

The following example shows how to apply a `Flatten` transform to merge multiple
`PCollection` objects.

{{< highlight java >}}
// Flatten takes a PCollectionList of PCollection objects of a given type.
// Returns a single PCollection that contains all of the elements in the PCollection objects in that list.
PCollection<String> pc1 = ...;
PCollection<String> pc2 = ...;
PCollection<String> pc3 = ...;
PCollectionList<String> collections = PCollectionList.of(pc1).and(pc2).and(pc3);

PCollection<String> merged = collections.apply(Flatten.<String>pCollections());
{{< /highlight >}}


{{< highlight py >}}
# Flatten takes a tuple of PCollection objects.
# Returns a single PCollection that contains all of the elements in the PCollection objects in that tuple.
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_multiple_pcollections_flatten >}}
{{< /highlight >}}

##### 4.2.5.1. Data encoding in merged collections {#data-encoding-merged-collections}

By default, the coder for the output `PCollection` is the same as the coder for
the first `PCollection` in the input `PCollectionList`. However, the input
`PCollection` objects can each use different coders, as long as they all contain
the same data type in your chosen language.

##### 4.2.5.2. Merging windowed collections {#merging-windowed-collections}

When using `Flatten` to merge `PCollection` objects that have a windowing
strategy applied, all of the `PCollection` objects you want to merge must use a
compatible windowing strategy and window sizing. For example, all the
collections you're merging must all use (hypothetically) identical 5-minute
fixed windows or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use `Flatten` to merge `PCollection` objects with
incompatible windows, Beam generates an `IllegalStateException` error when your
pipeline is constructed.

#### 4.2.6. Partition {#partition}

<span class="language-java">[`Partition`](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/transforms/Partition.html)</span>
<span class="language-py">[`Partition`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)</span>
is a Beam transform for `PCollection` objects that store the same data
type. `Partition` splits a single `PCollection` into a fixed number of smaller
collections.

`Partition` divides the elements of a `PCollection` according to a partitioning
function that you provide. The partitioning function contains the logic that
determines how to split up the elements of the input `PCollection` into each
resulting partition `PCollection`. The number of partitions must be determined
at graph construction time. You can, for example, pass the number of partitions
as a command-line option at runtime (which will then be used to build your
pipeline graph), but you cannot determine the number of partitions in
mid-pipeline (based on data calculated after your pipeline graph is constructed,
for instance).

The following example divides a `PCollection` into percentile groups.

{{< highlight java >}}
// Provide an int value with the desired number of result partitions, and a PartitionFn that represents the
// partitioning function. In this example, we define the PartitionFn in-line. Returns a PCollectionList
// containing each of the resulting partitions as individual PCollection objects.
PCollection<Student> students = ...;
// Split students up into 10 partitions, by percentile:
PCollectionList<Student> studentsByPercentile =
    students.apply(Partition.of(10, new PartitionFn<Student>() {
        public int partitionFor(Student student, int numPartitions) {
            return student.getPercentile()  // 0..99
                 * numPartitions / 100;
        }}));

// You can extract each partition from the PCollectionList using the get method, as follows:
PCollection<Student> fortiethPercentile = studentsByPercentile.get(4);
{{< /highlight >}}

{{< highlight py >}}
# Provide an int value with the desired number of result partitions, and a partitioning function (partition_fn in this example).
# Returns a tuple of PCollection objects containing each of the resulting partitions as individual PCollection objects.
students = ...
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_multiple_pcollections_partition >}}

# You can extract each partition from the tuple of PCollection objects as follows:
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_multiple_pcollections_partition_40th >}}
{{< /highlight >}}

### 4.3. Requirements for writing user code for Beam transforms {#requirements-for-writing-user-code-for-beam-transforms}

When you build user code for a Beam transform, you should keep in mind the
distributed nature of execution. For example, there might be many copies of your
function running on a lot of different machines in parallel, and those copies
function independently, without communicating or sharing state with any of the
other copies. Depending on the Pipeline Runner and processing back-end you
choose for your pipeline, each copy of your user code function may be retried or
run multiple times. As such, you should be cautious about including things like
state dependency in your user code.

In general, your user code must fulfill at least these requirements:

* Your function object must be **serializable**.
* Your function object must be **thread-compatible**, and be aware that *the
  Beam SDKs are not thread-safe*.

In addition, it's recommended that you make your function object **idempotent**.
Non-idempotent functions are supported by Beam, but require additional
thought to ensure correctness when there are external side effects.

> **Note:** These requirements apply to subclasses of `DoFn` (a function object
> used with the [ParDo](#pardo) transform), `CombineFn` (a function object used
> with the [Combine](#combine) transform), and `WindowFn` (a function object
> used with the [Window](#windowing) transform).

#### 4.3.1. Serializability {#user-code-serializability}

Any function object you provide to a transform must be **fully serializable**.
This is because a copy of the function needs to be serialized and transmitted to
a remote worker in your processing cluster. The base classes for user code, such
as `DoFn`, `CombineFn`, and `WindowFn`, already implement `Serializable`;
however, your subclass must not add any non-serializable members.

Some other serializability factors you should keep in mind are:

* Transient fields in your function object are *not* transmitted to worker
  instances, because they are not automatically serialized.
* Avoid loading a field with a large amount of data before serialization.
* Individual instances of your function object cannot share data.
* Mutating a function object after it gets applied will have no effect.
* Take care when declaring your function object inline by using an anonymous
  inner class instance. In a non-static context, your inner class instance will
  implicitly contain a pointer to the enclosing class and that class' state.
  That enclosing class will also be serialized, and thus the same considerations
  that apply to the function object itself also apply to this outer class.

#### 4.3.2. Thread-compatibility {#user-code-thread-compatibility}

Your function object should be thread-compatible. Each instance of your function
object is accessed by a single thread at a time on a worker instance, unless you
explicitly create your own threads. Note, however, that **the Beam SDKs are not
thread-safe**. If you create your own threads in your user code, you must
provide your own synchronization. Note that static members in your function
object are not passed to worker instances and that multiple instances of your
function may be accessed from different threads.

#### 4.3.3. Idempotence {#user-code-idempotence}

It's recommended that you make your function object idempotent--that is, that it
can be repeated or retried as often as necessary without causing unintended side
effects. Non-idempotent functions are supported, however the Beam model provides
no guarantees as to the number of times your user code might be invoked or retried;
as such, keeping your function object idempotent keeps your pipeline's output
deterministic, and your transforms' behavior more predictable and easier to debug.

### 4.4. Side inputs {#side-inputs}

In addition to the main input `PCollection`, you can provide additional inputs
to a `ParDo` transform in the form of side inputs. A side input is an additional
input that your `DoFn` can access each time it processes an element in the input
`PCollection`. When you specify a side input, you create a view of some other
data that can be read from within the `ParDo` transform's `DoFn` while processing
each element.

Side inputs are useful if your `ParDo` needs to inject additional data when
processing each element in the input `PCollection`, but the additional data
needs to be determined at runtime (and not hard-coded). Such values might be
determined by the input data, or depend on a different branch of your pipeline.


#### 4.4.1. Passing side inputs to ParDo {#side-inputs-pardo}

{{< highlight java >}}
  // Pass side inputs to your ParDo transform by invoking .withSideInputs.
  // Inside your DoFn, access the side input by using the method DoFn.ProcessContext.sideInput.

  // The input PCollection to ParDo.
  PCollection<String> words = ...;

  // A PCollection of word lengths that we'll combine into a single value.
  PCollection<Integer> wordLengths = ...; // Singleton PCollection

  // Create a singleton PCollectionView from wordLengths using Combine.globally and View.asSingleton.
  final PCollectionView<Integer> maxWordLengthCutOffView =
     wordLengths.apply(Combine.globally(new Max.MaxIntFn()).asSingletonView());


  // Apply a ParDo that takes maxWordLengthCutOffView as a side input.
  PCollection<String> wordsBelowCutOff =
  words.apply(ParDo
      .of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
            // In our DoFn, access the side input.
            int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
            if (word.length() <= lengthCutOff) {
              out.output(word);
            }
          }
      }).withSideInputs(maxWordLengthCutOffView)
  );
{{< /highlight >}}

{{< highlight py >}}
# Side inputs are available as extra arguments in the DoFn's process method or Map / FlatMap's callable.
# Optional, positional, and keyword arguments are all supported. Deferred arguments are unwrapped into their
# actual values. For example, using pvalue.AsIteor(pcoll) at pipeline construction time results in an iterable
# of the actual elements of pcoll being passed into each process invocation. In this example, side inputs are
# passed to a FlatMap transform as extra arguments and consumed by filter_using_length.
words = ...
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_side_input >}}

# We can also pass side inputs to a ParDo transform, which will get passed to its process method.
# The first two arguments for the process method would be self and element.

{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_side_input_dofn >}}
...
{{< /highlight >}}

#### 4.4.2. Side inputs and windowing {#side-inputs-windowing}

A windowed `PCollection` may be infinite and thus cannot be compressed into a
single value (or single collection class). When you create a `PCollectionView`
of a windowed `PCollection`, the `PCollectionView` represents a single entity
per window (one singleton per window, one list per window, etc.).

Beam uses the window(s) for the main input element to look up the appropriate
window for the side input element. Beam projects the main input element's window
into the side input's window set, and then uses the side input from the
resulting window. If the main input and side inputs have identical windows, the
projection provides the exact corresponding window. However, if the inputs have
different windows, Beam uses the projection to choose the most appropriate side
input window.

For example, if the main input is windowed using fixed-time windows of one
minute, and the side input is windowed using fixed-time windows of one hour,
Beam projects the main input window against the side input window set and
selects the side input value from the appropriate hour-long side input window.

If the main input element exists in more than one window, then `processElement`
gets called multiple times, once for each window. Each call to `processElement`
projects the "current" window for the main input element, and thus might provide
a different view of the side input each time.

If the side input has multiple trigger firings, Beam uses the value from the
latest trigger firing. This is particularly useful if you use a side input with
a single global window and specify a trigger.

### 4.5. Additional outputs {#additional-outputs}

While `ParDo` always produces a main output `PCollection` (as the return value
from `apply`), you can also have your `ParDo` produce any number of additional
output `PCollection`s. If you choose to have multiple outputs, your `ParDo`
returns all of the output `PCollection`s (including the main output) bundled
together.

#### 4.5.1. Tags for multiple outputs {#output-tags}

{{< highlight java >}}
// To emit elements to multiple output PCollections, create a TupleTag object to identify each collection
// that your ParDo produces. For example, if your ParDo produces three output PCollections (the main output
// and two additional outputs), you must create three TupleTags. The following example code shows how to
// create TupleTags for a ParDo with three output PCollections.

  // Input PCollection to our ParDo.
  PCollection<String> words = ...;

  // The ParDo will filter words whose length is below a cutoff and add them to
  // the main output PCollection<String>.
  // If a word is above the cutoff, the ParDo will add the word length to an
  // output PCollection<Integer>.
  // If a word starts with the string "MARKER", the ParDo will add that word to an
  // output PCollection<String>.
  final int wordLengthCutOff = 10;

  // Create three TupleTags, one for each output PCollection.
  // Output that contains words below the length cutoff.
  final TupleTag<String> wordsBelowCutOffTag =
      new TupleTag<String>(){};
  // Output that contains word lengths.
  final TupleTag<Integer> wordLengthsAboveCutOffTag =
      new TupleTag<Integer>(){};
  // Output that contains "MARKER" words.
  final TupleTag<String> markedWordsTag =
      new TupleTag<String>(){};

// Passing Output Tags to ParDo:
// After you specify the TupleTags for each of your ParDo outputs, pass the tags to your ParDo by invoking
// .withOutputTags. You pass the tag for the main output first, and then the tags for any additional outputs
// in a TupleTagList. Building on our previous example, we pass the three TupleTags for our three output
// PCollections to our ParDo. Note that all of the outputs (including the main output PCollection) are
// bundled into the returned PCollectionTuple.

  PCollectionTuple results =
      words.apply(ParDo
          .of(new DoFn<String, String>() {
            // DoFn continues here.
            ...
          })
          // Specify the tag for the main output.
          .withOutputTags(wordsBelowCutOffTag,
          // Specify the tags for the two additional outputs as a TupleTagList.
                          TupleTagList.of(wordLengthsAboveCutOffTag)
                                      .and(markedWordsTag)));
{{< /highlight >}}

{{< highlight py >}}
# To emit elements to multiple output PCollections, invoke with_outputs() on the ParDo, and specify the
# expected tags for the outputs. with_outputs() returns a DoOutputsTuple object. Tags specified in
# with_outputs are attributes on the returned DoOutputsTuple object. The tags give access to the
# corresponding output PCollections.

{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_with_tagged_outputs >}}

# The result is also iterable, ordered in the same order that the tags were passed to with_outputs(),
# the main tag (if specified) first.

{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_with_tagged_outputs_iter >}}
{{< /highlight >}}

#### 4.5.2. Emitting to multiple outputs in your DoFn {#multiple-outputs-dofn}

{{< highlight java >}}
// Inside your ParDo's DoFn, you can emit an element to a specific output PCollection by providing a
// MultiOutputReceiver to your process method, and passing in the appropriate TupleTag to obtain an OutputReceiver.
// After your ParDo, extract the resulting output PCollections from the returned PCollectionTuple.
// Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.

  .of(new DoFn<String, String>() {
     public void processElement(@Element String word, MultiOutputReceiver out) {
       if (word.length() <= wordLengthCutOff) {
         // Emit short word to the main output.
         // In this example, it is the output with tag wordsBelowCutOffTag.
         out.get(wordsBelowCutOffTag).output(word);
       } else {
         // Emit long word length to the output with tag wordLengthsAboveCutOffTag.
         out.get(wordLengthsAboveCutOffTag).output(word.length());
       }
       if (word.startsWith("MARKER")) {
         // Emit word to the output with tag markedWordsTag.
         out.get(markedWordsTag).output(word);
       }
     }}));
{{< /highlight >}}

{{< highlight py >}}
# Inside your ParDo's DoFn, you can emit an element to a specific output by wrapping the value and the output tag (str).
# using the pvalue.OutputValue wrapper class.
# Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.

{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_emitting_values_on_tagged_outputs >}}

# Producing multiple outputs is also available in Map and FlatMap.
# Here is an example that uses FlatMap and shows that the tags do not need to be specified ahead of time.

{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_pardo_with_undeclared_outputs >}}
{{< /highlight >}}

#### 4.5.3. Accessing additional parameters in your DoFn {#other-dofn-parameters}

{{< paragraph class="language-java" >}}
In addition to the element and the `OutputReceiver`, Beam will populate other parameters to your DoFn's `@ProcessElement` method.
Any combination of these parameters can be added to your process method in any order.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
In addition to the element, Beam will populate other parameters to your DoFn's `process` method.
Any combination of these parameters can be added to your process method in any order.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
**Timestamp:**
To access the timestamp of an input element, add a parameter annotated with `@Timestamp` of type `Instant`. For example:
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
**Timestamp:**
To access the timestamp of an input element, add a keyword parameter default to `DoFn.TimestampParam`. For example:
{{< /paragraph >}}

{{< highlight java >}}
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, @Timestamp Instant timestamp) {
  }})
{{< /highlight >}}

{{< highlight py >}}
import apache_beam as beam

class ProcessRecord(beam.DoFn):

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
     # access timestamp of element.
     pass

{{< /highlight >}}

{{< paragraph class="language-java" >}}
**Window:**
To access the window an input element falls into, add a parameter of the type of the window used for the input `PCollection`.
If the parameter is a window type (a subclass of `BoundedWindow`) that does not match the input `PCollection`, then an error
will be raised. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the
`@ProcessElement` method will be invoked multiple time for the element, once for each window. For example, when fixed windows
are being used, the window is of type `IntervalWindow`.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
**Window:**
To access the window an input element falls into, add a keyword parameter default to `DoFn.WindowParam`.
If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the
`process` method will be invoked multiple time for the element, once for each window.
{{< /paragraph >}}

{{< highlight java >}}
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, IntervalWindow window) {
  }})
{{< /highlight >}}

{{< highlight py >}}
import apache_beam as beam

class ProcessRecord(beam.DoFn):

  def process(self, element, window=beam.DoFn.WindowParam):
     # access window e.g. window.end.micros
     pass

{{< /highlight >}}

{{< paragraph class="language-java" >}}
**PaneInfo:**
When triggers are used, Beam provides a `PaneInfo` object that contains information about the current firing. Using `PaneInfo`
you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
**PaneInfo:**
When triggers are used, Beam provides a `DoFn.PaneInfoParam` object that contains information about the current firing. Using `DoFn.PaneInfoParam`
you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.
This feature implementation in Python SDK is not fully completed; see more at [BEAM-3759](https://issues.apache.org/jira/browse/BEAM-3759).
{{< /paragraph >}}

{{< highlight java >}}
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PaneInfo paneInfo) {
  }})
{{< /highlight >}}

{{< highlight py >}}
import apache_beam as beam

class ProcessRecord(beam.DoFn):

  def process(self, element, pane_info=beam.DoFn.PaneInfoParam):
     # access pane info, e.g. pane_info.is_first, pane_info.is_last, pane_info.timing
     pass

{{< /highlight >}}

{{< paragraph class="language-java" >}}
**PipelineOptions:**
The `PipelineOptions` for the current pipeline can always be accessed in a process method by adding it
as a parameter:
{{< /paragraph >}}

{{< highlight java >}}
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PipelineOptions options) {
  }})
{{< /highlight >}}

{{< paragraph class="language-java" >}}
`@OnTimer` methods can also access many of these parameters. Timestamp, Window, key, `PipelineOptions`, `OutputReceiver`, and
`MultiOutputReceiver` parameters can all be accessed in an `@OnTimer` method. In addition, an `@OnTimer` method can take
a parameter of type `TimeDomain` which tells whether the timer is based on event time or processing time.
Timers are explained in more detail in the
[Timely (and Stateful) Processing with Apache Beam](/blog/2017/08/28/timely-processing.html) blog post.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
**Timer and State:**
In addition to aforementioned parameters, user defined Timer and State parameters can be used in a stateful DoFn.
Timers and States are explained in more detail in the
[Timely (and Stateful) Processing with Apache Beam](/blog/2017/08/28/timely-processing.html) blog post.
{{< /paragraph >}}

{{< highlight py >}}

class StatefulDoFn(beam.DoFn):
  """An example stateful DoFn with state and timer"""

  BUFFER_STATE_1 = BagStateSpec('buffer1', beam.BytesCoder())
  BUFFER_STATE_2 = BagStateSpec('buffer2', beam.VarIntCoder())
  WATERMARK_TIMER = TimerSpec('watermark_timer', TimeDomain.WATERMARK)

  def process(self,
              element,
              timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam,
              buffer_1=beam.DoFn.StateParam(BUFFER_STATE_1),
              buffer_2=beam.DoFn.StateParam(BUFFER_STATE_2),
              watermark_timer=beam.DoFn.TimerParam(WATERMARK_TIMER)):

    # Do your processing here
    key, value = element
    # Read all the data from buffer1
    all_values_in_buffer_1 = [x for x in buffer_1.read()]

    if StatefulDoFn._is_clear_buffer_1_required(all_values_in_buffer_1):
        # clear the buffer data if required conditions are met.
        buffer_1.clear()

    # add the value to buffer 2
    buffer_2.add(value)

    if StatefulDoFn._all_condition_met():
      # Clear the timer if certain condition met and you don't want to trigger
      # the callback method.
      watermark_timer.clear()

    yield element

  @on_timer(WATERMARK_TIMER)
  def on_expiry_1(self,
                  timestamp=beam.DoFn.TimestampParam,
                  window=beam.DoFn.WindowParam,
                  key=beam.DoFn.KeyParam,
                  buffer_1=beam.DoFn.StateParam(BUFFER_STATE_1),
                  buffer_2=beam.DoFn.StateParam(BUFFER_STATE_2)):
    # Window and key parameters are really useful especially for debugging issues.
    yield 'expired1'

  @staticmethod
  def _all_condition_met():
      # some logic
      return True

  @staticmethod
  def _is_clear_buffer_1_required(buffer_1_data):
      # Some business logic
      return True

{{< /highlight >}}

### 4.6. Composite transforms {#composite-transforms}

Transforms can have a nested structure, where a complex transform performs
multiple simpler transforms (such as more than one `ParDo`, `Combine`,
`GroupByKey`, or even other composite transforms). These transforms are called
composite transforms. Nesting multiple transforms inside a single composite
transform can make your code more modular and easier to understand.

The Beam SDK comes packed with many useful composite transforms. See the API
reference pages for a list of transforms:
  * [Pre-written Beam transforms for Java](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/transforms/package-summary.html)
  * [Pre-written Beam transforms for Python](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.transforms.html)

#### 4.6.1. An example composite transform {#composite-transform-example}

The `CountWords` transform in the [WordCount example program](/get-started/wordcount-example/)
is an example of a composite transform. `CountWords` is a `PTransform` subclass
that consists of multiple nested transforms.

In its `expand` method, the `CountWords` transform applies the following
transform operations:

  1. It applies a `ParDo` on the input `PCollection` of text lines, producing
     an output `PCollection` of individual words.
  2. It applies the Beam SDK library transform `Count` on the `PCollection` of
     words, producing a `PCollection` of key/value pairs. Each key represents a
     word in the text, and each value represents the number of times that word
     appeared in the original data.

{{< highlight java >}}
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" pipeline_monitoring_composite >}}
{{< /highlight >}}

> **Note:** Because `Count` is itself a composite transform,
> `CountWords` is also a nested composite transform.

#### 4.6.2. Creating a composite transform {#composite-transform-creation}

To create your own composite transform, create a subclass of the `PTransform`
class and override the `expand` method to specify the actual processing logic.
You can then use this transform just as you would a built-in transform from the
Beam SDK.

{{< paragraph class="language-java" >}}
For the `PTransform` class type parameters, you pass the `PCollection` types
that your transform takes as input, and produces as output. To take multiple
`PCollection`s as input, or produce multiple `PCollection`s as output, use one
of the multi-collection types for the relevant type parameter.
{{< /paragraph >}}

The following code sample shows how to declare a `PTransform` that accepts a
`PCollection` of `String`s for input, and outputs a `PCollection` of `Integer`s:

{{< highlight java >}}
  static class ComputeWordLengths
    extends PTransform<PCollection<String>, PCollection<Integer>> {
    ...
  }
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_composite_transform >}}
{{< /highlight >}}

Within your `PTransform` subclass, you'll need to override the `expand` method.
The `expand` method is where you add the processing logic for the `PTransform`.
Your override of `expand` must accept the appropriate type of input
`PCollection` as a parameter, and specify the output `PCollection` as the return
value.

The following code sample shows how to override `expand` for the
`ComputeWordLengths` class declared in the previous example:

{{< highlight java >}}
  static class ComputeWordLengths
      extends PTransform<PCollection<String>, PCollection<Integer>> {
    @Override
    public PCollection<Integer> expand(PCollection<String>) {
      ...
      // transform logic goes here
      ...
    }
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_composite_transform >}}
{{< /highlight >}}

As long as you override the `expand` method in your `PTransform` subclass to
accept the appropriate input `PCollection`(s) and return the corresponding
output `PCollection`(s), you can include as many transforms as you want. These
transforms can include core transforms, composite transforms, or the transforms
included in the Beam SDK libraries.

Your composite transform's parameters and return value must match the initial
input type and final return type for the entire transform, even if the
transform's intermediate data changes type multiple times.

**Note:** The `expand` method of a `PTransform` is not meant to be invoked
directly by the user of a transform. Instead, you should call the `apply` method
on the `PCollection` itself, with the transform as an argument. This allows
transforms to be nested within the structure of your pipeline.

#### 4.6.3. PTransform Style Guide {#ptransform-style-guide}

The [PTransform Style Guide](/contribute/ptransform-style-guide/)
contains additional information not included here, such as style guidelines,
logging and testing guidance, and language-specific considerations.  The guide
is a useful starting point when you want to write new composite PTransforms.

## 5. Pipeline I/O {#pipeline-io}

When you create a pipeline, you often need to read data from some external
source, such as a file or a database. Likewise, you may
want your pipeline to output its result data to an external storage system.
Beam provides read and write transforms for a [number of common data storage
types](/documentation/io/built-in/). If you want your pipeline
to read from or write to a data storage format that isn't supported by the
built-in transforms, you can [implement your own read and write
transforms](/documentation/io/developing-io-overview/).

### 5.1. Reading input data {#pipeline-io-reading-data}

Read transforms read data from an external source and return a `PCollection`
representation of the data for use by your pipeline. You can use a read
transform at any point while constructing your pipeline to create a new
`PCollection`, though it will be most common at the start of your pipeline.

{{< highlight java >}}
PCollection<String> lines = p.apply(TextIO.read().from("gs://some/inputData.txt"));
{{< /highlight >}}

{{< highlight py >}}
lines = pipeline | beam.io.ReadFromText('gs://some/inputData.txt')
{{< /highlight >}}

### 5.2. Writing output data {#pipeline-io-writing-data}

Write transforms write the data in a `PCollection` to an external data source.
You will most often use write transforms at the end of your pipeline to output
your pipeline's final results. However, you can use a write transform to output
a `PCollection`'s data at any point in your pipeline.

{{< highlight java >}}
output.apply(TextIO.write().to("gs://some/outputData"));
{{< /highlight >}}

{{< highlight py >}}
output | beam.io.WriteToText('gs://some/outputData')
{{< /highlight >}}

### 5.3. File-based input and output data {#file-based-data}

#### 5.3.1. Reading from multiple locations {#file-based-reading-multiple-locations}

Many read transforms support reading from multiple input files matching a glob
operator you provide. Note that glob operators are filesystem-specific and obey
filesystem-specific consistency models. The following TextIO example uses a glob
operator (`*`) to read all matching input files that have prefix "input-" and the
suffix ".csv" in the given location:

{{< highlight java >}}
p.apply("ReadFromText",
    TextIO.read().from("protocol://my_bucket/path/to/input-*.csv"));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_pipelineio_read >}}
{{< /highlight >}}

To read data from disparate sources into a single `PCollection`, read each one
independently and then use the [Flatten](#flatten) transform to create a single
`PCollection`.

#### 5.3.2. Writing to multiple output files {#file-based-writing-multiple-files}

For file-based output data, write transforms write to multiple output files by
default. When you pass an output file name to a write transform, the file name
is used as the prefix for all output files that the write transform produces.
You can append a suffix to each output file by specifying a suffix.

The following write transform example writes multiple output files to a
location. Each file has the prefix "numbers", a numeric tag, and the suffix
".csv".

{{< highlight java >}}
records.apply("WriteToText",
    TextIO.write().to("protocol://my_bucket/path/to/numbers")
                .withSuffix(".csv"));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_pipelineio_write >}}
{{< /highlight >}}

### 5.4. Beam-provided I/O transforms {#provided-io-transforms}

See the [Beam-provided I/O Transforms](/documentation/io/built-in/)
page for a list of the currently available I/O transforms.

## 6. Schemas {#schemas}

Often, the types of the records being processed have an obvious structure. Common Beam sources produce
JSON, Avro, Protocol Buffer, or database row objects; all of these types have well defined structures,
structures that can often be determined by examining the type. Even within a SDK pipeline, Simple Java POJOs
(or  equivalent structures in other languages) are often used as intermediate types, and these also have a
 clear structure that can be inferred by inspecting the class. By understanding the structure of a pipeline’s
 records, we can provide much more concise APIs for data processing.

### 6.1. What is a schema? {#what-is-a-schema}

Most structured records share some common characteristics:
* They can be subdivided into separate named fields. Fields usually have string names, but sometimes - as in the case of indexed
 tuples - have numerical indices instead.
* There is a confined list of primitive types that a field can have. These often match primitive types in most programming
 languages: int, long, string, etc.
* Often a field type can be marked as optional (sometimes referred to as nullable) or required.

Often records have a nested structure. A nested structure occurs when a field itself has subfields so the
type of the field itself has a schema. Fields that are array or map types is also a common feature of these structured
records.

For example, consider the following schema, representing actions in a fictitious e-commerce company:

**Purchase**

<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>userId</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>itemId</td>
      <td>INT64</td>
    </tr>
    <tr>
      <td>shippingAddress</td>
      <td>ROW(ShippingAddress)</td>
    </tr>
    <tr>
      <td>cost</td>
      <td>INT64</td>
    </tr>
    <tr>
      <td>transactions</td>
      <td>ARRAY[ROW(Transaction)]</td>
    </tr>
  </tbody>
</table>
<br/>

**ShippingAddress**

<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>streetAddress</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>city</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>state</td>
      <td>nullable STRING</td>
    </tr>
    <tr>
      <td>country</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>postCode</td>
      <td>STRING</td>
    </tr>
  </tbody>
</table>
<br/>

**Transaction**

<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>bank</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>purchaseAmount</td>
      <td>DOUBLE</td>
    </tr>
  </tbody>
</table>
<br/>

Purchase event records are represented by the above purchase schema. Each purchase event contains a shipping address, which
is a nested row containing its own schema. Each purchase also contains an array of credit-card transactions
(a list, because a purchase might be split across multiple credit cards); each item in the transaction list is a row
with its own schema.

This provides an abstract description of the types involved, one that is abstracted away from any specific programming
language.

Schemas provide us a type-system for Beam records that is independent of any specific programming-language type. There
might be multiple Java classes that all have the same schema (for example a Protocol-Buffer class or a POJO class),
and Beam will allow us to seamlessly convert between these types. Schemas also provide a simple way to reason about
types across different programming-language APIs.

A `PCollection` with a schema does not need to have a `Coder` specified, as Beam knows how to encode and decode
Schema rows; Beam uses a special coder to encode schema types.

### 6.2. Schemas for programming language types {#schemas-for-pl-types}

While schemas themselves are language independent, they are designed to embed naturally into the programming languages
of the Beam SDK being used. This allows Beam users to continue using native types while reaping the advantage of
having Beam understand their element schemas.

{{< paragraph class="language-java" >}}
In Java you could use the following set of classes to represent the purchase schema.  Beam will automatically
infer the correct schema based on the members of the class.
{{< /paragraph >}}

{{< highlight java >}}
@DefaultSchema(JavaBeanSchema.class)
public class Purchase {
  public String getUserId();  // Returns the id of the user who made the purchase.
  public long getItemId();  // Returns the identifier of the item that was purchased.
  public ShippingAddress getShippingAddress();  // Returns the shipping address, a nested type.
  public long getCostCents();  // Returns the cost of the item.
  public List<Transaction> getTransactions();  // Returns the transactions that paid for this purchase (returns a list, since the purchase might be spread out over multiple credit cards).

  @SchemaCreate
  public Purchase(String userId, long itemId, ShippingAddress shippingAddress, long costCents,
                  List<Transaction> transactions) {
      ...
  }
}

@DefaultSchema(JavaBeanSchema.class)
public class ShippingAddress {
  public String getStreetAddress();
  public String getCity();
  @Nullable public String getState();
  public String getCountry();
  public String getPostCode();

  @SchemaCreate
  public ShippingAddress(String streetAddress, String city, @Nullable String state, String country,
                         String postCode) {
     ...
  }
}

@DefaultSchema(JavaBeanSchema.class)
public class Transaction {
  public String getBank();
  public double getPurchaseAmount();

  @SchemaCreate
  public Transaction(String bank, double purchaseAmount) {
     ...
  }
}
{{< /highlight >}}

Using JavaBean classes as above is one way to map a schema to Java classes. However multiple Java classes might have
the same schema, in which case the different Java types can often be used interchangeably. Beam will add implicit
conversions between types that have matching schemas. For example, the above
`Transaction` class has the same schema as the following class:

{{< highlight java >}}
@DefaultSchema(JavaFieldSchema.class)
public class TransactionPojo {
  public String bank;
  public double purchaseAmount;
}
{{< /highlight >}}

So if we had two `PCollection`s as follows

{{< highlight java >}}
PCollection<Transaction> transactionBeans = readTransactionsAsJavaBean();
PCollection<TransactionPojos> transactionPojos = readTransactionsAsPojo();
{{< /highlight >}}

Then these two `PCollection`s would have the same schema, even though their Java types would be different. This means
for example the following two code snippets are valid:

{{< highlight java >}}
transactionBeans.apply(ParDo.of(new DoFn<...>() {
   @ProcessElement public void process(@Element TransactionPojo pojo) {
      ...
   }
}));
{{< /highlight >}}

and
{{< highlight java >}}
transactionPojos.apply(ParDo.of(new DoFn<...>() {
   @ProcessElement public void process(@Element Transaction row) {
    }
}));
{{< /highlight >}}

Even though the in both cases the `@Element` parameter differs from the the `PCollection`'s Java type, since the
schemas are the same Beam will automatically make the conversion. The built-in `Convert` transform can also be used
to translate between Java types of equivalent schemas, as detailed below.

### 6.3. Schema definition {#schema-definition}

The schema for a `PCollection` defines elements of that `PCollection` as an ordered list of named fields. Each field
has a name, a type, and possibly a set of user options. The type of a field can be primitive or composite. The following
are the primitive types currently supported by Beam:

<table>
  <thead>
    <tr class="header">
      <th>Type</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BYTE</td>
      <td>An 8-bit signed value</td>
    </tr>
     <tr>
       <td>INT16</td>
       <td>A 16-bit signed value</td>
     </tr>
     <tr>
       <td>INT32</td>
       <td>A 32-bit signed value</td>
     </tr>
    <tr>
      <td>INT64</td>
       <td>A 64-bit signed value</td>
     </tr>
     <tr>
       <td>DECIMAL</td>
       <td>An arbitrary-precision decimal type</td>
     </tr>
     <tr>
       <td>FLOAT</td>
       <td>A 32-bit IEEE 754 floating point number</td>
     </tr>
     <tr>
       <td>DOUBLE</td>
       <td>A 64-bit IEEE 754 floating point number</td>
     </tr>
     <tr>
       <td>STRING</td>
       <td>A string</td>
     </tr>
     <tr>
       <td>DATETIME</td>
       <td>A timestamp represented as milliseconds since the epoch</td>
     </tr>
     <tr>
       <td>BOOLEAN</td>
       <td>A boolean value</td>
     </tr>
     <tr>
       <td>BYTES</td>
       <td>A raw byte array</td>
     </tr>
  </tbody>
</table>
<br/>

A field can also reference a nested schema. In this case, the field will have type ROW, and the nested schema will
be an attribute of this field type.

Three collection types are supported as field types: ARRAY, ITERABLE and MAP:
* **ARRAY** This represents a repeated value type, where the repeated elements can have any supported type. Arrays of
nested rows are supported, as are arrays of arrays.
* **ITERABLE** This is very similar to the array type, it represents a repeated value, but one in which the full list of
items is not known until iterated over. This is intended for the case where an iterable might be larger than the
available memory, and backed by external storage (for example, this can happen with the iterable returned by a
`GroupByKey`). The repeated elements can have any supported type.
* **MAP** This represents an associative map from keys to values. All schema types are supported for both keys and values.
 Values that contain map types cannot be used as keys in any grouping operation.

### 6.4. Logical types {#logical-types}

Users can extend the schema type system to add custom logical types that can be used as a field. A logical type is
identified by a unique identifier and an argument. A logical type also specifies an underlying schema type to be used
for storage, along with conversions to and from that type. As an example, a logical union can always be represented as
a row with nullable fields, where the user ensures that only one of those fields is ever set at a time. However this can
be tedious and complex to manage. The OneOf logical type provides a value class that makes it easier to manage the type
as a union, while still using a row with nullable fields as its underlying storage. Each logical type also has a
unique identifier, so they can be interpreted by other languages as well. More examples of logical types are listed
below.

#### 6.4.1. Defining a logical type {#defining-a-logical-type}

To define a logical type you must specify a Schema type to be used to represent the underlying type as well as a unique
identifier for that type. A logical type imposes additional semantics on top a schema type. For example, a logical
type to represent nanosecond timestamps is represented as a schema containing an INT64 and an INT32 field. This schema
alone does not say anything about how to interpret this type, however the logical type tells you that this represents
a nanosecond timestamp, with the INT64 field representing seconds and the INT32 field representing nanoseconds.

Logical types are also specified by an argument, which allows creating a class of related types. For example, a
limited-precision decimal type would have an integer argument indicating how many digits of precision are represented.
The argument is represented by a schema type, so can itself be a complex type.

{{< paragraph class="language-java" >}}
In Java, a logical type is specified as a subclass of the `LogicalType` class. A custom Java class can be specified to represent the logical type and conversion functions must be supplied to convert back and forth between this Java class and the underlying Schema type representation. For example, the logical type representing nanosecond timestamp might be implemented as follows
{{< /paragraph >}}

{{< highlight java >}}
// A Logical type using java.time.Instant to represent the logical type.
public class TimestampNanos implements LogicalType<Instant, Row> {
  // The underlying schema used to represent rows.
  private final Schema SCHEMA = Schema.builder().addInt64Field("seconds").addInt32Field("nanos").build();
  @Override public String getIdentifier() { return "timestampNanos"; }
  @Override public FieldType getBaseType() { return schema; }

  // Convert the representation type to the underlying Row type. Called by Beam when necessary.
  @Override public Row toBaseType(Instant instant) {
    return Row.withSchema(schema).addValues(instant.getEpochSecond(), instant.getNano()).build();
  }

  // Convert the underlying Row type to an Instant. Called by Beam when necessary.
  @Override public Instant toInputType(Row base) {
    return Instant.of(row.getInt64("seconds"), row.getInt32("nanos"));
  }

     ...
}
{{< /highlight >}}

#### 6.4.2. Useful logical types {#built-in-logical-types}

##### **EnumerationType**

This logical type allows creating an enumeration type consisting of a set of named constants.

{{< highlight java >}}
Schema schema = Schema.builder()
               …
     .addLogicalTypeField("color", EnumerationType.create("RED", "GREEN", "BLUE"))
     .build();
{{< /highlight >}}

The value of this field is stored in the row as an INT32 type, however the logical type defines a value type that lets
you access the enumeration either as a string or a value. For example:

{{< highlight java >}}
EnumerationType.Value enumValue = enumType.valueOf("RED");
enumValue.getValue();  // Returns 0, the integer value of the constant.
enumValue.toString();  // Returns "RED", the string value of the constant
{{< /highlight >}}

Given a row object with an enumeration field, you can also extract the field as the enumeration value.

{{< highlight java >}}
EnumerationType.Value enumValue = row.getLogicalTypeValue("color", EnumerationType.Value.class);
{{< /highlight >}}

Automatic schema inference from Java POJOs and JavaBeans automatically converts Java enums to EnumerationType logical
types.

##### **OneOfType**

OneOfType allows creating a disjoint union type over a set of schema fields. For example:

{{< highlight java >}}
Schema schema = Schema.builder()
               …
     .addLogicalTypeField("oneOfField",
        OneOfType.create(Field.of("intField", FieldType.INT32),
                         Field.of("stringField", FieldType.STRING),
                         Field.of("bytesField", FieldType.BYTES)))
      .build();
{{< /highlight >}}

The value of this field is stored in the row as another Row type, where all the fields are marked as nullable. The
logical type however defines a Value object that contains an enumeration value indicating which field was set and allows
 getting just that field:

{{< highlight java >}}
// Returns an enumeration indicating all possible case values for the enum.
// For the above example, this will be
// EnumerationType.create("intField", "stringField", "bytesField");
EnumerationType oneOfEnum = onOfType.getCaseEnumType();

// Creates an instance of the union with the string field set.
OneOfType.Value oneOfValue = oneOfType.createValue("stringField", "foobar");

// Handle the oneof
switch (oneOfValue.getCaseEnumType().toString()) {
  case "intField":
    return processInt(oneOfValue.getValue(Integer.class));
  case "stringField":
    return processString(oneOfValue.getValue(String.class));
  case "bytesField":
    return processBytes(oneOfValue.getValue(bytes[].class));
}
{{< /highlight >}}

In the above example we used the field names in the switch statement for clarity, however the enum integer values could
 also be used.

### 6.5. Creating Schemas {#creating-schemas}

In order to take advantage of schemas, your `PCollection`s must have a schema attached to it. Often, the source itself will attach a schema to the PCollection. For example, when using `AvroIO` to read Avro files, the source can automatically infer a Beam schema from the Avro schema and attach that to the Beam `PCollection`. However not all sources produce schemas. In addition, often Beam pipelines have intermediate stages and types, and those also can benefit from the expressiveness of schemas.

#### 6.5.1. Inferring schemas {#inferring-schemas}

{{< paragraph class="language-java" >}}
Beam is able to infer schemas from a variety of common Java types. The `@DefaultSchema` annotation can be used to tell Beam to infer schemas from a specific type. The annotation takes a `SchemaProvider` as an argument, and `SchemaProvider` classes are already built in for common Java types. The `SchemaRegistry` can also be invoked programmatically for cases where it is not practical to annotate the Java type itself.
{{< /paragraph >}}

##### **Java POJOs**

A POJO (Plain Old Java Object) is a Java object that is not bound by any restriction other than the Java Language
Specification. A POJO can contain member variables that are primitives, that are other POJOs, or are collections maps or
arrays thereof. POJOs do not have to extend prespecified classes or extend any specific interfaces.

If a POJO class is annotated with `@DefaultSchema(JavaFieldSchema.class)`, Beam will automatically infer a schema for
this class. Nested classes are supported as are classes with `List`, array, and `Map` fields.

For example, annotating the following class tells Beam to infer a schema from this POJO class and apply it to any
`PCollection<TransactionPojo>`.

{{< highlight java >}}
@DefaultSchema(JavaFieldSchema.class)
public class TransactionPojo {
  public final String bank;
  public final double purchaseAmount;
  @SchemaCreate
  public TransactionPojo(String bank, double purchaseAmount) {
    this.bank = bank.
    this.purchaseAmount = purchaseAmount;
  }
}
// Beam will automatically infer the correct schema for this PCollection. No coder is needed as a result.
PCollection<TransactionPojo> pojos = readPojos();
{{< /highlight >}}

The `@SchemaCreate` annotation tells Beam that this constructor can be used to create instances of TransactionPojo,
assuming that constructor parameters have the same names as the field names. `@SchemaCreate` can also be used to annotate
static factory methods on the class, allowing the constructor to remain private. If there is no `@SchemaCreate`
 annotation then all the fields must be non-final and the class must have a zero-argument constructor.

There are a couple of other useful annotations that affect how Beam infers schemas. By default the schema field names
inferred will match that of the class field names. However `@SchemaFieldName` can be used to specify a different name to
be used for the schema field. `@SchemaIgnore` can be used to mark specific class fields as excluded from the inferred
schema. For example, it’s common to have ephemeral fields in a class that should not be included in a schema
(e.g. caching the hash value to prevent expensive recomputation of the hash), and `@SchemaIgnore` can be used to
exclude these fields. Note that ignored fields will not be included in the encoding of these records.

In some cases it is not convenient to annotate the POJO class, for example if the POJO is in a different package that is
not owned by the Beam pipeline author. In these cases the schema inference can be triggered programmatically in
pipeline’s main function as follows:

{{< highlight java >}}
 pipeline.getSchemaRegistry().registerPOJO(TransactionPOJO.class);
{{< /highlight >}}

##### **Java Beans**

Java Beans are a de-facto standard for creating reusable property classes in Java. While the full
standard has many characteristics, the key ones are that all properties are accessed via getter and setter classes, and
the name format for these getters and setters is standardized. A Java Bean class can be annotated with
`@DefaultSchema(JavaBeanSchema.class)` and Beam will automatically infer a schema for this class. For example:

{{< highlight java >}}
@DefaultSchema(JavaBeanSchema.class)
public class TransactionBean {
  public TransactionBean() { … }
  public String getBank() { … }
  public void setBank(String bank) { … }
  public double getPurchaseAmount() { … }
  public void setPurchaseAmount(double purchaseAmount) { … }
}
// Beam will automatically infer the correct schema for this PCollection. No coder is needed as a result.
PCollection<TransactionBean> beans = readBeans();
{{< /highlight >}}

The `@SchemaCreate` annotation can be used to specify a constructor or a static factory method, in which case the
setters and zero-argument constructor can be omitted.

{{< highlight java >}}
@DefaultSchema(JavaBeanSchema.class)
public class TransactionBean {
  @SchemaCreate
  Public TransactionBean(String bank, double purchaseAmount) { … }
  public String getBank() { … }
  public double getPurchaseAmount() { … }
}
{{< /highlight >}}

`@SchemaFieldName` and `@SchemaIgnore` can be used to alter the schema inferred, just like with POJO classes.

##### **AutoValue**

Java value classes are notoriously difficult to generate correctly. There is a lot of boilerplate you must create in
order to properly implement a value class. AutoValue is a popular library for easily generating such classes by
implementing a simple abstract base class.

Beam can infer a schema from an AutoValue class. For example:

{{< highlight java >}}
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class TransactionValue {
  public abstract String getBank();
  public abstract double getPurchaseAmount();
}
{{< /highlight >}}

This is all that’s needed to generate a simple AutoValue class, and the above `@DefaultSchema` annotation tells Beam to
infer a schema from it. This also allows AutoValue elements to be used inside of `PCollection`s.

`@SchemaFieldName` and `@SchemaIgnore` can be used to alter the schema inferred.

### 6.6. Using Schema Transforms {#using-schemas}

A schema on a `PCollection` enables a rich variety of relational transforms. The fact that each record is composed of
named fields allows for simple and readable aggregations that reference fields by name, similar to the aggregations in
a SQL expression.

#### 6.6.1. Field selection syntax

The advantage of schemas is that they allow referencing of element fields by name. Beam provides a selection syntax for
referencing fields, including nested and repeated fields. This syntax is used by all of the schema transforms when
referencing the fields they operate on. The syntax can also be used inside of a DoFn to specify which schema fields to
process.

Addressing fields by name still retains type safety as Beam will check that schemas match at the time the pipeline graph
is constructed. If a field is specified that does not exist in the schema, the pipeline will fail to launch. In addition,
if a field is specified with a type that does not match the type of that field in the schema, the pipeline will fail to
launch.

The following characters are not allowed in field names: . *  [ ] { }

##### **Top-level fields**

In order to select a field at the top level of a schema, the name of the field is specified. For example, to select just
the user ids from a `PCollection` of purchases one would write (using the `Select` transform)

{{< highlight java >}}
purchases.apply(Select.fieldNames("userId"));
{{< /highlight >}}

##### **Nested fields**

Individual nested fields can be specified using the dot operator. For example, to select just the postal code from the
 shipping address one would write

{{< highlight java >}}
purchases.apply(Select.fieldNames("shippingAddress.postCode"));
{{< /highlight >}}

##### **Wildcards**

The * operator can be specified at any nesting level to represent all fields at that level. For example, to select all
shipping-address fields one would write

{{< highlight java >}}
purchases.apply(Select.fieldNames("shippingAddress.*"));
{{< /highlight >}}

##### **Arrays**

An array field, where the array element type is a row, can also have subfields of the element type addressed. When
selected, the result is an array of the selected subfield type. For example

{{< highlight java >}}
purchases.apply(Select.fieldNames("transactions[].bank"));
{{< /highlight >}}

Will result in a row containing an array field with element-type string, containing the list of banks for each
transaction.

While the use of  [] brackets in the selector is recommended, to make it clear that array elements are being selected,
they can be omitted for brevity. In the future, array slicing will be supported, allowing selection of portions of the
array.

##### **Maps**

A map field, where the value type is a row, can also have subfields of the value type addressed. When selected, the
result is a map where the keys are the same as in the original map but the value is the specified type. Similar to
arrays, the use of {} curly brackets in the selector is recommended, to make it clear that map value elements are being
selected, they can be omitted for brevity. In the future, map key selectors will be supported, allowing selection of
specific keys from the map. For example, given the following schema:

**PurchasesByType**

<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>purchases</td>
      <td>MAP{STRING, ROW{PURCHASE}</td>
    </tr>
  </tbody>
</table>
<br/>

The following

{{< highlight java >}}
purchasesByType.apply(Select.fieldNames("purchases{}.userId"));
{{< /highlight >}}

Will result in a row containing a map field with key-type string and value-type string. The selected map will contain
all of the keys from the original map, and the values will be the userId contained in the purchase record.

While the use of {} brackets in the selector is recommended, to make it clear that map value elements are being selected,
they can be omitted for brevity. In the future, map slicing will be supported, allowing selection of specific keys from
the map.

#### 6.6.2. Schema transforms

Beam provides a collection of transforms that operate natively on schemas. These transforms are very expressive,
allowing selections and aggregations in terms of named schema fields. Following are some examples of useful
schema transforms.

##### **Selecting input**

Often a computation is only interested in a subset of the fields in an input `PCollection`. The `Select` transform allows
one to easily project out only the fields of interest. The resulting `PCollection` has  a schema containing each selected
field as a top-level field. Both top-level and nested fields can be selected. For example, in the Purchase schema, one
could select only the userId and streetAddress fields as follows

{{< highlight java >}}
purchases.apply(Select.fieldNames("userId", "shippingAddress.streetAddress"));
{{< /highlight >}}

The resulting `PCollection` will have the following schema

<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>userId</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>streetAddress</td>
      <td>STRING</td>
    </tr>
  </tbody>
</table>
<br/>

The same is true for wildcard selections. The following

{{< highlight java >}}
purchases.apply(Select.fieldNames("userId", "shippingAddress.*"));
{{< /highlight >}}

Will result in the following schema

<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>userId</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>streetAddress</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>city</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>state</td>
      <td>nullable STRING</td>
    </tr>
    <tr>
      <td>country</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>postCode</td>
      <td>STRING</td>
    </tr>
  </tbody>
</table>
<br/>

When selecting fields nested inside of an array, the same rule applies that each selected field appears separately as a
top-level field in the resulting row. This means that if multiple fields are selected from the same nested row, each
selected field will appear as its own array field. For example

{{< highlight java >}}
purchases.apply(Select.fieldNames( "transactions.bank", "transactions.purchaseAmount"));
{{< /highlight >}}

Will result in the following schema
<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>bank</td>
      <td>ARRAY[STRING]</td>
    </tr>
    <tr>
      <td>purchaseAmount</td>
      <td>ARRAY[DOUBLE]</td>
    </tr>
  </tbody>
</table>
<br/>

Wildcard selections are equivalent to separately selecting each field.

Selecting fields nested inside of maps have the same semantics as arrays. If you select multiple fields from a map
, then each selected field will be expanded to its own map at the top level. This means that the set of map keys will
 be copied, once for each selected field.

Sometimes different nested rows will have fields with the same name. Selecting multiple of these fields would result in
a name conflict, as all selected fields are put in the same row schema. When this situation arises, the
`Select.withFieldNameAs` builder method can be used to provide an alternate name for the selected field.

Another use of the Select transform is to flatten a nested schema into a single flat schema. For example

{{< highlight java >}}
purchases.apply(Select.flattenedSchema());
{{< /highlight >}}

Will result in the following schema
<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>userId</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>itemId</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>shippingAddress_streetAddress</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>shippingAddress_city</td>
      <td>nullable STRING</td>
    </tr>
    <tr>
      <td>shippingAddress_state</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>shippingAddress_country</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>shippingAddress_postCode</td>
      <td>STRING</td>
    </tr>
     <tr>
       <td>costCents</td>
       <td>INT64</td>
     </tr>
     <tr>
       <td>transactions_bank</td>
       <td>ARRAY[STRING]</td>
     </tr>
    <tr>
      <td>transactions_purchaseAmount</td>
      <td>ARRAY[DOUBLE]</td>
    </tr>
  </tbody>
</table>
<br/>

##### **Grouping aggregations**

The `Group` transform allows simply grouping data by any number of fields in the input schema, applying aggregations to
those groupings, and storing the result of those aggregations in a new schema field. The output of the `Group` transform
has a schema with one field corresponding to each aggregation performed.

The simplest usage of `Group` specifies no aggregations, in which case all inputs matching the provided set of fields
are grouped together into an `ITERABLE` field. For example

{{< highlight java >}}
purchases.apply(Group.byFieldNames("userId", "shippingAddress.streetAddress"));
{{< /highlight >}}

The output schema of this is:

<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>key</td>
      <td>ROW{userId:STRING, streetAddress:STRING}</td>
    </tr>
    <tr>
      <td>values</td>
      <td>ITERABLE[ROW[Purchase]]</td>
    </tr>
  </tbody>
</table>
<br/>

The key field contains the grouping key and the values field contains a list of all the values that matched that key.

The names of the key and values fields in the output schema can be controlled using this withKeyField and withValueField
builders, as follows:

{{< highlight java >}}
purchases.apply(Group.byFieldNames("userId", "shippingAddress.streetAddress")
    .withKeyField("userAndStreet")
    .withValueField("matchingPurchases"));
{{< /highlight >}}

It is quite common to apply one or more aggregations to the grouped result. Each aggregation can  specify one or more fields
to aggregate, an aggregation function, and the name of the resulting field in the output schema. For example, the
following application computes three aggregations grouped by userId, with all aggregations represented in a single
output schema:

{{< highlight java >}}
purchases.apply(Group.byFieldNames("userId")
    .aggregateField("itemId", Count.combineFn(), "numPurchases")
    .aggregateField("costCents", Sum.ofLongs(), "totalSpendCents")
    .aggregateField("costCents", Top.<Long>largestLongsFn(10), "topPurchases"));
{{< /highlight >}}

The result of this aggregation will have the following schema:
<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>key</td>
      <td>ROW{userId:STRING}</td>
    </tr>
    <tr>
      <td>value</td>
      <td>ROW{numPurchases: INT64, totalSpendCents: INT64, topPurchases: ARRAY[INT64]}</td>
    </tr>
  </tbody>
</table>
<br/>

Often `Selected.flattenedSchema` will be use to flatten the result into a non-nested, flat schema.

##### **Joins**

Beam supports equijoins on schema `PCollections` - namely joins where the join condition depends on the equality of a
subset of fields. For example, the following examples uses the Purchases schema to join transactions with the reviews
that are likely associated with that transaction (both the user and product match that in the transaction). This is a
"natural join" - one in which the same field names are used on both the left-hand and right-hand sides of the join -
and is specified with the `using` keyword:

{{< highlight java >}}
PCollection<Transaction> transactions = readTransactions();
PCollection<Review> reviews = readReviews();
PCollection<Row> joined = transactions.apply(
    Join.innerJoin(reviews).using("userId", "productId"));
{{< /highlight >}}

The resulting schema is the following:
<table>
  <thead>
    <tr class="header">
      <th><b>Field Name</b></th>
      <th><b>Field Type</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>lhs</td>
      <td>ROW{Transaction}</td>
    </tr>
    <tr>
      <td>rhs</td>
      <td>ROW{Review}</td>
    </tr>
  </tbody>
</table>
<br/>

Each resulting row contains one Review and one Review that matched the join condition.

If the fields to match in the two schemas have different names, then the on function can be used. For example, if the
Review schema named those fields differently than the Transaction schema, then we could write the following:

{{< highlight java >}}
PCollection<Row> joined = transactions.apply(
    Join.innerJoin(reviews).on(
      FieldsEqual
         .left("userId", "productId")
         .right("reviewUserId", "reviewProductId")));
{{< /highlight >}}

In addition to inner joins, the Join transform supports full outer joins, left outer joins, and right outer joins.

##### **Complex joins**

While most joins tend to be binary joins - joining two inputs together - sometimes you have more than two input
streams that all need to be joined on a common key. The `CoGroup` transform allows joining multiple `PCollections`
together based on equality of schema fields. Each `PCollection` can be marked as required or optional in the final
join record, providing a generalization of outer joins to joins with greater than two input `PCollection`s. The output
can optionally be expanded - providing individual joined records, as in the `Join` transform. The output can also be
processed in unexpanded format - providing the join key along with Iterables of all records from each input that matched
that key.

##### **Filtering events**

The `Filter` transform can be configured with a set of predicates, each one based one specified fields. Only records for
which all predicates return true will pass the filter. For example the following

{{< highlight java >}}
purchases.apply(Filter
    .whereFieldName("costCents", c -> c > 100 * 20)
    .whereFieldName("shippingAddress.country", c -> c.equals("de"));
{{< /highlight >}}

Will produce all purchases made from Germany with a purchase price of greater than twenty cents.


##### **Adding fields to a schema**

The AddFields transform can be used to extend a schema with new fields. Input rows will be extended to the new schema by
inserting null values for the new fields, though alternate default values can be specified; if the default null value
is used then the new field type will be marked as nullable. Nested subfields can be added using the field selection
syntax, including nested fields inside arrays or map values.

For example, the following application

{{< highlight java >}}
purchases.apply(AddFields.<PurchasePojo>create()
    .field("timeOfDaySeconds", FieldType.INT32)
    .field("shippingAddress.deliveryNotes", FieldType.STRING)
    .field("transactions.isFlagged", FieldType.BOOLEAN, false));
{{< /highlight >}}

Results in a `PCollection` with an expanded schema. All of the rows and fields of the input, but also with the specified
fields added to the schema. All resulting rows will have null values filled in for the **timeOfDaySeconds** and the
**shippingAddress.deliveryNotes** fields, and a false value filled in for the **transactions.isFlagged** field.

##### **Removing fields from a schema**

`DropFields` allows specific fields to be dropped from a schema. Input rows will have their schemas truncated, and any
values for dropped fields will be removed from the output. Nested fields can also be dropped using the field selection
syntax.

For example, the following snippet

{{< highlight java >}}
purchases.apply(DropFields.fields("userId", "shippingAddress.streetAddress"));
{{< /highlight >}}

Results in a copy of the input with those two fields and their corresponding values removed.

##### **Renaming schema fields**

`RenameFields` allows specific fields in a schema to be renamed. The field values in input rows are left unchanged, only
the schema is modified. This transform is often used to prepare records for output to a schema-aware sink, such as an
RDBMS, to make sure that the `PCollection` schema field names match that of the output. It can also be used to rename
fields generated by other transforms to make them more usable (similar to SELECT AS in SQL). Nested fields can also be
renamed using the field-selection syntax.

For example, the following snippet

{{< highlight java >}}
purchases.apply(RenameFields.<PurchasePojo>create()
  .rename("userId", "userIdentifier")
  .rename("shippingAddress.streetAddress", "shippingAddress.street"));
{{< /highlight >}}

Results in the same set of unmodified input elements, however the schema on the PCollection has been changed to rename
**userId** to **userIdentifier** and **shippingAddress.streetAddress** to **shippingAddress.street**.

##### **Converting between types**

As mentioned, Beam can automatically convert between different Java types, as long as those types have equivalent
schemas. One way to do this is by using the `Convert` transform, as follows.

{{< highlight java >}}
PCollection<PurchaseBean> purchaseBeans = readPurchasesAsBeans();
PCollection<PurchasePojo> pojoPurchases =
    purchaseBeans.apply(Convert.to(PurchasePojo.class));
{{< /highlight >}}

Beam will validate that the inferred schema for `PurchasePojo` matches that of the input `PCollection`, and will
then cast to a `PCollection<PurchasePojo>`.

Since the `Row` class can support any schema, any `PCollection` with schema can be cast to a `PCollection` of rows, as
follows.

{{< highlight java >}}
PCollection<Row> purchaseRows = purchaseBeans.apply(Convert.toRows());
{{< /highlight >}}

If the source type is a single-field schema, Convert will also convert to the type of the field if asked, effectively
unboxing the row. For example, give a schema with a single INT64 field, the following will convert it to a
`PCollection<Long>`

{{< highlight java >}}
PCollection<Long> longs = rows.apply(Convert.to(TypeDescriptors.longs()));
{{< /highlight >}}

In all cases, type checking is done at pipeline graph construction, and if the types do not match the schema then the
pipeline will fail to launch.

#### 6.6.3. Schemas in ParDo

A `PCollection` with a schema can apply a `ParDo`, just like any other `PCollection`. However the Beam runner is aware
 of schemas when applying a `ParDo`, which enables additional functionality.

##### **Input conversion**

Since Beam knows the schema of the source `PCollection`, it can automatically convert the elements to any Java type for
which a matching schema is known. For example, using the above-mentioned Transaction schema, say we have the following
`PCollection`:

{{< highlight java >}}
PCollection<PurchasePojo> purchases = readPurchases();
{{< /highlight >}}

If there were no schema, then the applied `DoFn` would have to accept an element of type `TransactionPojo`. However
since there is a schema, you could apply the following DoFn:

{{< highlight java >}}
purchases.appy(ParDo.of(new DoFn<PurchasePojo, PurchasePojo>() {
  @ProcessElement public void process(@Element PurchaseBean purchase) {
      ...
  }
}));
{{< /highlight >}}

Even though the `@Element` parameter does not match the Java type of the `PCollection`, since it has a matching schema
Beam will automatically convert elements. If the schema does not match, Beam will detect this at graph-construction time
and will fail the job with a type error.

Since every schema can be represented by a Row type, Row can also be used here:

{{< highlight java >}}
purchases.appy(ParDo.of(new DoFn<PurchasePojo, PurchasePojo>() {
  @ProcessElement public void process(@Element Row purchase) {
      ...
  }
}));
{{< /highlight >}}

##### **Input selection**

Since the input has a schema, you can also automatically select specific fields to process in the DoFn.

Given the above purchases `PCollection`, say you want to process just the userId and the itemId fields. You can do these
using the above-described selection expressions, as follows:

{{< highlight java >}}
purchases.appy(ParDo.of(new DoFn<PurchasePojo, PurchasePojo>() {
  @ProcessElement public void process(
     @FieldAccess("userId") String userId, @FieldAccess("itemId") long itemId) {
      ...
  }
}));
{{< /highlight >}}

You can also select nested fields, as follows.

{{< highlight java >}}
purchases.appy(ParDo.of(new DoFn<PurchasePojo, PurchasePojo>() {
  @ProcessElement public void process(
    @FieldAccess("shippingAddress.street") String street) {
      ...
  }
}));
{{< /highlight >}}

For more information, see the section on field-selection expressions. When selecting subschemas, Beam will
automatically convert to any matching schema type, just like when reading the entire row.


## 7. Data encoding and type safety {#data-encoding-and-type-safety}

When Beam runners execute your pipeline, they often need to materialize the
intermediate data in your `PCollection`s, which requires converting elements to
and from byte strings. The Beam SDKs use objects called `Coder`s to describe how
the elements of a given `PCollection` may be encoded and decoded.

> Note that coders are unrelated to parsing or formatting data when interacting
> with external data sources or sinks. Such parsing or formatting should
> typically be done explicitly, using transforms such as `ParDo` or
> `MapElements`.

{{< paragraph class="language-java" >}}
In the Beam SDK for Java, the type `Coder` provides the methods required for
encoding and decoding data. The SDK for Java provides a number of Coder
subclasses that work with a variety of standard Java types, such as Integer,
Long, Double, StringUtf8 and more. You can find all of the available Coder
subclasses in the [Coder package](https://github.com/apache/beam/tree/master/sdks/java/core/src/main/java/org/apache/beam/sdk/coders).
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
In the Beam SDK for Python, the type `Coder` provides the methods required for
encoding and decoding data. The SDK for Python provides a number of Coder
subclasses that work with a variety of standard Python types, such as primitive
types, Tuple, Iterable, StringUtf8 and more. You can find all of the available
Coder subclasses in the
[apache_beam.coders](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/coders)
package.
{{< /paragraph >}}

> Note that coders do not necessarily have a 1:1 relationship with types. For
> example, the Integer type can have multiple valid coders, and input and output
> data can use different Integer coders. A transform might have Integer-typed
> input data that uses BigEndianIntegerCoder, and Integer-typed output data that
> uses VarIntCoder.

### 7.1. Specifying coders {#specifying-coders}

The Beam SDKs require a coder for every `PCollection` in your pipeline. In most
cases, the Beam SDK is able to automatically infer a `Coder` for a `PCollection`
based on its element type or the transform that produces it, however, in some
cases the pipeline author will need to specify a `Coder` explicitly, or develop
a `Coder` for their custom type.

{{< paragraph class="language-java" >}}
You can explicitly set the coder for an existing `PCollection` by using the
method `PCollection.setCoder`. Note that you cannot call `setCoder` on a
`PCollection` that has been finalized (e.g. by calling `.apply` on it).
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
You can get the coder for an existing `PCollection` by using the method
`getCoder`. This method will fail with an `IllegalStateException` if a coder has
not been set and cannot be inferred for the given `PCollection`.
{{< /paragraph >}}

Beam SDKs use a variety of mechanisms when attempting to automatically infer the
`Coder` for a `PCollection`.

{{< paragraph class="language-java" >}}
Each pipeline object has a `CoderRegistry`. The `CoderRegistry` represents a
mapping of Java types to the default coders that the pipeline should use for
`PCollection`s of each type.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
The Beam SDK for Python has a `CoderRegistry` that represents a mapping of
Python types to the default coder that should be used for `PCollection`s of each
type.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
By default, the Beam SDK for Java automatically infers the `Coder` for the
elements of a `PCollection` produced by a `PTransform` using the type parameter
from the transform's function object, such as `DoFn`. In the case of `ParDo`,
for example, a `DoFn<Integer, String>` function object accepts an input element
of type `Integer` and produces an output element of type `String`. In such a
case, the SDK for Java will automatically infer the default `Coder` for the
output `PCollection<String>` (in the default pipeline `CoderRegistry`, this is
`StringUtf8Coder`).
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
By default, the Beam SDK for Python automatically infers the `Coder` for the
elements of an output `PCollection` using the typehints from the transform's
function object, such as `DoFn`. In the case of `ParDo`, for example a `DoFn`
with the typehints `@beam.typehints.with_input_types(int)` and
`@beam.typehints.with_output_types(str)` accepts an input element of type int
and produces an output element of type str. In such a case, the Beam SDK for
Python will automatically infer the default `Coder` for the output `PCollection`
(in the default pipeline `CoderRegistry`, this is `BytesCoder`).
{{< /paragraph >}}

> NOTE: If you create your `PCollection` from in-memory data by using the
> `Create` transform, you cannot rely on coder inference and default coders.
> `Create` does not have access to any typing information for its arguments, and
> may not be able to infer a coder if the argument list contains a value whose
> exact run-time class doesn't have a default coder registered.

{{< paragraph class="language-java" >}}
When using `Create`, the simplest way to ensure that you have the correct coder
is by invoking `withCoder` when you apply the `Create` transform.
{{< /paragraph >}}

### 7.2. Default coders and the CoderRegistry {#default-coders-and-the-coderregistry}

Each Pipeline object has a `CoderRegistry` object, which maps language types to
the default coder the pipeline should use for those types. You can use the
`CoderRegistry` yourself to look up the default coder for a given type, or to
register a new default coder for a given type.

`CoderRegistry` contains a default mapping of coders to standard
<span class="language-java">Java</span><span class="language-py">Python</span>
types for any pipeline you create using the Beam SDK for
<span class="language-java">Java</span><span class="language-py">Python</span>.
The following table shows the standard mapping:

{{< paragraph class="language-java" >}}
<table>
  <thead>
    <tr class="header">
      <th>Java Type</th>
      <th>Default Coder</th>
    </tr>
  </thead>
  <tbody>
    <tr class="odd">
      <td>Double</td>
      <td>DoubleCoder</td>
    </tr>
    <tr class="even">
      <td>Instant</td>
      <td>InstantCoder</td>
    </tr>
    <tr class="odd">
      <td>Integer</td>
      <td>VarIntCoder</td>
    </tr>
    <tr class="even">
      <td>Iterable</td>
      <td>IterableCoder</td>
    </tr>
    <tr class="odd">
      <td>KV</td>
      <td>KvCoder</td>
    </tr>
    <tr class="even">
      <td>List</td>
      <td>ListCoder</td>
    </tr>
    <tr class="odd">
      <td>Map</td>
      <td>MapCoder</td>
    </tr>
    <tr class="even">
      <td>Long</td>
      <td>VarLongCoder</td>
    </tr>
    <tr class="odd">
      <td>String</td>
      <td>StringUtf8Coder</td>
    </tr>
    <tr class="even">
      <td>TableRow</td>
      <td>TableRowJsonCoder</td>
    </tr>
    <tr class="odd">
      <td>Void</td>
      <td>VoidCoder</td>
    </tr>
    <tr class="even">
      <td>byte[ ]</td>
      <td>ByteArrayCoder</td>
    </tr>
    <tr class="odd">
      <td>TimestampedValue</td>
      <td>TimestampedValueCoder</td>
    </tr>
  </tbody>
</table>
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
<table>
  <thead>
    <tr class="header">
      <th>Python Type</th>
      <th>Default Coder</th>
    </tr>
  </thead>
  <tbody>
    <tr class="odd">
      <td>int</td>
      <td>VarIntCoder</td>
    </tr>
    <tr class="even">
      <td>float</td>
      <td>FloatCoder</td>
    </tr>
    <tr class="odd">
      <td>str</td>
      <td>BytesCoder</td>
    </tr>
    <tr class="even">
      <td>bytes</td>
      <td>StrUtf8Coder</td>
    </tr>
    <tr class="odd">
      <td>Tuple</td>
      <td>TupleCoder</td>
    </tr>
  </tbody>
</table>
{{< /paragraph >}}

#### 7.2.1. Looking up a default coder {#default-coder-lookup}

{{< paragraph class="language-java" >}}
You can use the method `CoderRegistry.getCoder` to determine the default
Coder for a Java type. You can access the `CoderRegistry` for a given pipeline
by using the method `Pipeline.getCoderRegistry`. This allows you to determine
(or set) the default Coder for a Java type on a per-pipeline basis: i.e. "for
this pipeline, verify that Integer values are encoded using
`BigEndianIntegerCoder`."
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
You can use the method `CoderRegistry.get_coder` to determine the default Coder
for a Python type. You can use `coders.registry` to access the `CoderRegistry`.
This allows you to determine (or set) the default Coder for a Python type.
{{< /paragraph >}}

#### 7.2.2. Setting the default coder for a type {#setting-default-coder}

To set the default Coder for a
<span class="language-java">Java</span><span class="language-py">Python</span>
type for a particular pipeline, you obtain and modify the pipeline's
`CoderRegistry`. You use the method
<span class="language-java">`Pipeline.getCoderRegistry`</span>
<span class="language-py">`coders.registry`</span>
to get the `CoderRegistry` object, and then use the method
<span class="language-java">`CoderRegistry.registerCoder`</span>
<span class="language-py">`CoderRegistry.register_coder`</span>
to register a new `Coder` for the target type.

The following example code demonstrates how to set a default Coder, in this case
`BigEndianIntegerCoder`, for
<span class="language-java">Integer</span><span class="language-py">int</span>
values for a pipeline.

{{< highlight java >}}
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);

CoderRegistry cr = p.getCoderRegistry();
cr.registerCoder(Integer.class, BigEndianIntegerCoder.class);
{{< /highlight >}}

{{< highlight py >}}
apache_beam.coders.registry.register_coder(int, BigEndianIntegerCoder)
{{< /highlight >}}

#### 7.2.3. Annotating a custom data type with a default coder {#annotating-custom-type-default-coder}

{{< paragraph class="language-java" >}}
If your pipeline program defines a custom data type, you can use the
`@DefaultCoder` annotation to specify the coder to use with that type. For
example, let's say you have a custom data type for which you want to use
`SerializableCoder`. You can use the `@DefaultCoder` annotation as follows:
{{< /paragraph >}}

{{< highlight java >}}
@DefaultCoder(AvroCoder.class)
public class MyCustomDataType {
  ...
}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
If you've created a custom coder to match your data type, and you want to use
the `@DefaultCoder` annotation, your coder class must implement a static
`Coder.of(Class<T>)` factory method.
{{< /paragraph >}}

{{< highlight java >}}
public class MyCustomCoder implements Coder {
  public static Coder<T> of(Class<T> clazz) {...}
  ...
}

@DefaultCoder(MyCustomCoder.class)
public class MyCustomDataType {
  ...
}
{{< /highlight >}}

{{< paragraph class="language-py" >}}
The Beam SDK for Python does not support annotating data types with a default
coder. If you would like to set a default coder, use the method described in the
previous section, *Setting the default coder for a type*.
{{< /paragraph >}}

## 8. Windowing {#windowing}

Windowing subdivides a `PCollection` according to the timestamps of its
individual elements. Transforms that aggregate multiple elements, such as
`GroupByKey` and `Combine`, work implicitly on a per-window basis — they process
each `PCollection` as a succession of multiple, finite windows, though the
entire collection itself may be of unbounded size.

A related concept, called **triggers**, determines when to emit the results of
aggregation as unbounded data arrives. You can use triggers to refine the
windowing strategy for your `PCollection`. Triggers allow you to deal with
late-arriving data or to provide early results. See the [triggers](#triggers)
section for more information.

### 8.1. Windowing basics {#windowing-basics}

Some Beam transforms, such as `GroupByKey` and `Combine`, group multiple
elements by a common key. Ordinarily, that grouping operation groups all of the
elements that have the same key within the entire data set. With an unbounded
data set, it is impossible to collect all of the elements, since new elements
are constantly being added and may be infinitely many (e.g. streaming data). If
you are working with unbounded `PCollection`s, windowing is especially useful.

In the Beam model, any `PCollection` (including unbounded `PCollection`s) can be
subdivided into logical windows. Each element in a `PCollection` is assigned to
one or more windows according to the `PCollection`'s windowing function, and
each individual window contains a finite number of elements. Grouping transforms
then consider each `PCollection`'s elements on a per-window basis. `GroupByKey`,
for example, implicitly groups the elements of a `PCollection` by _key and
window_.

**Caution:** Beam's default windowing behavior is to assign all elements of a
`PCollection` to a single, global window and discard late data, _even for
unbounded `PCollection`s_. Before you use a grouping transform such as
`GroupByKey` on an unbounded `PCollection`, you must do at least one of the
following:
 * Set a non-global windowing function. See [Setting your PCollection's
   windowing function](#setting-your-pcollections-windowing-function).
 * Set a non-default [trigger](#triggers). This allows the global window to emit
   results under other conditions, since the default windowing behavior (waiting
   for all data to arrive) will never occur.

If you don't set a non-global windowing function or a non-default trigger for
your unbounded `PCollection` and subsequently use a grouping transform such as
`GroupByKey` or `Combine`, your pipeline will generate an error upon
construction and your job will fail.

#### 8.1.1. Windowing constraints {#windowing-constraints}

After you set the windowing function for a `PCollection`, the elements' windows
are used the next time you apply a grouping transform to that `PCollection`.
Window grouping occurs on an as-needed basis. If you set a windowing function
using the `Window` transform, each element is assigned to a window, but the
windows are not considered until `GroupByKey` or `Combine` aggregates across a
window and key. This can have different effects on your pipeline.  Consider the
example pipeline in the figure below:

![Diagram of pipeline applying windowing](/images/windowing-pipeline-unbounded.svg)

**Figure 3:** Pipeline applying windowing

In the above pipeline, we create an unbounded `PCollection` by reading a set of
key/value pairs using `KafkaIO`, and then apply a windowing function to that
collection using the `Window` transform. We then apply a `ParDo` to the the
collection, and then later group the result of that `ParDo` using `GroupByKey`.
The windowing function has no effect on the `ParDo` transform, because the
windows are not actually used until they're needed for the `GroupByKey`.
Subsequent transforms, however, are applied to the result of the `GroupByKey` --
data is grouped by both key and window.

#### 8.1.2. Windowing with bounded PCollections {#windowing-bounded-collections}

You can use windowing with fixed-size data sets in **bounded** `PCollection`s.
However, note that windowing considers only the implicit timestamps attached to
each element of a `PCollection`, and data sources that create fixed data sets
(such as `TextIO`) assign the same timestamp to every element. This means that
all the elements are by default part of a single, global window.

To use windowing with fixed data sets, you can assign your own timestamps to
each element. To assign timestamps to elements, use a `ParDo` transform with a
`DoFn` that outputs each element with a new timestamp (for example, the
[WithTimestamps](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/transforms/WithTimestamps.html)
transform in the Beam SDK for Java).

To illustrate how windowing with a bounded `PCollection` can affect how your
pipeline processes data, consider the following pipeline:

![Diagram of GroupByKey and ParDo without windowing, on a bounded collection](/images/unwindowed-pipeline-bounded.svg)

**Figure 4:** `GroupByKey` and `ParDo` without windowing, on a bounded collection.

In the above pipeline, we create a bounded `PCollection` by reading lines from a
file using `TextIO`. We then group the collection using `GroupByKey`,
and apply a `ParDo` transform to the grouped `PCollection`. In this example, the
`GroupByKey` creates a collection of unique keys, and then `ParDo` gets applied
exactly once per key.

Note that even if you don’t set a windowing function, there is still a window --
all elements in your `PCollection` are assigned to a single global window.

Now, consider the same pipeline, but using a windowing function:

![Diagram of GroupByKey and ParDo with windowing, on a bounded collection](/images/windowing-pipeline-bounded.svg)

**Figure 5:** `GroupByKey` and `ParDo` with windowing, on a bounded collection.

As before, the pipeline creates a bounded `PCollection` of file lines. We
then set a [windowing function](#setting-your-pcollections-windowing-function)
for that `PCollection`.  The `GroupByKey` transform groups the elements of the
`PCollection` by both key and window, based on the windowing function. The
subsequent `ParDo` transform gets applied multiple times per key, once for each
window.

### 8.2. Provided windowing functions {#provided-windowing-functions}

You can define different kinds of windows to divide the elements of your
`PCollection`. Beam provides several windowing functions, including:

*  Fixed Time Windows
*  Sliding Time Windows
*  Per-Session Windows
*  Single Global Window
*  Calendar-based Windows (not supported by the Beam SDK for Python)

You can also define your own `WindowFn` if you have a more complex need.

Note that each element can logically belong to more than one window, depending
on the windowing function you use. Sliding time windowing, for example, creates
overlapping windows wherein a single element can be assigned to multiple
windows.


#### 8.2.1. Fixed time windows {#fixed-time-windows}

The simplest form of windowing is using **fixed time windows**: given a
timestamped `PCollection` which might be continuously updating, each window
might capture (for example) all elements with timestamps that fall into a 30
second interval.

A fixed time window represents a consistent duration, non overlapping time
interval in the data stream. Consider windows with a 30 second duration: all
of the elements in your unbounded `PCollection` with timestamp values from
0:00:00 up to (but not including) 0:00:30 belong to the first window, elements
with timestamp values from 0:00:30 up to (but not including) 0:01:00 belong to
the second window, and so on.

![Diagram of fixed time windows, 30s in duration](/images/fixed-time-windows.png)

**Figure 6:** Fixed time windows, 30s in duration.

#### 8.2.2. Sliding time windows {#sliding-time-windows}

A **sliding time window** also represents time intervals in the data stream;
however, sliding time windows can overlap. For example, each window might
capture 60 seconds worth of data, but a new window starts every 30 seconds.
The frequency with which sliding windows begin is called the _period_.
Therefore, our example would have a window _duration_ of 60 seconds and a
_period_ of 30 seconds.

Because multiple windows overlap, most elements in a data set will belong to
more than one window. This kind of windowing is useful for taking running
averages of data; using sliding time windows, you can compute a running average
of the past 60 seconds' worth of data, updated every 30 seconds, in our
example.

![Diagram of sliding time windows, with 1 minute window duration and 30s window period](/images/sliding-time-windows.png)

**Figure 7:** Sliding time windows, with 1 minute window duration and 30s window
period.

#### 8.2.3. Session windows {#session-windows}

A **session window** function defines windows that contain elements that are
within a certain gap duration of another element. Session windowing applies on a
per-key basis and is useful for data that is irregularly distributed with
respect to time. For example, a data stream representing user mouse activity may
have long periods of idle time interspersed with high concentrations of clicks.
If data arrives after the minimum specified gap duration time, this initiates
the start of a new window.

![Diagram of session windows with a minimum gap duration](/images/session-windows.png)

**Figure 8:** Session windows, with a minimum gap duration. Note how each data key
has different windows, according to its data distribution.

#### 8.2.4. The single global window {#single-global-window}

By default, all data in a `PCollection` is assigned to the single global window,
and late data is discarded. If your data set is of a fixed size, you can use the
global window default for your `PCollection`.

You can use the single global window if you are working with an unbounded data set
(e.g. from a streaming data source) but use caution when applying aggregating
transforms such as `GroupByKey` and `Combine`. The single global window with a
default trigger generally requires the entire data set to be available before
processing, which is not possible with continuously updating data. To perform
aggregations on an unbounded `PCollection` that uses global windowing, you
should specify a non-default trigger for that `PCollection`.

### 8.3. Setting your PCollection's windowing function {#setting-your-pcollections-windowing-function}

You can set the windowing function for a `PCollection` by applying the `Window`
transform. When you apply the `Window` transform, you must provide a `WindowFn`.
The `WindowFn` determines the windowing function your `PCollection` will use for
subsequent grouping transforms, such as a fixed or sliding time window.

When you set a windowing function, you may also want to set a trigger for your
`PCollection`. The trigger determines when each individual window is aggregated
and emitted, and helps refine how the windowing function performs with respect
to late data and computing early results. See the [triggers](#triggers) section
for more information.

#### 8.3.1. Fixed-time windows {#using-fixed-time-windows}

The following example code shows how to apply `Window` to divide a `PCollection`
into fixed windows, each 60 seconds in length:

{{< highlight java >}}
    PCollection<String> items = ...;
    PCollection<String> fixedWindowedItems = items.apply(
        Window.<String>into(FixedWindows.of(Duration.standardSeconds(60))));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" setting_fixed_windows >}}
{{< /highlight >}}

#### 8.3.2. Sliding time windows {#using-sliding-time-windows}

The following example code shows how to apply `Window` to divide a `PCollection`
into sliding time windows. Each window is 30 seconds in length, and a new window
begins every five seconds:

{{< highlight java >}}
    PCollection<String> items = ...;
    PCollection<String> slidingWindowedItems = items.apply(
        Window.<String>into(SlidingWindows.of(Duration.standardSeconds(30)).every(Duration.standardSeconds(5))));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" setting_sliding_windows >}}
{{< /highlight >}}

#### 8.3.3. Session windows {#using-session-windows}

The following example code shows how to apply `Window` to divide a `PCollection`
into session windows, where each session must be separated by a time gap of at
least 10 minutes (600 seconds):

{{< highlight java >}}
    PCollection<String> items = ...;
    PCollection<String> sessionWindowedItems = items.apply(
        Window.<String>into(Sessions.withGapDuration(Duration.standardSeconds(600))));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" setting_session_windows >}}
{{< /highlight >}}

Note that the sessions are per-key — each key in the collection will have its
own session groupings depending on the data distribution.

#### 8.3.4. Single global window {#using-single-global-window}

If your `PCollection` is bounded (the size is fixed), you can assign all the
elements to a single global window. The following example code shows how to set
a single global window for a `PCollection`:

{{< highlight java >}}
    PCollection<String> items = ...;
    PCollection<String> batchItems = items.apply(
        Window.<String>into(new GlobalWindows()));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" setting_global_window >}}
{{< /highlight >}}

### 8.4. Watermarks and late data {#watermarks-and-late-data}

In any data processing system, there is a certain amount of lag between the time
a data event occurs (the "event time", determined by the timestamp on the data
element itself) and the time the actual data element gets processed at any stage
in your pipeline (the "processing time", determined by the clock on the system
processing the element). In addition, there are no guarantees that data events
will appear in your pipeline in the same order that they were generated.

For example, let's say we have a `PCollection` that's using fixed-time
windowing, with windows that are five minutes long. For each window, Beam must
collect all the data with an _event time_ timestamp in the given window range
(between 0:00 and 4:59 in the first window, for instance). Data with timestamps
outside that range (data from 5:00 or later) belong to a different window.

However, data isn't always guaranteed to arrive in a pipeline in time order, or
to always arrive at predictable intervals. Beam tracks a _watermark_, which is
the system's notion of when all data in a certain window can be expected to have
arrived in the pipeline. Once the watermark progresses past the end of a window,
any further element that arrives with a timestamp in that window is considered
**late data**.

From our example, suppose we have a simple watermark that assumes approximately
30s of lag time between the data timestamps (the event time) and the time the
data appears in the pipeline (the processing time), then Beam would close the
first window at 5:30. If a data record arrives at 5:34, but with a timestamp
that would put it in the 0:00-4:59 window (say, 3:38), then that record is late
data.

Note: For simplicity, we've assumed that we're using a very straightforward
watermark that estimates the lag time. In practice, your `PCollection`'s data
source determines the watermark, and watermarks can be more precise or complex.

Beam's default windowing configuration tries to determine when all data has
arrived (based on the type of data source) and then advances the watermark past
the end of the window. This default configuration does _not_ allow late data.
[Triggers](#triggers) allow you to modify and refine the windowing strategy for
a `PCollection`. You can use triggers to decide when each individual window
aggregates and reports its results, including how the window emits late
elements.

#### 8.4.1. Managing late data {#managing-late-data}


You can allow late data by invoking the `.withAllowedLateness` operation when
you set your `PCollection`'s windowing strategy. The following code example
demonstrates a windowing strategy that will allow late data up to two days after
the end of a window.

{{< highlight java >}}
    PCollection<String> items = ...;
    PCollection<String> fixedWindowedItems = items.apply(
        Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
              .withAllowedLateness(Duration.standardDays(2)));
{{< /highlight >}}


{{< highlight py >}}
   pc = [Initial PCollection]
   pc | beam.WindowInto(
              FixedWindows(60),
              trigger=trigger_fn,
              accumulation_mode=accumulation_mode,
              timestamp_combiner=timestamp_combiner,
              allowed_lateness=Duration(seconds=2*24*60*60)) # 2 days
{{< /highlight >}}

When you set `.withAllowedLateness` on a `PCollection`, that allowed lateness
propagates forward to any subsequent `PCollection` derived from the first
`PCollection` you applied allowed lateness to. If you want to change the allowed
lateness later in your pipeline, you must do so explicitly by applying
`Window.configure().withAllowedLateness()`.

### 8.5. Adding timestamps to a PCollection's elements {#adding-timestamps-to-a-pcollections-elements}

An unbounded source provides a timestamp for each element. Depending on your
unbounded source, you may need to configure how the timestamp is extracted from
the raw data stream.

However, bounded sources (such as a file from `TextIO`) do not provide
timestamps. If you need timestamps, you must add them to your `PCollection`’s
elements.

You can assign new timestamps to the elements of a `PCollection` by applying a
[ParDo](#pardo) transform that outputs new elements with timestamps that you
set.

An example might be if your pipeline reads log records from an input file, and
each log record includes a timestamp field; since your pipeline reads the
records in from a file, the file source doesn't assign timestamps automatically.
You can parse the timestamp field from each record and use a `ParDo` transform
with a `DoFn` to attach the timestamps to each element in your `PCollection`.

{{< highlight java >}}
      PCollection<LogEntry> unstampedLogs = ...;
      PCollection<LogEntry> stampedLogs =
          unstampedLogs.apply(ParDo.of(new DoFn<LogEntry, LogEntry>() {
            public void processElement(@Element LogEntry element, OutputReceiver<LogEntry> out) {
              // Extract the timestamp from log entry we're currently processing.
              Instant logTimeStamp = extractTimeStampFromLogEntry(element);
              // Use OutputReceiver.outputWithTimestamp (rather than
              // OutputReceiver.output) to emit the entry with timestamp attached.
              out.outputWithTimestamp(element, logTimeStamp);
            }
          }));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" setting_timestamp >}}
{{< /highlight >}}

## 9. Triggers {#triggers}

When collecting and grouping data into windows, Beam uses **triggers** to
determine when to emit the aggregated results of each window (referred to as a
*pane*). If you use Beam's default windowing configuration and [default
trigger](#default-trigger), Beam outputs the aggregated result when it
[estimates all data has arrived](#watermarks-and-late-data), and discards all
subsequent data for that window.

You can set triggers for your `PCollection`s to change this default behavior.
Beam provides a number of pre-built triggers that you can set:

*   **Event time triggers**. These triggers operate on the event time, as
    indicated by the timestamp on each data element. Beam's default trigger is
    event time-based.
*   **Processing time triggers**. These triggers operate on the processing time
    -- the time when the data element is processed at any given stage in the
    pipeline.
*   **Data-driven triggers**. These triggers operate by examining the data as it
    arrives in each window, and firing when that data meets a certain property.
    Currently, data-driven triggers only support firing after a certain number
    of data elements.
*   **Composite triggers**. These triggers combine multiple triggers in various
    ways.

At a high level, triggers provide two additional capabilities compared to simply
outputting at the end of a window:

*   Triggers allow Beam to emit early results, before all the data in a given
    window has arrived. For example, emitting after a certain amount of time
    elapses, or after a certain number of elements arrives.
*   Triggers allow processing of late data by triggering after the event time
    watermark passes the end of the window.

These capabilities allow you to control the flow of your data and balance
between different factors depending on your use case:

*   **Completeness:** How important is it to have all of your data before you
    compute your result?
*   **Latency:** How long do you want to wait for data? For example, do you wait
    until you think you have all data? Do you process data as it arrives?
*   **Cost:** How much compute power/money are you willing to spend to lower the
    latency?

For example, a system that requires time-sensitive updates might use a strict
time-based trigger that emits a window every *N* seconds, valuing promptness
over data completeness. A system that values data completeness more than the
exact timing of results might choose to use Beam's default trigger, which fires
at the end of the window.

You can also set a trigger for an unbounded `PCollection` that uses a [single
global window for its windowing function](#windowing). This can be useful when
you want your pipeline to provide periodic updates on an unbounded data set —
for example, a running average of all data provided to the present time, updated
every N seconds or every N elements.

### 9.1. Event time triggers {#event-time-triggers}

The `AfterWatermark` trigger operates on *event time*. The `AfterWatermark`
trigger emits the contents of a window after the
[watermark](#watermarks-and-late-data) passes the end of the window, based on the
timestamps attached to the data elements. The watermark is a global progress
metric, and is Beam's notion of input completeness within your pipeline at any
given point. <span class="language-java">`AfterWatermark.pastEndOfWindow()`</span>
<span class="language-py">`AfterWatermark`</span> *only* fires when the
watermark passes the end of the window.

In addition, you can configure triggers that fire if your pipeline receives data
before or after the end of the window.

The following example shows a billing scenario, and uses both early and late
firings:

{{< highlight java >}}
  // Create a bill at the end of the month.
  AfterWatermark.pastEndOfWindow()
      // During the month, get near real-time estimates.
      .withEarlyFirings(
          AfterProcessingTime
              .pastFirstElementInPane()
              .plusDuration(Duration.standardMinutes(1))
      // Fire on any late data so the bill can be corrected.
      .withLateFirings(AfterPane.elementCountAtLeast(1))
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_early_late_triggers >}}
{{< /highlight >}}

#### 9.1.1. Default trigger {#default-trigger}

The default trigger for a `PCollection` is based on event time, and emits the
results of the window when the Beam's watermark passes the end of the window,
and then fires each time late data arrives.

However, if you are using both the default windowing configuration and the
default trigger, the default trigger emits exactly once, and late data is
discarded. This is because the default windowing configuration has an allowed
lateness value of 0. See the Handling Late Data section for information about
modifying this behavior.

### 9.2. Processing time triggers {#processing-time-triggers}

The `AfterProcessingTime` trigger operates on *processing time*. For example,
the <span class="language-java">`AfterProcessingTime.pastFirstElementInPane()`</span>
<span class="language-py">`AfterProcessingTime`</span> trigger emits a window
after a certain amount of processing time has passed since data was received.
The processing time is determined by the system clock, rather than the data
element's timestamp.

The `AfterProcessingTime` trigger is useful for triggering early results from a
window, particularly a window with a large time frame such as a single global
window.

### 9.3. Data-driven triggers {#data-driven-triggers}

Beam provides one data-driven trigger,
<span class="language-java">`AfterPane.elementCountAtLeast()`</span>
<span class="language-py">`AfterCount`</span>. This trigger works on an element
count; it fires after the current pane has collected at least *N* elements. This
allows a window to emit early results (before all the data has accumulated),
which can be particularly useful if you are using a single global window.

It is important to note that if, for example, you specify
<span class="language-java">`.elementCountAtLeast(50)`</span>
<span class="language-py">AfterCount(50)</span> and only 32 elements arrive,
those 32 elements sit around forever. If the 32 elements are important to you,
consider using [composite triggers](#composite-triggers) to combine multiple
conditions. This allows you to specify multiple firing conditions such as "fire
either when I receive 50 elements, or every 1 second".

### 9.4. Setting a trigger {#setting-a-trigger}

When you set a windowing function for a `PCollection` by using the
<span class="language-java">`Window`</span><span class="language-py">`WindowInto`</span>
transform, you can also specify a trigger.

{{< paragraph class="language-java" >}}
You set the trigger(s) for a `PCollection` by invoking the method
`.triggering()` on the result of your `Window.into()` transform. This code
sample sets a time-based trigger for a `PCollection`, which emits results one
minute after the first element in that window has been processed.  The last line
in the code sample, `.discardingFiredPanes()`, sets the window's **accumulation
mode**.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
You set the trigger(s) for a `PCollection` by setting the `trigger` parameter
when you use the `WindowInto` transform. This code sample sets a time-based
trigger for a `PCollection`, which emits results one minute after the first
element in that window has been processed. The `accumulation_mode` parameter
sets the window's **accumulation mode**.
{{< /paragraph >}}

{{< highlight java >}}
  PCollection<String> pc = ...;
  pc.apply(Window.<String>into(FixedWindows.of(1, TimeUnit.MINUTES))
                               .triggering(AfterProcessingTime.pastFirstElementInPane()
                                                              .plusDelayOf(Duration.standardMinutes(1)))
                               .discardingFiredPanes());
{{< /highlight >}}

{{< highlight py >}}
  pcollection | WindowInto(
    FixedWindows(1 * 60),
    trigger=AfterProcessingTime(1 * 60),
    accumulation_mode=AccumulationMode.DISCARDING)
{{< /highlight >}}

#### 9.4.1. Window accumulation modes {#window-accumulation-modes}

When you specify a trigger, you must also set the the window's **accumulation
mode**. When a trigger fires, it emits the current contents of the window as a
pane. Since a trigger can fire multiple times, the accumulation mode determines
whether the system *accumulates* the window panes as the trigger fires, or
*discards* them.

{{< paragraph class="language-java" >}}
To set a window to accumulate the panes that are produced when the trigger
fires, invoke`.accumulatingFiredPanes()` when you set the trigger. To set a
window to discard fired panes, invoke `.discardingFiredPanes()`.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
To set a window to accumulate the panes that are produced when the trigger
fires, set the `accumulation_mode` parameter to `ACCUMULATING` when you set the
trigger. To set a window to discard fired panes, set `accumulation_mode` to
`DISCARDING`.
{{< /paragraph >}}

Let's look an example that uses a `PCollection` with fixed-time windowing and a
data-based trigger. This is something you might do if, for example, each window
represented a ten-minute running average, but you wanted to display the current
value of the average in a UI more frequently than every ten minutes. We'll
assume the following conditions:

*   The `PCollection` uses 10-minute fixed-time windows.
*   The `PCollection` has a repeating trigger that fires every time 3 elements
    arrive.

The following diagram shows data events for key X as they arrive in the
PCollection and are assigned to windows. To keep the diagram a bit simpler,
we'll assume that the events all arrive in the pipeline in order.

![Diagram of data events for acculumating mode example](/images/trigger-accumulation.png)

##### 9.4.1.1. Accumulating mode {#accumulating-mode}

If our trigger is set to accumulating mode, the trigger emits the following
values each time it fires. Keep in mind that the trigger fires every time three
elements arrive:

```
  First trigger firing:  [5, 8, 3]
  Second trigger firing: [5, 8, 3, 15, 19, 23]
  Third trigger firing:  [5, 8, 3, 15, 19, 23, 9, 13, 10]
```


##### 9.4.1.2. Discarding mode {#discarding-mode}

If our trigger is set to discarding mode, the trigger emits the following values
on each firing:

```
  First trigger firing:  [5, 8, 3]
  Second trigger firing:           [15, 19, 23]
  Third trigger firing:                         [9, 13, 10]
```

#### 9.4.2. Handling late data {#handling-late-data}


If you want your pipeline to process data that arrives after the watermark
passes the end of the window, you can apply an *allowed lateness* when you set
your windowing configuration. This gives your trigger the opportunity to react
to the late data. If allowed lateness is set, the default trigger will emit new
results immediately whenever late data arrives.

You set the allowed lateness by using `.withAllowedLateness()` when you set your
windowing function:

{{< highlight java >}}
  PCollection<String> pc = ...;
  pc.apply(Window.<String>into(FixedWindows.of(1, TimeUnit.MINUTES))
                              .triggering(AfterProcessingTime.pastFirstElementInPane()
                                                             .plusDelayOf(Duration.standardMinutes(1)))
                              .withAllowedLateness(Duration.standardMinutes(30));
{{< /highlight >}}

{{< highlight py >}}
  pc = [Initial PCollection]
  pc | beam.WindowInto(
            FixedWindows(60),
            trigger=AfterProcessingTime(60),
            allowed_lateness=1800) # 30 minutes
     | ...

{{< /highlight >}}

This allowed lateness propagates to all `PCollection`s derived as a result of
applying transforms to the original `PCollection`. If you want to change the
allowed lateness later in your pipeline, you can apply
`Window.configure().withAllowedLateness()` again, explicitly.


### 9.5. Composite triggers {#composite-triggers}

You can combine multiple triggers to form **composite triggers**, and can
specify a trigger to emit results repeatedly, at most once, or under other
custom conditions.

#### 9.5.1. Composite trigger types {#composite-trigger-types}

Beam includes the following composite triggers:

*   You can add additional early firings or late firings to
    `AfterWatermark.pastEndOfWindow` via `.withEarlyFirings` and
    `.withLateFirings`.
*   `Repeatedly.forever` specifies a trigger that executes forever. Any time the
    trigger's conditions are met, it causes a window to emit results and then
    resets and starts over. It can be useful to combine `Repeatedly.forever`
    with `.orFinally` to specify a condition that causes the repeating trigger
    to stop.
*   `AfterEach.inOrder` combines multiple triggers to fire in a specific
    sequence. Each time a trigger in the sequence emits a window, the sequence
    advances to the next trigger.
*   `AfterFirst` takes multiple triggers and emits the first time *any* of its
    argument triggers is satisfied. This is equivalent to a logical OR operation
    for multiple triggers.
*   `AfterAll` takes multiple triggers and emits when *all* of its argument
    triggers are satisfied. This is equivalent to a logical AND operation for
    multiple triggers.
*   `orFinally` can serve as a final condition to cause any trigger to fire one
    final time and never fire again.

#### 9.5.2. Composition with AfterWatermark {#composite-afterwatermark}

Some of the most useful composite triggers fire a single time when Beam
estimates that all the data has arrived (i.e. when the watermark passes the end
of the window) combined with either, or both, of the following:

*   Speculative firings that precede the watermark passing the end of the window
    to allow faster processing of partial results.

*   Late firings that happen after the watermark passes the end of the window,
    to allow for handling late-arriving data

You can express this pattern using `AfterWatermark`. For example, the following
example trigger code fires on the following conditions:

*   On Beam's estimate that all the data has arrived (the watermark passes the
    end of the window)

*   Any time late data arrives, after a ten-minute delay

{{< paragraph class="language-java" >}}
*   After two days, we assume no more data of interest will arrive, and the
    trigger stops executing
{{< /paragraph >}}

{{< highlight java >}}
  .apply(Window
      .configure()
      .triggering(AfterWatermark
           .pastEndOfWindow()
           .withLateFirings(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(10))))
      .withAllowedLateness(Duration.standardDays(2)));
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_composite_triggers >}}
{{< /highlight >}}

#### 9.5.3. Other composite triggers {#other-composite-triggers}

You can also build other sorts of composite triggers. The following example code
shows a simple composite trigger that fires whenever the pane has at least 100
elements, or after a minute.

{{< highlight java >}}
  Repeatedly.forever(AfterFirst.of(
      AfterPane.elementCountAtLeast(100),
      AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" model_other_composite_triggers >}}
{{< /highlight >}}

## 10. Metrics {#metrics}
In the Beam model, metrics provide some insight into the current state of a user pipeline,
potentially while the pipeline is running. There could be different reasons for that, for instance:
*   Check the number of errors encountered while running a specific step in the pipeline;
*   Monitor the number of RPCs made to backend service;
*   Retrieve an accurate count of the number of elements that have been processed;
*   ...and so on.

### 10.1 The main concepts of Beam metrics
*   **Named**. Each metric has a name which consists of a namespace and an actual name. The
    namespace can be used to differentiate between multiple metrics with the same name and also
    allows querying for all metrics within a specific namespace.
*   **Scoped**. Each metric is reported against a specific step in the pipeline, indicating what
    code was running when the metric was incremented.
*   **Dynamically Created**. Metrics may be created during runtime without pre-declaring them, in
    much the same way a logger could be created. This makes it easier to produce metrics in utility
    code and have them usefully reported.
*   **Degrade Gracefully**. If a runner doesn’t support some part of reporting metrics, the
    fallback behavior is to drop the metric updates rather than failing the pipeline. If a runner
    doesn’t support some part of querying metrics, the runner will not return the associated data.

Reported metrics are implicitly scoped to the transform within the pipeline that reported them.
This allows reporting the same metric name in multiple places and identifying the value each
transform reported, as well as aggregating the metric across the entire pipeline.

> **Note:** It is runner-dependent whether metrics are accessible during pipeline execution or only
after jobs have completed.

### 10.2 Types of metrics {#types-of-metrics}
There are three types of metrics that are supported for the moment: `Counter`, `Distribution` and
`Gauge`.

**Counter**: A metric that reports a single long value and can be incremented or decremented.

{{< highlight java >}}
Counter counter = Metrics.counter( "namespace", "counter1");

@ProcessElement
public void processElement(ProcessContext context) {
  // count the elements
  counter.inc();
  ...
}
{{< /highlight >}}

**Distribution**: A metric that reports information about the distribution of reported values.

{{< highlight java >}}
Distribution distribution = Metrics.distribution( "namespace", "distribution1");

@ProcessElement
public void processElement(ProcessContext context) {
  Integer element = context.element();
    // create a distribution (histogram) of the values
    distribution.update(element);
    ...
}
{{< /highlight >}}

**Gauge**: A metric that reports the latest value out of reported values. Since metrics are
collected from many workers the value may not be the absolute last, but one of the latest values.

{{< highlight java >}}
Gauge gauge = Metrics.gauge( "namespace", "gauge1");

@ProcessElement
public void processElement(ProcessContext context) {
  Integer element = context.element();
  // create a gauge (latest value received) of the values
  gauge.set(element);
  ...
}
{{< /highlight >}}

### 10.3 Querying metrics {#querying-metrics}
`PipelineResult` has a method `metrics()` which returns a `MetricResults` object that allows
accessing metrics. The main method available in `MetricResults` allows querying for all metrics
matching a given filter.

{{< highlight java >}}
public interface PipelineResult {
  MetricResults metrics();
}

public abstract class MetricResults {
  public abstract MetricQueryResults queryMetrics(@Nullable MetricsFilter filter);
}

public interface MetricQueryResults {
  Iterable<MetricResult<Long>> getCounters();
  Iterable<MetricResult<DistributionResult>> getDistributions();
  Iterable<MetricResult<GaugeResult>> getGauges();
}

public interface MetricResult<T> {
  MetricName getName();
  String getStep();
  T getCommitted();
  T getAttempted();
}
{{< /highlight >}}

### 10.4 Using metrics in pipeline {#using-metrics}
Below, there is a simple example of how to use a `Counter` metric in a user pipeline.

{{< highlight java >}}
// creating a pipeline with custom metrics DoFn
pipeline
    .apply(...)
    .apply(ParDo.of(new MyMetricsDoFn()));

pipelineResult = pipeline.run().waitUntilFinish(...);

// request the metric called "counter1" in namespace called "namespace"
MetricQueryResults metrics =
    pipelineResult
        .metrics()
        .queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named("namespace", "counter1"))
                .build());

// print the metric value - there should be only one line because there is only one metric
// called "counter1" in the namespace called "namespace"
for (MetricResult<Long> counter: metrics.getCounters()) {
  System.out.println(counter.getName() + ":" + counter.getAttempted());
}

public class MyMetricsDoFn extends DoFn<Integer, Integer> {
  private final Counter counter = Metrics.counter( "namespace", "counter1");

  @ProcessElement
  public void processElement(ProcessContext context) {
    // count the elements
    counter.inc();
    context.output(context.element());
  }
}
{{< /highlight >}}

### 10.5 Export metrics {#export-metrics}

Beam metrics can be exported to external sinks. If a metrics sink is set up in the configuration, the runner will push metrics to it at a default 5s period.
The configuration is held in the [MetricsOptions](https://beam.apache.org/releases/javadoc/2.19.0/org/apache/beam/sdk/metrics/MetricsOptions.html) class.
It contains push period configuration and also sink specific options such as type and URL. As for now only the REST HTTP and the Graphite sinks are supported and only
Flink and Spark runners support metrics export.

Also Beam metrics are exported to inner Spark and Flink dashboards to be consulted in their respective UI.

## 11. State and Timers {#state-and-timers}

Beam's windowing and triggering facilities provide a powerful abstraction for grouping and aggregating unbounded input
data based on timestamps. However there are aggregation use cases for which developers may require a higher degree of
control than provided by windows and triggers. Beam provides an API for manually managing per-key state, allowing for
fine-grained control over aggregations.

Beam's state API models state per key. To use the state API, you start out with a keyed `PCollection`, which in Java
is modeled as a `PCollection<KV<K, V>>`. A `ParDo` processing this `PCollection` can now declare state variables. Inside
the `ParDo` these state variables can be used to write or update state for the current key or to read previous state
written for that key. State is always fully scoped only to the current processing key.

Windowing can still be used together with stateful processing. All state for a key is scoped to the current window. This
means that the first time a key is seen for a given window any state reads will return empty, and that a runner can
garbage collect state when a window is completed. It's also often useful to use Beam's windowed aggregations prior to
the stateful operator. For example, using a combiner to preaggregate data, and then storing aggregated data inside of
state. Merging windows are not currently supported when using state and timers.

Sometimes stateful processing is used to implement state-machine style processing inside a `DoFn`. When doing this,
care must be taken to remember that the elements in input PCollection have no guaranteed order and to ensure that the
program logic is resilient to this. Unit tests written using the DirectRunner will shuffle the order of element
processing, and are recommended to test for correctness.

{{< paragraph class="language-java" >}}
In Java DoFn declares states to be accessed by creating final `StateSpec` member variables representing each state. Each
state must be named using the `StateId` annotation; this name is unique to a ParDo in the graph and has no relation
to other nodes in the graph. A `DoFn` can declare multiple state variables.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
In Python DoFn declares states to be accessed by creating `StateSpec` class member variables representing each state. Each
`StateSpec` is initialized with a name, this name is unique to a ParDo in the graph and has no relation
to other nodes in the graph. A `DoFn` can declare multiple state variables.
{{< /paragraph >}}

### 11.1 Types of state {#types-of-state}

Beam provides several types of state:

#### ValueState

A ValueState is a scalar state value. For each key in the input, a ValueState will store a typed value that can be
read and modified inside the DoFn's `@ProcessElement` or `@OnTimer` methods. If the type of the ValueState has a coder
registered, then Beam will automatically infer the coder for the state value. Otherwise, a coder can be explicitly
specified when creating the ValueState. For example, the following ParDo creates a  single state variable that
accumulates the number of elements seen.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @StateId("state") private final StateSpec<ValueState<Integer>> numElements = StateSpecs.value();

  @ProcessElement public void process(@StateId("state") ValueState<Integer> state) {
    // Read the number element seen so far for this user key.
    // state.read() returns null if it was never set. The below code allows us to have a default value of 0.
    int currentValue = MoreObjects.firstNonNull(state.read(), 0);
    // Update the state.
    state.write(currentValue + 1);
  }
}));
{{< /highlight >}}

Beam also allows explicitly specifying a coder for `ValueState` values. For example:

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @StateId("state") private final StateSpec<ValueState<MyType>> numElements = StateSpecs.value(new MyTypeCoder());
                 ...
}));
{{< /highlight >}}

#### CombiningState

`CombiningState` allows you to create a state object that is updated using a Beam combiner. For example, the previous
`ValueState` example could be rewritten to use `CombiningState`

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @StateId("state") private final StateSpec<CombiningState<Integer, int[], Integer>> numElements =
      StateSpecs.combining(Sum.ofIntegers());

  @ProcessElement public void process(@StateId("state") ValueState<Integer> state) {
    state.add(1);
  }
}));
{{< /highlight >}}

{{< highlight python >}}
class CombiningStateDoFn(DoFn):
  SUM_TOTAL = CombiningValueStateSpec('total', sum)

  def process(self, element, state=SoFn.StateParam(SUM_TOTAL)):
    state.add(1)

_ = (p | 'Read per user' >> ReadPerUser()
       | 'Combine state pardo' >> beam.ParDo(CombiningStateDofn()))
{{< /highlight >}}

#### BagState

A common use case for state is to accumulate multiple elements. `BagState` allows for accumulating an unordered set
of elements. This allows for addition of elements to the collection without requiring the reading of the entire
collection first, which is an efficiency gain. In addition, runners that support paged reads can allow individual
bags larger than available memory.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @StateId("state") private final StateSpec<BagState<ValueT>> numElements = StateSpecs.bag();

  @ProcessElement public void process(
    @Element KV<String, ValueT> element,
    @StateId("state") BagState<ValueT> state) {
    // Add the current element to the bag for this key.
    state.add(element.getValue());
    if (shouldFetch()) {
      // Occasionally we fetch and process the values.
      Iterable<ValueT> values = state.read();
      processValues(values);
      state.clear();  // Clear the state for this key.
    }
  }
}));
{{< /highlight >}}

{{< highlight python >}}
class BagStateDoFn(DoFn):
  ALL_ELEMENTS = BagStateSpec('buffer', coders.VarIntCoder())

  def process(self, element_pair, state=DoFn.StateParam(ALL_ELEMENTS)):
    state.add(element_pair[1])
    if should_fetch():
      all_elements = list(state.read())
      process_values(all_elements)
      state.clear()

_ = (p | 'Read per user' >> ReadPerUser()
       | 'Bag state pardo' >> beam.ParDo(BagStateDoFn()))
{{< /highlight >}}

### 11.2 Deferred state reads {#deferred-state-reads}

When a `DoFn` contains multiple state specifications, reading each one in order can be slow. Calling the `read()` function
on a state can cause the runner to perform a blocking read. Performing multiple blocking reads in sequence adds latency
to element processing. If you know that a state will always be read, you can annotate it as @AlwaysFetched, and then the
runner can prefetch all of the states necessary. For example:

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
   @StateId("state1") private final StateSpec<ValueState<Integer>> state1 = StateSpecs.value();
   @StateId("state2") private final StateSpec<ValueState<String>> state2 = StateSpecs.value();
   @StateId("state3") private final StateSpec<BagState<ValueT>> state3 = StateSpecs.bag();

  @ProcessElement public void process(
    @AlwaysFetched @StateId("state1") ValueState<Integer> state1,
    @AlwaysFetched @StateId("state2") ValueState<String> state2,
    @AlwaysFetched @StateId("state3") BagState<ValueT> state3) {
    state1.read();
    state2.read();
    state3.read();
  }
}));
{{< /highlight >}}

If however there are code paths in which the states are not fetched, then annotating with @AlwaysFetched will add
unnecessary fetching for those paths. In this case, the readLater method allows the runner to know that the state will
be read in the future, allowing multiple state reads to be batched together.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @StateId("state1") private final StateSpec<ValueState<Integer>> state1 = StateSpecs.value();
  @StateId("state2") private final StateSpec<ValueState<String>> state2 = StateSpecs.value();
  @StateId("state3") private final StateSpec<BagState<ValueT>> state3 = StateSpecs.bag();

  @ProcessElement public void process(
    @StateId("state1") ValueState<Integer> state1,
    @StateId("state2") ValueState<String> state2,
    @StateId("state3") BagState<ValueT> state3) {
    if (/* should read state */) {
      state1.readLater();
      state2.readLater();
      state3.readLater();
    }

    // The runner can now batch all three states into a single read, reducing latency.
    processState1(state1.read());
    processState2(state2.read());
    processState3(state3.read());
  }
}));
{{< /highlight >}}

### 11.3 Timers {#timers}

Beam provides a per-key timer callback API. This allows for delayed processing of data stored using the state API.
Timers can be set to callback at either an event-time or a processing-time timestamp. Every timer is identified with a
TimerId. A given timer for a key can only be set for a single timestamp. Calling set on a timer overwrites the previous
firing time for that key's timer.

#### 11.3.1 Event-time timers {#event-time-timers}

Event-time timers fire when the input watermark for the DoFn passes the time at which the timer is set, meaning that
the runner believes that there are no more elements to be processed with timestamps before the timer timestamp. This
allows for event-time aggregations.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @StateId("state") private final StateSpec<ValueState<Integer>> state = StateSpecs.value();
  @TimerId("timer") private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement public void process(
      @Element KV<String, ValueT> element,
      @Timestamp Instant elementTs,
      @StateId("state") ValueState<Integer> state,
      @TimerId("timer") Timer timer) {
     ...
     // Set an event-time timer to the element timestamp.
     timer.set(elementTs);
  }

   @OnTimer("timer") public void onTimer() {
      //Process timer.
   }
}));
{{< /highlight >}}

{{< highlight python >}}
class EventTimerDoFn(DoFn):
  ALL_ELEMENTS = BagStateSpec('buffer', coders.VarIntCoder())
  TIMER = TimerSpec('timer', TimeDomain.WATERMARK)

  def process(self,
              element_pair,
              t = DoFn.TimestampParam,
              buffer = DoFn.StateParam(ALL_ELEMENTS),
              timer = DoFn.TimerParam(TIMER)):
    buffer.add(element_pair[1])
    # Set an event-time timer to the element timestamp.
    timer.set(t)

  @on_timer(TIMER)
  def expiry_callback(self, buffer = DoFn.StateParam(ALL_ELEMENTS)):
    state.clear()

_ = (p | 'Read per user' >> ReadPerUser()
       | 'EventTime timer pardo' >> beam.ParDo(EventTimerDoFn()))
{{< /highlight >}}

#### 11.3.2 Processing-time timers {#processing-time-timers}

Processing-time timers fire when the real wall-clock time passes. This is often used to create larger batches of data
before processing. It can also be used to schedule events that should occur at a specific time. Just like with
event-time timers, processing-time timers are per key - each key has a separate copy of the timer.

While processing-time timers can be set to an absolute timestamp, it is very common to set them to an offset relative
to the current time. In Java, the `Timer.offset` and `Timer.setRelative` methods can be used to accomplish this.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @TimerId("timer") private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @ProcessElement public void process(@TimerId("timer") Timer timer) {
     ...
     // Set a timer to go off 30 seconds in the future.
     timer.offset(Duration.standardSeconds(30)).setRelative();
  }

   @OnTimer("timer") public void onTimer() {
      //Process timer.
   }
}));
{{< /highlight >}}

{{< highlight python >}}
class ProcessingTimerDoFn(DoFn):
  ALL_ELEMENTS = BagStateSpec('buffer', coders.VarIntCoder())
  TIMER = TimerSpec('timer', TimeDomain.REAL_TIME)

  def process(self,
              element_pair,
              buffer = DoFn.StateParam(ALL_ELEMENTS),
              timer = DoFn.TimerParam(TIMER)):
    buffer.add(element_pair[1])
    # Set a timer to go off 30 seconds in the future.
    timer.set(Timestamp.now() + Duration(seconds=30))

  @on_timer(TIMER)
  def expiry_callback(self, buffer = DoFn.StateParam(ALL_ELEMENTS)):
    # Process timer.
    state.clear()

_ = (p | 'Read per user' >> ReadPerUser()
       | 'ProcessingTime timer pardo' >> beam.ParDo(ProcessingTimerDoFn()))
{{< /highlight >}}

#### 11.3.3 Dynamic timer tags {#dynamic-timer-tags}

Beam also supports dynamically setting a timer tag using `TimerMap`. This allows for setting multiple different timers
in a `DoFn` and allowing for the timer tags to be dynamically chosen - e.g. based on data in the input elements. A
timer with a specific tag can only be set to a single timestamp, so setting the timer again has the effect of
overwriting the previous expiration time for the timer with that tag. Each `TimerMap` is identified with a timer family
id, and timers in different timer families are independent.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @TimerFamily("actionTimers") private final TimerSpec timer =
    TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

  @ProcessElement public void process(
      @Element KV<String, ValueT> element,
      @Timestamp Instant elementTs,
      @TimerFamily("actionTimers") TimerMap timers) {
     timers.set(element.getValue().getActionType(), elementTs);
  }

   @OnTimerFamily("actionTimers") public void onTimer(@TimerId String timerId) {
     LOG.info("Timer fired with id " + timerId);
   }
}));
{{< /highlight >}}

{{< highlight python >}}
To be supported, See BEAM-9602
{{< /highlight >}}
#### 11.3.4 Timer output timestamps {#timer-output-timestamps}

By default, event-time timers will hold the output watermark of the `ParDo` to the timestamp of the timer. This means
that if a timer is set to 12pm, any windowed aggregations or event-time timers later in the pipeline graph that finish
after 12pm will not expire. The timestamp of the timer is also the default output timestamp for the timer callback. This
means that any elements output from the onTimer method will have a timestamp equal to the timestamp of the timer firing.
For processing-time timers, the default output timestamp and watermark hold is the value of the input watermark at the
time the timer was set.

In some cases, a DoFn needs to output timestamps earlier than the timer expiration time, and therefore also needs to
hold its output watermark to those timestamps. For example, consider the following pipeline that temporarily batches
records into state, and sets a timer to drain the state. This code may appear correct, but will not work properly.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  @StateId("elementBag") private final StateSpec<BagState<ValueT>> elementBag = StateSpecs.bag();
  @StateId("timerSet") private final StateSpec<ValueState<Boolean>> timerSet = StateSpecs.value();
  @TimerId("outputState") private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @ProcessElement public void process(
      @Element KV<String, ValueT> element,
      @StateId("elementBag") BagState<ValueT> elementBag,
      @StateId("timerSet") ValueState<Boolean> timerSet,
      @TimerId("outputState") Timer timer) {
    // Add the current element to the bag for this key.
    elementBag.add(element.getValue());
    if (!MoreObjects.firstNonNull(timerSet.read(), false)) {
      // If the timer is not current set, then set it to go off in a minute.
      timer.offset(Duration.standardMinutes(1)).setRelative();
      timerSet.write(true);
    }
  }

  @OnTimer("outputState") public void onTimer(
      @StateId("elementBag") BagState<ValueT> elementBag,
      @StateId("timerSet") ValueState<Boolean> timerSet,
      OutputReceiver<ValueT> output) {
    for (ValueT bufferedElement : elementBag.read()) {
      // Output each element.
      output.outputWithTimestamp(bufferedElement, bufferedElement.timestamp());
    }
    elementBag.clear();
    // Note that the timer has now fired.
    timerSet.clear();
  }
}));
{{< /highlight >}}

The problem with this code is that the ParDo is buffering elements, however nothing is preventing the watermark
from advancing past the timestamp of those elements, so all those elements might be dropped as late data. In order
to prevent this from happening, an output timestamp needs to be set on the timer to prevent the watermark from advancing
past the timestamp of the minimum element. The following code demonstrates this.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  // The bag of elements accumulated.
  @StateId("elementBag") private final StateSpec<BagState<ValueT>> elementBag = StateSpecs.bag();
  // The timestamp of the timer set.
  @StateId("timerTimestamp") private final StateSpec<ValueState<Long>> timerTimestamp = StateSpecs.value();
  // The minimum timestamp stored in the bag.
  @StateId("minTimestampInBag") private final StateSpec<CombiningState<Long, long[], Long>>
     minTimestampInBag = StateSpecs.combining(Min.ofLongs());

  @TimerId("outputState") private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @ProcessElement public void process(
      @Element KV<String, ValueT> element,
      @StateId("elementBag") BagState<ValueT> elementBag,
      @AlwaysFetched @StateId("timerTimestamp") ValueState<Long> timerTimestamp,
      @AlwaysFetched @StateId("minTimestampInBag") CombiningState<Long, long[], Long> minTimestamp,
      @TimerId("outputState") Timer timer) {
    // Add the current element to the bag for this key.
    elementBag.add(element.getValue());
    // Keep track of the minimum element timestamp currently stored in the bag.
    minTimestamp.add(element.getValue().timestamp());

    // If the timer is already set, then reset it at the same time but with an updated output timestamp (otherwise
    // we would keep resetting the timer to the future). If there is no timer set, then set one to expire in a minute.
    Long timerTimestampMs = timerTimestamp.read();
    Instant timerToSet = (timerTimestamp.isEmpty().read())
        ? Instant.now().plus(Duration.standardMinutes(1)) : new Instant(timerTimestampMs);
    // Setting the outputTimestamp to the minimum timestamp in the bag holds the watermark to that timestamp until the
    // timer fires. This allows outputting all the elements with their timestamp.
    timer.withOutputTimestamp(minTimestamp.read()).set(timerToSet).
    timerTimestamp.write(timerToSet.getMillis());
  }

  @OnTimer("outputState") public void onTimer(
      @StateId("elementBag") BagState<ValueT> elementBag,
      @StateId("timerTimestamp") ValueState<Long> timerTimestamp,
      OutputReceiver<ValueT> output) {
    for (ValueT bufferedElement : elementBag.read()) {
      // Output each element.
      output.outputWithTimestamp(bufferedElement, bufferedElement.timestamp());
    }
    // Note that the timer has now fired.
    timerTimestamp.clear();
  }
}));
{{< /highlight >}}

### 11.4 Garbage collecting state {#garbage-collecting-state}
Per-key state needs to be garbage collected, or eventually the increasing size of state may negatively impact
performance. There are two common strategies for garbage collecting state.

##### 11.4.1 **Using windows for garbage collection** {#using-windows-for-garbage-collection}
All state and timers for a key is scoped to the window it is in. This means that depending on the timestamp of the
input element the ParDo will see different values for the state depending on the window that element falls into. In
addition, once the input watermark passes the end of the window, the runner should garbage collect all state for that
window. (note: if allowed lateness is set to a positive value for the window, the runner must wait for the watermark to
pass the end of the window plus the allowed lateness before garbage collecting state). This can be used as a
garbage-collection strategy.

For example, given the following:

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(Window.into(CalendarWindows.days(1)
   .withTimeZone(DateTimeZone.forID("America/Los_Angeles"))));
       .apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
           @StateId("state") private final StateSpec<ValueState<Integer>> state = StateSpecs.value();
                              ...
           @ProcessElement public void process(@Timestamp Instant ts, @StateId("state") ValueState<Integer> state) {
              // The state is scoped to a calendar day window. That means that if the input timestamp ts is after
              // midnight PST, then a new copy of the state will be seen for the next day.
           }
         }));
{{< /highlight >}}

{{< highlight python >}}
class StateDoFn(DoFn):
  ALL_ELEMENTS = BagStateSpec('buffer', coders.VarIntCoder())

  def process(self,
              element_pair,
              buffer = DoFn.StateParam(ALL_ELEMENTS)):
    ...

_ = (p | 'Read per user' >> ReadPerUser()
       | 'Windowing' >> beam.WindowInto(FixedWindows(60 * 60 * 24))
       | 'DoFn' >> beam.ParDo(StateDoFn()))
{{< /highlight >}}

This `ParDo` stores state per day. Once the pipeline is done processing data for a given day, all the state for that
day is garbage collected.

##### 11.4.1 **Using timers For garbage collection** {#using-timers-for-garbage-collection}

In some cases, it is difficult to find a windowing strategy that models the desired garbage-collection strategy. For
example, a common desire is to garbage collect state for a key once no activity has been seen on the key for some time.
This can be done by updating a timer that garbage collects state. For example

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  // The state for the key.
  @StateId("state") private final StateSpec<ValueState<ValueT>> state = StateSpecs.value();

  // The maximum element timestamp seen so far.
  @StateId("maxTimestampSeen") private final StateSpec<CombiningState<Long, long[], Long>>
     maxTimestamp = StateSpecs.combining(Max.ofLongs());

  @TimerId("gcTimer") private final TimerSpec gcTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement public void process(
      @Element KV<String, ValueT> element,
      @Timestamp Instant ts,
      @StateId("state") ValueState<ValueT> state,
      @StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestamp,
      @TimerId("gcTimer") gcTimer) {
    updateState(state, element);
    maxTimestamp.add(ts.getMillis());

    // Set the timer to be one hour after the maximum timestamp seen. This will keep overwriting the same timer, so
    // as long as there is activity on this key the state will stay active. Once the key goes inactive for one hour's
    // worth of event time (as measured by the watermark), then the gc timer will fire.
    Instant expirationTime = new Instant(maxTimestamp.read()).plus(Duration.standardHours(1));
    timer.set(expirationTime);
  }

  @OnTimer("gcTimer") public void onTimer(
      @StateId("state") ValueState<ValueT> state,
      @StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestamp) {
       // Clear all state for the key.
       state.clear();
       maxTimestamp.clear();
    }
 }
{{< /highlight >}}

{{< highlight python >}}
class UserDoFn(DoFn):
  ALL_ELEMENTS = BagStateSpec('state', coders.VarIntCoder())
  MAX_TIMESTAMP = CombiningValueStateSpec('max_timestamp_seen', max)
  TIMER = TimerSpec('gc-timer', TimeDomain.WATERMARK)

  def process(self,
              element,
              t = DoFn.TimestampParam,
              state = DoFn.StateParam(ALL_ELEMENTS),
              max_timestamp = DoFn.StateParam(MAX_TIMESTAMP),
              timer = DoFn.TimerParam(TIMER)):
    update_state(state, element)
    max_timestamp.add(t.micros)

    # Set the timer to be one hour after the maximum timestamp seen. This will keep overwriting the same timer, so
    # as long as there is activity on this key the state will stay active. Once the key goes inactive for one hour's
    # worth of event time (as measured by the watermark), then the gc timer will fire.
    expiration_time = Timestamp(micros=max_timestamp.read()) + Duration(seconds=60*60)
    timer.set(expiration_time)

  @on_timer(TIMER)
  def expiry_callback(self,
                      state = DoFn.StateParam(ALL_ELEMENTS),
                      max_timestamp = DoFn.StateParam(MAX_TIMESTAMP)):
    state.clear()
    max_timestamp.clear()


_ = (p | 'Read per user' >> ReadPerUser()
       | 'User DoFn' >> beam.ParDo(UserDoFn()))
{{< /highlight >}}

### 11.5 State and timers examples {#state-timers-examples}

Following are some example uses of state and timers

#### 11.5.1. Joining clicks and views {#joining-clicks-and-views}

In this example, the pipeline is processing data from an e-commerce site's home page. There are two input streams:
a stream of views, representing suggested product links displayed to the user on the home page, and a stream of
clicks, representing actual user clicks on these links. The goal of the pipeline is to join click events with view
events, outputting a new joined event that contains information from both events. Each link has a unique identifier
that is present in both the view event and the join event.

Many view events will never be followed up with clicks. This pipeline will wait one hour for a click, after which it
will give up on this join. While every click event should have a view event, some small number of view events may be
lost and never make it to the Beam pipeline; the pipeline will similarly wait one hour after seeing a click event, and
give up if the view event does not arrive in that time. Input events are not ordered - it is possible to see the click
event before the view event. The one hour join timeout should be based on event time, not on processing time.

{{< highlight java >}}
// Read the event stream and key it by the link id.
PCollection<KV<String, Event>> eventsPerLinkId =
    readEvents()
    .apply(WithKeys.of(Event::getLinkId).withKeyType(TypeDescriptors.strings()));

perUser.apply(ParDo.of(new DoFn<KV<String, Event>, JoinedEvent>() {
  // Store the view event.
  @StateId("view") private final StateSpec<ValueState<Event>> viewState = StateSpecs.value();
  // Store the click event.
  @StateId("click") private final StateSpec<ValueState<Event>> clickState = StateSpecs.value();

  // The maximum element timestamp seen so far.
  @StateId("maxTimestampSeen") private final StateSpec<CombiningState<Long, long[], Long>>
     maxTimestamp = StateSpecs.combining(Max.ofLongs());

  // Timer that fires when an hour goes by with an incomplete join.
  @TimerId("gcTimer") private final TimerSpec gcTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement public void process(
      @Element KV<String, Event> element,
      @Timestamp Instant ts,
      @AlwaysFetched @StateId("view") ValueState<Event> viewState,
      @AlwaysFetched @StateId("click") ValueState<Event> clickState,
      @AlwaysFetched @StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestampState,
      @TimerId("gcTimer") gcTimer,
      OutputReceiver<JoinedEvent> output) {
    // Store the event into the correct state variable.
    Event event = element.getValue();
    ValueState<Event> valueState = event.getType().equals(VIEW) ? viewState : clickState;
    valueState.write(event);

    Event view = viewState.read();
    Event click = clickState.read();
    (if view != null && click != null) {
      // We've seen both a view and a click. Output a joined event and clear state.
      output.output(JoinedEvent.of(view, click));
      clearState(viewState, clickState, maxTimestampState);
    } else {
       // We've only seen on half of the join.
       // Set the timer to be one hour after the maximum timestamp seen. This will keep overwriting the same timer, so
       // as long as there is activity on this key the state will stay active. Once the key goes inactive for one hour's
       // worth of event time (as measured by the watermark), then the gc timer will fire.
        maxTimestampState.add(ts.getMillis());
       Instant expirationTime = new Instant(maxTimestampState.read()).plus(Duration.standardHours(1));
       gcTimer.set(expirationTime);
    }
  }

  @OnTimer("gcTimer") public void onTimer(
      @StateId("view") ValueState<Event> viewState,
      @StateId("click") ValueState<Event> clickState,
      @StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestampState) {
       // An hour has gone by with an incomplete join. Give up and clear the state.
       clearState(viewState, clickState, maxTimestampState);
    }

    private void clearState(
      @StateId("view") ValueState<Event> viewState,
      @StateId("click") ValueState<Event> clickState,
      @StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestampState) {
      viewState.clear();
      clickState.clear();
      maxTimestampState.clear();
    }
 }));
{{< /highlight >}}

#### 11.5.2 Batching RPCs {#batching-rpcs}

In this example, input elements are being forwarded to an external RPC service. The RPC accepts batch requests -
multiple events for the same user can be batched in a single RPC call. Since this RPC service also imposes rate limits,
we want to batch ten seconds worth of events together in order to reduce the number of calls.

{{< highlight java >}}
PCollection<KV<String, ValueT>> perUser = readPerUser();
perUser.apply(ParDo.of(new DoFn<KV<String, ValueT>, OutputT>() {
  // Store the elements buffered so far.
  @StateId("state") private final StateSpec<BagState<ValueT>> elements = StateSpecs.bag();
  // Keep track of whether a timer is currently set or not.
  @StateId("isTimerSet") private final StateSpec<ValueState<Boolean>> isTimerSet = StateSpecs.value();
  // The processing-time timer user to publish the RPC.
  @TimerId("outputState") private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @ProcessElement public void process(
    @Element KV<String, ValueT> element,
    @StateId("state") BagState<ValueT> elementsState,
    @StateId("isTimerSet") ValueState<Boolean> isTimerSetState,
    @TimerId("outputState") Timer timer) {
    // Add the current element to the bag for this key.
    state.add(element.getValue());
    if (!MoreObjects.firstNonNull(isTimerSetState.read(), false)) {
      // If there is no timer currently set, then set one to go off in 10 seconds.
      timer.offset(Duration.standardSeconds(10)).setRelative();
      isTimerSetState.write(true);
   }
  }

  @OnTimer("outputState") public void onTimer(
    @StateId("state") BagState<ValueT> elementsState,
    @StateId("isTimerSet") ValueState<Boolean> isTimerSetState) {
    // Send an RPC containing the batched elements and clear state.
    sendRPC(elementsState.read());
    elementsState.clear();
    isTimerSetState.clear();
  }
}));
{{< /highlight >}}
