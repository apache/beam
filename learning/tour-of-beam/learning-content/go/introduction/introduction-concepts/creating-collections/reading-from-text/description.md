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
# Tour of Beam Programming Guide

The Beam Programming Guide is intended for Beam users who want to use the Beam SDKs to create data processing pipelines. This guide provides guidance for using the Beam SDK classes to build and test pipelines. The programming guide is not intended to be an exhaustive reference, but rather a language-agnostic, high-level guide to programmatically building your Beam pipeline. As the programming guide is filled out, the text will include code samples in multiple languages to help illustrate how to implement Beam concepts in your pipelines.

For a brief introduction to Beam’s basic concepts,take a look at the Basics of the Beam model page before reading the programming guide.

### Overview

To use Beam, you first need to first create a driver program using the classes in one of the Beam SDKs. Your driver program defines your pipeline, including all of the inputs, transforms, and outputs. It also sets execution options for your pipeline (typically passed by using command-line options). These include the Pipeline Runner, which, in turn, determines what back-end your pipeline will run on.

The Beam SDKs provide several abstractions that simplify the mechanics of large-scale distributed data processing. The same Beam abstractions work with both batch and streaming data sources. When you create your Beam pipeline, you can think about your data processing task in terms of these abstractions. They include:

→ `Pipeline`: A Pipeline encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data. All Beam driver programs must create a Pipeline. When you create the Pipeline, you must also specify the execution options that tell the Pipeline where and how to run.

→ `PCollection`: A PCollection represents a distributed data set that your Beam pipeline operates on. The data set can be bounded, meaning it comes from a fixed source like a file, or unbounded, meaning it comes from a continuously updating source via a subscription or other mechanism. Your pipeline typically creates an initial PCollection by reading data from an external data source, but you can also create a PCollection from in-memory data within your driver program. From there, PCollections are the inputs and outputs for each step in your pipeline.

→ `PTransform`: A PTransform represents a data processing operation, or a step, in your pipeline. Every PTransform takes one or more PCollection objects as the input, performs a processing function that you provide on the elements of that PCollection, and then produces zero or more output PCollection objects.

→ `Scope`: The Go SDK has an explicit scope variable used to build a `Pipeline`. A Pipeline can return it’s root scope with the `Root()` method. The scope variable is then passed to `PTransform` functions that place them in the `Pipeline` that owns the `Scope`.

→ `I/O transforms`: Beam comes with a number of “IOs” - library PTransforms that read or write data to various external storage systems.

A typical Beam driver program works as follows:

→ Create a Pipeline object and set the pipeline execution options, including the Pipeline Runner.

→ Create an initial `PCollection` for pipeline data, either using the IOs to read data from an external storage system, or using a Create transform to build a `PCollection` from in-memory data.

→ Apply `PTransforms` to each `PCollection`. Transforms can change, filter, group, analyze, or otherwise process the elements in a PCollection. A transform creates a new output PCollection without modifying the input collection. A typical pipeline applies subsequent transforms to each new output PCollection in turn until the processing is complete. However, note that a pipeline does not have to be a single straight line of transforms applied one after another: think of PCollections as variables and PTransforms as functions applied to these variables: the shape of the pipeline can be an arbitrarily complex processing graph.

→ Use IOs to write the final, transformed PCollection(s) to an external source.

→ Run the pipeline using the designated Pipeline Runner.

When you run your Beam driver program, the Pipeline Runner that you designate constructs a workflow graph of your pipeline based on the PCollection objects you’ve created and the transforms that you’ve applied. That graph is then executed using the appropriate distributed processing back-end, becoming an asynchronous “job” (or equivalent) on that back-end.

### Creating a pipeline

The `Pipeline` abstraction encapsulates all the data and steps in your data processing task. Your Beam driver program typically starts by constructing a Pipeline object, and then using that object as the basis for creating the pipeline’s data sets as PCollections and its operations as `Transforms`.

To use Beam, your driver program must first create an instance of the Beam SDK class Pipeline (typically in the main() function). When you create your `Pipeline`, you’ll also need to set some configuration options. You can set your pipeline’s configuration options programmatically, but it’s often easier to set the options ahead of time (or read them from the command line) and pass them to the Pipeline object when you create the object.

```
// beam.Init() is an initialization hook that must be called
// near the beginning of main(), before creating a pipeline.
beam.Init()

// Create the Pipeline object and root scope.
pipeline, scope := beam.NewPipelineWithRoot()
```

### Configuring pipeline options

Use the pipeline options to configure different aspects of your pipeline, such as the pipeline runner that will execute your pipeline and any runner-specific configuration required by the chosen runner. Your pipeline options will potentially include information such as your project ID or a location for storing files.

### Setting PipelineOptions from command-line arguments

Use Go flags to parse command line arguments to configure your pipeline. Flags must be parsed before `beam.Init()` is called.

```
// If beamx or Go flags are used, flags must be parsed first,
// before beam.Init() is called.
flag.Parse()
```

This interprets command-line arguments this follow the format:

```
--<option>=<value>
```

### Creating custom options

You can add your own custom options in addition to the standard `PipelineOptions`.

The following example shows how to add `input` and `output` custom options:

```
// Use standard Go flags to define pipeline options.
var (
	input  = flag.String("input", "", "")
	output = flag.String("output", "", "")
)
```

You can also specify a description, which appears when a user passes `--help` as a command-line argument, and a default value.

```
var (
	input  = flag.String("input", "gs://my-bucket/input", "Input for the pipeline")
	output = flag.String("output", "gs://my-bucket/output", "Output for the pipeline")
)
```

### Creating PCollection

Now that you know how to create a Beam pipeline and pass parameters into it, it is time to learn how to create an initial `PCollection` and fill it with data.

There are several options on how to do that:

→ You can create a PCollection of data stored in an in-memory collection class in your driver program.

→ You can also read the data from a variety of external sources such as local and cloud-based files, databases, or other sources using Beam-provided I/O adapters

Through the tour, most of the examples use either `PCollection` created from in-memory data or data read from one of the cloud buckets: beam-examples, dataflow-samples. These buckets contain sample data sets specifically created for educational purposes.

We encourage you to take a look, explore these data sets and use them while learning Apache Beam.

### Creating a PCollection from in-memory data

You can use the Beam-provided Create transform to create a `PCollection` from an in-memory Go Collection. You can apply Create transform directly to your Pipeline object itself.

The following example code shows how to do this:

```
func main() {
	ctx := context.Background()

    // First create pipline
	p, s := beam.NewPipelineWithRoot()

    //Now create the PCollection using list of strings
	numbers := beam.Create(s, "To", "be", "or", "not", "to", "be","that", "is", "the", "question")

    //Create a numerical PCollection
	numbers := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

}
```

You can find the complete code of this example in the playground window you can run and experiment with.

One of the differences you will notice is that it also contains the part to output `PCollection` elements to the console. Don’t worry if you don’t quite understand it, as the concept of `ParDo` transform will be explained later in the course. Feel free, however, to use it in exercises and challenges to explore results.

Do you also notice in what order elements of PCollection appear in the console? Why is that? You can also run the example several times to see if the output stays the same or changes.

### Reading from text file

You use one of the Beam-provided I/O adapters to read from an external source. The adapters vary in their exact usage, but all of them read from some external data source and return a `PCollection` whose elements represent the data records in that source.

Each data source adapter has a Read transform; to read, you must apply that transform to the Pipeline object itself.

`TextIO.Read` , for example, reads from an external text file and returns a `PCollection` whose elements are of type String. Each String represents one line from the text file. Here’s how you would apply `TextIO.Read` to your Pipeline to create a `PCollection`:

```
func main() {
	ctx := context.Background()

    // First create pipline
	p, s := beam.NewPipelineWithRoot()

    // Now create the PCollection by reading text files. Separate elements will be added for each line in the input file
    lines :=  textio.Read(scope, 'gs://some/inputData.txt')

}
```

In the playground window, you can find an example that reads a king lear poem from the text file stored in the Google Storage bucket and fills PCollection with individual lines and then with individual words. Try it out and see what the output is.

One of the differences you will see is that the output is much shorter than the input file itself. This is because the number of elements in the output `PCollection` is limited with the `Sample.fixedSizeGlobally` transform. Use Sample.fixedSizeGlobally transform of is another technique you can use to troubleshoot and limit the output sent to the console for debugging purposes in case of large input datasets.

### Read from csv file

Data processing pipelines often work with tabular data. In many examples and challenges throughout the course, you’ll be working with one of the datasets stored as csv files in either beam-examples, dataflow-samples buckets.

Loading data from csv file requires some processing and consists of two main part:
* Loading text lines using `TextIO.Read` transform
* Parsing lines of text into tabular format