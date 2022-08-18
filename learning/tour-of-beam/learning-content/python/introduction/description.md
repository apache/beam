# Tour of Beam Programming Guide

The Beam Programming Guide is intended for Beam users who want to use the Beam SDKs to create data processing pipelines. It provides guidance for using the Beam SDK classes to build and test your pipeline. The programming guide is not intended as an exhaustive reference, but as a language-agnostic, high-level guide to programmatically building your Beam pipeline. As the programming guide is filled out, the text will include code samples in multiple languages to help illustrate how to implement Beam concepts in your pipelines.

If you want a brief introduction to Beam’s basic concepts before reading the programming guide, take a look at the Basics of the Beam model page.

### Overview

To use Beam, you need to first create a driver program using the classes in one of the Beam SDKs. Your driver program defines your pipeline, including all of the inputs, transforms, and outputs; it also sets execution options for your pipeline (typically passed in using command-line options). These include the Pipeline Runner, which, in turn, determines what back-end your pipeline will run on.

The Beam SDKs provide a number of abstractions that simplify the mechanics of large-scale distributed data processing. The same Beam abstractions work with both batch and streaming data sources. When you create your Beam pipeline, you can think about your data processing task in terms of these abstractions. They include:

&#8594; Pipeline: A Pipeline encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data. All Beam driver programs must create a Pipeline. When you create the Pipeline, you must also specify the execution options that tell the Pipeline where and how to run.

&#8594; PCollection: A PCollection represents a distributed data set that your Beam pipeline operates on. The data set can be bounded, meaning it comes from a fixed source like a file, or unbounded, meaning it comes from a continuously updating source via a subscription or other mechanism. Your pipeline typically creates an initial PCollection by reading data from an external data source, but you can also create a PCollection from in-memory data within your driver program. From there, PCollections are the inputs and outputs for each step in your pipeline.

&#8594; PTransform: A PTransform represents a data processing operation, or a step, in your pipeline. Every PTransform takes one or more PCollection objects as input, performs a processing function that you provide on the elements of that PCollection, and produces zero or more output PCollection objects.

&#8594; I/O transforms: Beam comes with a number of “IOs” - library PTransforms that read or write data to various external storage systems.

A typical Beam driver program works as follows:

&#8594; Create a Pipeline object and set the pipeline execution options, including the Pipeline Runner.

&#8594; Create an initial PCollection for pipeline data, either using the IOs to read data from an external storage system, or using a Create transform to build a PCollection from in-memory data.

&#8594; **Apply** PTransforms to each PCollection. Transforms can change, filter, group, analyze, or otherwise process the elements in a PCollection. A transform creates a new output PCollection without modifying the input collection. A typical pipeline applies subsequent transforms to each new output PCollection in turn until processing is complete. However, note that a pipeline does not have to be a single straight line of transforms applied one after another: think of PCollections as variables and PTransforms as functions applied to these variables: the shape of the pipeline can be an arbitrarily complex processing graph.

&#8594; Use IOs to write the final, transformed PCollection(s) to an external source.

&#8594; Run the pipeline using the designated Pipeline Runner.

When you run your Beam driver program, the Pipeline Runner that you designate constructs a workflow graph of your pipeline based on the PCollection objects you’ve created and transforms that you’ve applied. That graph is then executed using the appropriate distributed processing back-end, becoming an asynchronous “job” (or equivalent) on that back-end.

### Creating a pipeline

The `Pipeline` abstraction encapsulates all the data and steps in your data processing task. Your Beam driver program typically starts by constructing a Pipeline object, and then using that object as the basis for creating the pipeline’s data sets as PCollections and its operations as `Transforms`.

To use Beam, your driver program must first create an instance of the Beam SDK class Pipeline (typically in the main() function). When you create your `Pipeline`, you’ll also need to set some configuration options. You can set your pipeline’s configuration options programmatically, but it’s often easier to set the options ahead of time (or read them from the command line) and pass them to the Pipeline object when you create the object.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
  pass  # build your pipeline here
```

### Configuring pipeline options

Use the pipeline options to configure different aspects of your pipeline, such as the pipeline runner that will execute your pipeline and any runner-specific configuration required by the chosen runner. Your pipeline options will potentially include information such as your project ID or a location for storing files.

### Setting PipelineOptions from command-line arguments

While you can configure your pipeline by creating a `PipelineOptions` object and setting the fields directly, the Beam SDKs include a command-line parser that you can use to set fields in `PipelineOptions` using command-line arguments.

To read options from the command-line, construct your `PipelineOptions` object as demonstrated in the following example code:

```
from apache_beam.options.pipeline_options import PipelineOptions

beam_options = PipelineOptions()
```

This interprets command-line arguments that follow the format:

```
--<option>=<value>
```

Building your `PipelineOptions` this way lets you specify any of the options as a command-line argument.

> **Note**: The WordCount example pipeline demonstrates how to set pipeline options at runtime by using command-line options.

### Creating custom options

You can add your own custom options in addition to the standard `PipelineOptions`.

The following example shows how to add `input` and `output` custom options:

```
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input')
    parser.add_argument('--output')
```

You can also specify a description, which appears when a user passes `--help` as a command-line argument, and a default value.

You set the description and default value using annotations, as follows:

```
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='The file path for the input text to process.')
    parser.add_argument(
        '--output', required=True, help='The path prefix for output files.')
```

For Python, you can also simply parse your custom options with argparse; there is no need to create a separate PipelineOptions subclass.

Now your pipeline can accept `--input=value` and `--output=value` as command-line arguments.

