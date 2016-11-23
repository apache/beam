---
layout: default
title: "Create Your Pipeline"
permalink: /documentation/pipelines/create-your-pipeline/
---
# Create Your Pipeline

* TOC
{:toc}

Your Beam program expresses a data processing pipeline, from start to finish. This section explains the mechanics of using the classes in the Beam SDKs to build a pipeline. To construct a pipeline using the classes in the Beam SDKs, your program will need to perform the following general steps:

*   Create a `Pipeline` object.
*   Use a **Read** or **Create** transform to create one or more `PCollection`s for your pipeline data.
*   Apply **transforms** to each `PCollection`. Transforms can change, filter, group, analyze, or otherwise process the elements in a `PCollection`. Each transform creates a new output `PCollection`, to which you can apply additional transforms until processing is complete.
*   **Write** or otherwise output the final, transformed `PCollection`s.
*   **Run** the pipeline.

## Creating Your Pipeline Object

A Beam program often starts by creating a `Pipeline` object.

In the Beam SDKs, each pipeline is represented by an explicit object of type `Pipeline`. Each `Pipeline` object is an independent entity that encapsulates both the data the pipeline operates over and the transforms that get applied to that data.

To create a pipeline, declare a `Pipeline` object, and pass it some configuration options, which are explained in a section below. You pass the configuration options by creating an object of type `PipelineOptions`, which you can build by using the static method `PipelineOptionsFactory.create()`.

```java
// Start by defining the options for the pipeline.
PipelineOptions options = PipelineOptionsFactory.create();

// Then create the pipeline.
Pipeline p = Pipeline.create(options);
```

### Configuring Pipeline Options

Use the pipeline options to configure different aspects of your pipeline, such as the pipeline runner that will execute your pipeline and any runner-specific configuration required by the chosen runner. Your pipeline options will potentially include information such as your project ID or a location for storing files. 

When you run the pipeline on a runner of your choice, a copy of the PipelineOptions will be available to your code. For example, you can read PipelineOptions from a DoFn's Context.

#### Setting PipelineOptions from Command-Line Arguments

While you can configure your pipeline by creating a `PipelineOptions` object and setting the fields directly, the Beam SDKs include a command-line parser that you can use to set fields in `PipelineOptions` using command-line arguments.

To read options from the command-line, construct your `PipelineOptions` object as demonstrated in the following example code:

```java
MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
```

This interprets command-line arguments that follow the format:

```java
--<option>=<value>
```

> **Note:** Appending the method `.withValidation` will check for required command-line arguments and validate argument values.

Building your `PipelineOptions` this way lets you specify any of the options as a command-line argument.

> **Note:** The [WordCount example pipeline]({{ site.baseurl }}/get-started/wordcount-example) demonstrates how to set pipeline options at runtime by using command-line options.

#### Creating Custom Options

You can add your own custom options in addition to the standard `PipelineOptions`. To add your own options, define an interface with getter and setter methods for each option, as in the following example:

```java
public interface MyOptions extends PipelineOptions {
    String getMyCustomOption();
    void setMyCustomOption(String myCustomOption);
  }
```

You can also specify a description, which appears when a user passes `--help` as a command-line argument, and a default value.

You set the description and default value using annotations, as follows:

```java
public interface MyOptions extends PipelineOptions {
    @Description("My custom command line argument.")
    @Default.String("DEFAULT")
    String getMyCustomOption();
    void setMyCustomOption(String myCustomOption);
  }
```

It's recommended that you register your interface with `PipelineOptionsFactory` and then pass the interface when creating the `PipelineOptions` object. When you register your interface with `PipelineOptionsFactory`, the `--help` can find your custom options interface and add it to the output of the `--help` command. `PipelineOptionsFactory` will also validate that your custom options are compatible with all other registered options.

The following example code shows how to register your custom options interface with `PipelineOptionsFactory`:

```java
PipelineOptionsFactory.register(MyOptions.class);
MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                .withValidation()
                                                .as(MyOptions.class);
```

Now your pipeline can accept `--myCustomOption=value` as a command-line argument.

## Reading Data Into Your Pipeline

To create your pipeline's initial `PCollection`, you apply a root transform to your pipeline object. A root transform creates a `PCollection` from either an external data source or some local data you specify.

There are two kinds of root transforms in the Beam SDKs: `Read` and `Create`. `Read` transforms read data from an external source, such as a text file or a database table. `Create` transforms create a `PCollection` from an in-memory `java.util.Collection`.

The following example code shows how to `apply` a `TextIO.Read` root transform to read data from a text file. The transform is applied to a `Pipeline` object `p`, and returns a pipeline data set in the form of a `PCollection<String>`:

```java
PCollection<String> lines = p.apply(
  apply("ReadLines", TextIO.Read.from("gs://some/inputData.txt"));
```

## Applying Transforms to Process Pipeline Data

To use transforms in your pipeline, you **apply** them to the `PCollection` that you want to transform.

To apply a transform, you call the `apply` method on each `PCollection` that you want to process, passing the desired transform object as an argument.

The Beam SDKs contain a number of different transforms that you can apply to your pipeline's `PCollection`s. These include general-purpose core transforms, such as [ParDo]({{ site.baseurl }}/documentation/programming-guide/#transforms-pardo) or [Combine]({{ site.baseurl }}/documentation/programming-guide/#transforms-combine). There are also pre-written [composite transforms]({{ site.baseurl }}/documentation/programming-guide/#transforms-composite) included in the SDKs, which combine one or more of the core transforms in a useful processing pattern, such as counting or combining elements in a collection. You can also define your own more complex composite transforms to fit your pipeline's exact use case.

In the Beam Java SDK, each transform is a subclass of the base class `PTransform`. When you call `apply` on a `PCollection`, you pass the `PTransform` you want to use as an argument.

The following code shows how to `apply` a transform to a `PCollection` of strings. The transform is a user-defined custom transform that reverses the contents of each string and outputs a new `PCollection` containing the reversed strings.

The input is a `PCollection<String>` called `words`; the code passes an instance of a `PTransform` object called `ReverseWords` to `apply`, and saves the return value as the `PCollection<String>` called `reversedWords`.

```java
PCollection<String> words = ...;

PCollection<String> reversedWords = words.apply(new ReverseWords());
```

## Writing or Outputting Your Final Pipeline Data

Once your pipeline has applied all of its transforms, you'll usually need to output the results. To output your pipeline's final `PCollection`s, you apply a `Write` transform to that `PCollection`. `Write` transforms can output the elements of a `PCollection` to an external data sink, such as a database table. You can use `Write` to output a `PCollection` at any time in your pipeline, although you'll typically write out data at the end of your pipeline.

The following example code shows how to `apply` a `TextIO.Write` transform to write a `PCollection` of `String` to a text file:

```java
PCollection<String> filteredWords = ...;

filteredWords.apply("WriteMyFile", TextIO.Write.to("gs://some/outputData.txt"));
```

## Running Your Pipeline

Once you have constructed your pipeline, use the `run` method to execute the pipeline. Pipelines are executed asynchronously: the program you create sends a specification for your pipeline to a **pipeline runner**, which then constructs and runs the actual series of pipeline operations. 

```java
p.run();
```

The `run` method is asynchronous. If you'd like a blocking execution instead, run your pipeline appending the `waitUntilFinish` method:

```java
p.run().waitUntilFinish();
```

## What's next

*   [Test your pipeline]({{ site.baseurl }}/documentation/pipelines/test-your-pipeline).