---
layout: section
title: "Create Your Pipeline"
section_menu: section-menu/documentation.html
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

To create a pipeline, declare a `Pipeline` object, and pass it some [configuration options]({{ site.baseurl }}/documentation/programming-guide#configuring-pipeline-options).

```java
// Start by defining the options for the pipeline.
PipelineOptions options = PipelineOptionsFactory.create();

// Then create the pipeline.
Pipeline p = Pipeline.create(options);
```

## Reading Data Into Your Pipeline

To create your pipeline's initial `PCollection`, you apply a root transform to your pipeline object. A root transform creates a `PCollection` from either an external data source or some local data you specify.

There are two kinds of root transforms in the Beam SDKs: `Read` and `Create`. `Read` transforms read data from an external source, such as a text file or a database table. `Create` transforms create a `PCollection` from an in-memory `java.util.Collection`.

The following example code shows how to `apply` a `TextIO.Read` root transform to read data from a text file. The transform is applied to a `Pipeline` object `p`, and returns a pipeline data set in the form of a `PCollection<String>`:

```java
PCollection<String> lines = p.apply(
  "ReadLines", TextIO.read().from("gs://some/inputData.txt"));
```

## Applying Transforms to Process Pipeline Data

You can manipulate your data using the various [transforms]({{ site.baseurl }}/documentation/programming-guide/#transforms) provided in the Beam SDKs. To do this, you **apply** the trannsforms to your pipeline's `PCollection` by calling the `apply` method on each `PCollection` that you want to process and passing the desired transform object as an argument.

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

filteredWords.apply("WriteMyFile", TextIO.write().to("gs://some/outputData.txt"));
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

*   [Programming Guide]({{ site.baseurl }}/documentation/programming-guide) - Learn the details of creating your pipeline, configuring pipeline options, and applying transforms.
*   [Test your pipeline]({{ site.baseurl }}/documentation/pipelines/test-your-pipeline).
