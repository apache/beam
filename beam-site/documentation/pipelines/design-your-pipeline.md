---
layout: default
title: "Design Your Pipeline"
permalink: /documentation/pipelines/design-your-pipeline/
---
# Design Your Pipeline

* TOC
{:toc}

This page helps you design your Apache Beam pipeline. It includes information about how to determine your pipeline's structure, how to choose which transforms to apply to your data, and how to determine your input and output methods.

Before reading this section, it is recommended that you become familiar with the information in the [Beam programming guide]({{ site.baseurl }}/documentation/programming-guide).

## What to consider when designing your pipeline

When designing your Beam pipeline, consider a few basic questions:

*   **Where is your input data stored?** How many sets of input data do you have? This will determine what kinds of `Read` transforms you'll need to apply at the start of your pipeline.
*   **What does your data look like?** It might be plaintext, formatted log files, or rows in a database table. Some Beam transforms work exclusively on `PCollection`s of key/value pairs; you'll need to determine if and how your data is keyed and how to best represent that in your pipeline's `PCollection`(s).
*   **What do you want to do with your data?** The core transforms in the Beam SDKs are general purpose. Knowing how you need to change or manipulate your data will determine how you build core transforms like [ParDo]({{ site.baseurl }}/documentation/programming-guide/#transforms-pardo), or when you use pre-written transforms included with the Beam SDKs.
*   **What does your output data look like, and where should it go?** This will determine what kinds of `Write` transforms you'll need to apply at the end of your pipeline.

## A basic pipeline

The simplest pipelines represent a linear flow of operations, as shown in Figure 1 below:

<figure id="fig1">
    <img src="{{ site.baseurl }}/images/design-your-pipeline-linear.png"
         alt="A linear pipeline.">
</figure>
Figure 1: A linear pipeline.

However, your pipeline can be significantly more complex. A pipeline represents a [Directed Acyclic Graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) of steps. It can have multiple input sources, multiple output sinks, and its operations (transforms) can output multiple `PCollection`s. The following examples show some of the different shapes your pipeline can take.

## Branching PCollections

It's important to understand that transforms do not consume `PCollection`s; instead, they consider each individual element of a `PCollection` and create a new `PCollection` as output. This way, you can do different things to different elements in the same `PCollection`.

### Multiple transforms process the same PCollection

You can use the same `PCollection` as input for multiple transforms without consuming the input or altering it.

The pipeline illustrated in Figure 2 below reads its input, first names (Strings), from a single source, a database table, and creates a `PCollection` of table rows. Then, the pipeline applies multiple transforms to the **same** `PCollection`. Transform A extracts all the names in that `PCollection` that start with the letter 'A', and Transform B extracts all the names in that `PCollection` that start with the letter 'B'. Both transforms A and B have the same input `PCollection`.

<figure id="fig2">
    <img src="{{ site.baseurl }}/images/design-your-pipeline-multiple-pcollections.png"
         alt="A pipeline with multiple transforms. Note that the PCollection of table rows is processed by two transforms.">
</figure>
Figure 2: A pipeline with multiple transforms. Note that the PCollection of the database table rows is processed by two transforms.

### A single transform that uses side outputs

Another way to branch a pipeline is to have a **single** transform output to multiple `PCollection`s by using [side outputs]({{ site.baseurl }}/documentation/programming-guide/#transforms-sideio). Transforms that use side outputs, process each element of the input once, and allow you to output to zero or more `PCollection`s.

Figure 3 below illustrates the same example described above, but with one transform that uses a side output; Names that start with 'A' are added to the output `PCollection`, and names that start with 'B' are added to the side output `PCollection`.

<figure id="fig3">
    <img src="{{ site.baseurl }}/images/design-your-pipeline-side-outputs.png"
         alt="A pipeline with a transform that outputs multiple PCollections.">
</figure>
Figure 3: A pipeline with a transform that outputs multiple PCollections.

The pipeline in Figure 2 contains two transforms that process the elements in the same input `PCollection`. One transform uses the following logic pattern:

<pre>if (starts with 'A') { outputToPCollectionA }</pre>

while the other transform uses:

<pre>if (starts with 'B') { outputToPCollectionB }</pre>

Because each transform reads the entire input `PCollection`, each element in the input `PCollection` is processed twice.

The pipeline in Figure 3 performs the same operation in a different way - with only one transform that uses the logic

<pre>if (starts with 'A') { outputToPCollectionA } else if (starts with 'B') { outputToPCollectionB }</pre>

where each element in the input `PCollection` is processed once.

You can use either mechanism to produce multiple output `PCollection`s. However, using side outputs makes more sense if the transform's computation per element is time-consuming.

## Merging PCollections

Often, after you've branched your `PCollection` into multiple `PCollection`s via multiple transforms, you'll want to merge some or all of those resulting `PCollection`s back together. You can do so by using one of the following:

*   **Flatten** - You can use the `Flatten` transform in the Beam SDKs to merge multiple `PCollection`s of the **same type**.
*   **Join** - You can use the `CoGroupByKey` transform in the Beam SDK to perform a relational join between two `PCollection`s. The `PCollection`s must be keyed (i.e. they must be collections of key/value pairs) and they must use the same key type.

The example depicted in Figure 4 below is a continuation of the example illustrated in Figure 2 in the section above. After branching into two `PCollection`s, one with names that begin with 'A' and one with names that begin with 'B', the pipeline merges the two together into a single `PCollection` that now contains all names that begin with either 'A' or 'B'. Here, it makes sense to use `Flatten` because the `PCollection`s being merged both contain the same type.

<figure id="fig4">
    <img src="{{ site.baseurl }}/images/design-your-pipeline-flatten.png"
         alt="Part of a pipeline that merges multiple PCollections.">
</figure>
Figure 4: Part of a pipeline that merges multiple PCollections.

## Multiple sources

Your pipeline can read its input from one or more sources. If your pipeline reads from multiple sources and the data from those sources is related, it can be useful to join the inputs together. In the example illustrated in Figure 5 below, the pipeline reads names and addresses from a database table, and names and order numbers from a text file. The pipeline then uses `CoGroupByKey` to join this information, where the key is the name; the resulting `PCollection` contains all the combinations of names, addresses, and orders.

<figure id="fig5">
    <img src="{{ site.baseurl }}/images/design-your-pipeline-join.png"
         alt="A pipeline with multiple input sources.">
</figure>
Figure 5: A pipeline with multiple input sources.

## What's next

*   [Create your own pipeline]({{ site.baseurl }}/documentation/pipelines/create-your-pipeline).
*   [Test your pipeline]({{ site.baseurl }}/documentation/pipelines/test-your-pipeline).