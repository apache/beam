---
layout: default
title: "Beam Overview"
permalink: /get-started/beam-overview/
redirect_from:
  - /use/beam-overview/
  - /docs/use/beam-overview/
---

# Apache Beam Overview

Apache Beam is an open source, unified programming model that you can use to create a data processing **pipeline**. You start by building a program that defines the pipeline using one of the open source Beam SDKs. The pipeline is then executed by one of Beam's supported **distributed processing back-ends**, which include [Apache Apex](http://apex.apache.org), [Apache Flink](http://flink.apache.org), [Apache Spark](http://spark.apache.org), and [Google Cloud Dataflow](https://cloud.google.com/dataflow).

Beam is particularly useful for [Embarrassingly Parallel](http://en.wikipedia.org/wiki/Embarassingly_parallel) data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel. You can also use Beam for Extract, Transform, and Load (ETL) tasks and pure data integration. These tasks are useful for moving data between different storage media and data sources, transforming data into a more desirable format, or loading data onto a new system.

## Apache Beam SDKs

The Beam SDKs provide a unified programming model that can represent and transform data sets of any size, whether the input is a finite data set from a batch data source, or an infinite data set from a streaming data source. The Beam SDKs use the same classes to represent both bounded and unbounded data, and the same transforms to operate on that data. You use the Beam SDK of your choice to build a program that defines your data processing pipeline.

Beam currently supports the following language-specific SDKs:

<table class="table table-condensed">
<tr>
  <th>Language</th>
  <th>SDK Status</th>
</tr>
<tr>
  <td>Java</td>
  <td>Active Development</td>
</tr>
<tr>
  <td>Python</td>
  <td>Coming Soon</td>
</tr>
<tr>
  <td>Other</td>
  <td>TBD</td>
</tr>
</table>

## Apache Beam Pipeline Runners

The Beam Pipeline Runners translate the data processing pipeline you define with your Beam program into the API compatible with the distributed processing back-end of your choice. When you run your Beam program, you'll need to specify the appropriate runner for the back-end where you want to execute your pipeline.

Beam currently supports Runners that work with the following distributed processing back-ends:

<table class="table table-condensed">
<tr>
  <th>Runner</th>
  <th>Status</th>
</tr>
<tr>
  <td>Apache Apex</td>
  <td>In Development</td>
</tr>
<tr>
  <td>Apache Flink</td>
  <td>In Development</td>
</tr>
<tr>
  <td>Apache Spark</td>
  <td>In Development</td>
</tr>
<tr>
  <td>Google Cloud Dataflow</td>
  <td>In Development</td>
</tr>
</table>

**Note:** You can always execute your pipeline locally for testing and debugging purposes.

## Getting Started with Apache Beam

Get started using Beam for your data processing tasks by following the [Quickstart]({{ site.baseurl }}/get-started/quickstart) and the [WordCount Examples Walkthrough]({{ site.baseurl }}/get-started/wordcount-example).
