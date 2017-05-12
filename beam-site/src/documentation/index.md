---
layout: default
title: "Learn about Beam"
permalink: /documentation/
redirect_from:
  - /learn/
  - /docs/learn/
---

# Apache Beam Documentation

This section provides in-depth conceptual information and reference material for the Beam Model, SDKs, and Runners:

## Concepts 

Learn about the Beam Programming Model and the concepts common to all Beam SDKs and Runners.

* The [Programming Guide]({{ site.baseurl }}/documentation/programming-guide/) introduces all the key Beam concepts.
* Visit [Additional Resources]({{ site.baseurl }}/documentation/resources/) for some of our favorite articles and talks about Beam. 

## Pipeline Fundamentals

* [Design Your Pipeline]({{ site.baseurl }}/documentation/pipelines/design-your-pipeline/) by planning your pipeline’s structure, choosing transforms to apply to your data, and determining your input and output methods.
* [Create Your Pipeline]({{ site.baseurl }}/documentation/pipelines/create-your-pipeline/) using the classes in the Beam SDKs.
* [Test Your Pipeline]({{ site.baseurl }}/documentation/pipelines/test-your-pipeline/) to minimize debugging a pipeline’s remote execution.

## SDKs

Find status and reference information on all of the available Beam SDKs.

* [Java SDK]({{ site.baseurl }}/documentation/sdks/java/) 
* [Python SDK]({{ site.baseurl }}/documentation/sdks/python/)

## Runners

A Beam Runner runs a Beam pipeline on a specific (often distributed) data processing system.

### Available Runners

* [DirectRunner]({{ site.baseurl }}/documentation/runners/direct/): Runs locally on your machine -- great for developing, testing, and debugging.
* [ApexRunner]({{ site.baseurl }}/documentation/runners/apex/): Runs on [Apache Apex](http://apex.apache.org).
* [FlinkRunner]({{ site.baseurl }}/documentation/runners/flink/): Runs on [Apache Flink](http://flink.apache.org).
* [SparkRunner]({{ site.baseurl }}/documentation/runners/spark/): Runs on [Apache Spark](http://spark.apache.org).
* [DataflowRunner]({{ site.baseurl }}/documentation/runners/dataflow/): Runs on [Google Cloud Dataflow](https://cloud.google.com/dataflow), a fully managed service within [Google Cloud Platform](https://cloud.google.com/).
* _[Under Development]_ [GearpumpRunner]({{ site.baseurl }}/contribute/work-in-progress/#feature-branches): Runs on [Apache Gearpump (incubating)](http://gearpump.apache.org). 

### Choosing a Runner

Beam is designed to enable pipelines to be portable across different runners. However, given every runner has different capabilities, they also have different abilities to implement the core concepts in the Beam model. The [Capability Matrix]({{ site.baseurl }}/documentation/runners/capability-matrix/) provides a detailed comparison of runner functionality.

Once you have chosen which runner to use, see that runner's page for more information about any initial runner-specific setup as well as any required or optional `PipelineOptions` for configuring it's execution. You may also want to refer back to the Quickstart for [Java]({{ site.baseurl }}/get-started/quickstart-java) or [Python]({{ site.baseurl }}/get-started/quickstart-py) for instructions on executing the sample WordCount pipeline.
