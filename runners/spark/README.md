spark-dataflow
==============

## Intro

Spark-dataflow allows users to execute data pipelines written against the Google Cloud Dataflow API
with Apache Spark. Spark-dataflow is an early prototype, and we'll be working on it continuously.
If this project interests you, we welcome issues, comments, and (especially!) pull requests.
To get an idea of what we have already identified as
areas that need improvement, checkout the issues listed in the github repo.

## Motivation

We had two primary goals when we started working on Spark-dataflow:

1. *Provide portability for data pipelines written for Google Cloud Dataflow.* Google makes
it really easy to get started writing pipelines against the Dataflow API, but they wanted
to be sure that creating a pipeline using their tools would not lock developers in to their
platform. A Spark-based implementation of Dataflow means that you can take your pipeline
logic with you wherever you go. This also means that any new machine learning and anomaly
detection algorithms that are developed against the Dataflow API are available to everyone,
regardless of their underlying execution platform.

2. *Experiment with new data pipeline design patterns.* The Dataflow API has a number of
interesting ideas, especially with respect to the unification of batch and stream data
processing into a single API that maps into two separate engines. The Dataflow streaming
engine, based on Google's [Millwheel](http://research.google.com/pubs/pub41378.html), does
not have a direct open source analogue, and we wanted to understand how to replicate its
functionality using frameworks like Spark Streaming.

## Getting Started

The Maven coordinates of the current version of this project are:

    <groupId>com.cloudera.dataflow.spark</groupId>
    <artifactId>spark-dataflow</artifactId>
    <version>0.0.1</version>
    
and are hosted in Cloudera's repository at:

    <repository>
      <id>cloudera.repo</id>
      <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
    </repository>

If we wanted to run a dataflow pipeline with the default options of a single threaded spark
instance in local mode, we would do the following:

    Pipeline p = <logic for pipeline creation >
    EvaluationResult result = SparkPipelineRunner.create().run(p);

To create a pipeline runner to run against a different spark cluster, with a custom master url we
would do the following:

    Pipeline p = <logic for pipeline creation >
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setSparkMaster("spark://host:port");
    EvaluationResult result = SparkPipelineRunner.create(options).run(p);

