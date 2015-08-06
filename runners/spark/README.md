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
    <version>0.4.0</version>
    
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

## Word Count Example

First download a text document to use as input:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > /tmp/kinglear.txt

Then run the [word count example][wc] from the SDK using a single threaded Spark instance
in local mode:

    mvn exec:exec -DmainClass=com.google.cloud.dataflow.examples.WordCount \
      -Dinput=/tmp/kinglear.txt -Doutput=/tmp/out -Drunner=SparkPipelineRunner \
      -DsparkMaster=local

Check the output by running:

    head /tmp/out-00000-of-00001

__Note: running examples using `mvn exec:exec` only works for Spark local mode at the
moment. See the next section for how to run on a cluster.__

[wc]: https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WordCount.java

## Running on a Cluster

Spark Dataflow pipelines can be run on a cluster using the `spark-submit` command.

First copy a text document to HDFS:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt | hadoop fs -put - kinglear.txt

Then run the word count example using Spark submit with the `yarn-client` master
(`yarn-cluster` works just as well):

    spark-submit \
      --class com.google.cloud.dataflow.examples.WordCount \
      --master yarn-client \
      target/spark-dataflow-*-spark-app.jar \
        --input=kinglear.txt --output=out --runner=SparkPipelineRunner --sparkMaster=yarn-client

Check the output by running:

    hadoop fs -tail out-00000-of-00002

## How to Release

Committers can release the project using the standard [Maven Release Plugin](http://maven.apache.org/maven-release/maven-release-plugin/) commands:

    mvn release:prepare
    mvn release:perform -Darguments="-Dgpg.passphrase=XXX"

Note that you will need a [public GPG key](http://www.apache.org/dev/openpgp.html).

[![Build Status](https://travis-ci.org/cloudera/spark-dataflow.png?branch=master)](https://travis-ci.org/cloudera/spark-dataflow)
[![codecov.io](https://codecov.io/github/cloudera/spark-dataflow/coverage.svg?branch=master)](https://codecov.io/github/cloudera/spark-dataflow?branch=master)
