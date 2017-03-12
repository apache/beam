<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

Spark Beam Runner (Spark-Runner)
================================

## Intro

The Spark-Runner allows users to execute data pipelines written against the Apache Beam API
with Apache Spark. This runner allows to execute both batch and streaming pipelines on top of the Spark engine.

## Overview

### Features

- ParDo
- GroupByKey
- Combine
- Windowing
- Flatten
- View
- Side inputs/outputs
- Encoding

### Fault-Tolerance

The Spark runner fault-tolerance guarantees the same guarantees as [Apache Spark](http://spark.apache.org/).

### Monitoring

The Spark runner supports user-defined counters via Beam Aggregators implemented on top of Spark's [Accumulators](http://spark.apache.org/docs/1.6.3/programming-guide.html#accumulators).  
The Aggregators (defined by the pipeline author) and Spark's internal metrics are reported using Spark's [metrics system](http://spark.apache.org/docs/1.6.3/monitoring.html#metrics).  
Spark also provides a web UI for monitoring, more details [here](http://spark.apache.org/docs/1.6.3/monitoring.html).

## Beam Model support

### Batch

The Spark runner provides full support for the Beam Model in batch processing via Spark [RDD](http://spark.apache.org/docs/1.6.3/programming-guide.html#resilient-distributed-datasets-rdds)s.

### Streaming

Providing full support for the Beam Model in streaming pipelines is under development. To follow-up you can subscribe to our [mailing list](http://beam.apache.org/get-started/support/).

### issue tracking

See [Beam JIRA](https://issues.apache.org/jira/browse/BEAM) (runner-spark)


## Getting Started

To get the latest version of the Spark Runner, first clone the Beam repository:

    git clone https://github.com/apache/beam

    
Then switch to the newly created directory and run Maven to build the Apache Beam:

    cd beam
    mvn clean install -DskipTests

Now Apache Beam and the Spark Runner are installed in your local maven repository.

If we wanted to run a Beam pipeline with the default options of a Spark instance in local mode, 
we would do the following:

    Pipeline p = <logic for pipeline creation >
    PipelineResult result = p.run();
    result.waitUntilFinish();

To create a pipeline runner to run against a different Spark cluster, with a custom master url we
would do the following:

    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setSparkMaster("spark://host:port");
    Pipeline p = <logic for pipeline creation >
    PipelineResult result = p.run();
    result.waitUntilFinish();

## Word Count Example

First download a text document to use as input:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > /tmp/kinglear.txt
    
Switch to the Spark runner directory:

    cd runners/spark
    
Then run the [word count example][wc] from the SDK using a Spark instance in local mode:

    mvn exec:exec -DmainClass=org.apache.beam.runners.spark.examples.WordCount \
          -Dinput=/tmp/kinglear.txt -Doutput=/tmp/out -Drunner=SparkRunner \
          -DsparkMaster=local

Check the output by running:

    head /tmp/out-00000-of-00001

__Note: running examples using `mvn exec:exec` only works for Spark local mode at the
moment. See the next section for how to run on a cluster.__

[wc]: https://github.com/apache/beam/blob/master/runners/spark/src/main/java/org/apache/beam/runners/spark/examples/WordCount.java
## Running on a Cluster

Spark Beam pipelines can be run on a cluster using the `spark-submit` command.

TBD pending native HDFS support (currently blocked by [BEAM-59](https://issues.apache.org/jira/browse/BEAM-59)).
