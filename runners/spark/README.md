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

### Sources and Sinks

- Text
- Hadoop
- Avro
- Kafka

### Fault-Tolerance

The Spark runner fault-tolerance guarantees the same guarantees as [Apache Spark](http://spark.apache.org/).

### Monitoring

The Spark runner supports monitoring via Beam Aggregators implemented on top of Spark's [Accumulators](http://spark.apache.org/docs/latest/programming-guide.html#accumulators).  
Spark also provides a web UI for monitoring, more details [here](http://spark.apache.org/docs/latest/monitoring.html).

## Beam Model support

### Batch

The Spark runner provides support for batch processing of Beam bounded PCollections as Spark [RDD](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds)s.

### Streaming

The Spark runner currently provides partial support for stream processing of Beam unbounded PCollections as Spark [DStream](http://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams)s.  
Current implementation of *Windowing* takes the first window size in the pipeline and treats it as the Spark "batch interval", while following windows will be treated as *Processing Time* windows.  
Further work is required to provide full support for the Beam Model *event-time* and *out-of-order* stream processing.

### issue tracking

See [Beam JIRA](https://issues.apache.org/jira/browse/BEAM) (runner-spark)


## Getting Started

To get the latest version of the Spark Runner, first clone the Beam repository:

    git clone https://github.com/apache/incubator-beam

    
Then switch to the newly created directory and run Maven to build the Apache Beam:

    cd incubator-beam
    mvn clean install -DskipTests

Now Apache Beam and the Spark Runner are installed in your local maven repository.

If we wanted to run a Beam pipeline with the default options of a single threaded Spark
instance in local mode, we would do the following:

    Pipeline p = <logic for pipeline creation >
    EvaluationResult result = SparkPipelineRunner.create().run(p);

To create a pipeline runner to run against a different Spark cluster, with a custom master url we
would do the following:

    Pipeline p = <logic for pipeline creation >
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setSparkMaster("spark://host:port");
    EvaluationResult result = SparkPipelineRunner.create(options).run(p);

## Word Count Example

First download a text document to use as input:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > /tmp/kinglear.txt
    
Switch to the Spark runner directory:

    cd runners/spark
    
Then run the [word count example][wc] from the SDK using a single threaded Spark instance
in local mode:

    mvn exec:exec -DmainClass=com.google.cloud.dataflow.examples.WordCount \
      -Dinput=/tmp/kinglear.txt -Doutput=/tmp/out -Drunner=SparkPipelineRunner \
      -DsparkMaster=local

Check the output by running:

    head /tmp/out-00000-of-00001

__Note: running examples using `mvn exec:exec` only works for Spark local mode at the
moment. See the next section for how to run on a cluster.__

[wc]: https://github.com/apache/incubator-beam/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WordCount.java
## Running on a Cluster

Spark Beam pipelines can be run on a cluster using the `spark-submit` command.

First copy a text document to HDFS:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt | hadoop fs -put - kinglear.txt

Then run the word count example using Spark submit with the `yarn-client` master
(`yarn-cluster` works just as well):

    spark-submit \
      --class com.google.cloud.dataflow.examples.WordCount \
      --master yarn-client \
      target/spark-runner-*-spark-app.jar \
        --inputFile=kinglear.txt --output=out --runner=SparkPipelineRunner --sparkMaster=yarn-client

Check the output by running:

    hadoop fs -tail out-00000-of-00002
