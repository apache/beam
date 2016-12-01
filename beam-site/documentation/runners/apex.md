---
layout: default
title: "Apache Apex Runner"
permalink: /documentation/runners/apex/
---
# Using the Apache Apex Runner

The Apex Runner executes Apache Beam pipelines using [Apache Apex](http://apex.apache.org/) as an underlying engine. The runner has broad support for the [Beam model and supports streaming and batch pipelines]({{ site.baseurl }}/documentation/runners/capability-matrix/).

[Apache Apex](http://apex.apache.org/) is a stream processing platform and framework for low-latency, high-throughput and fault-tolerant analytics applications on Apache Hadoop. Apex has a unified streaming architecture and can be used for real-time and batch processing. With its stateful stream processing architecture, Apex can support all of the concepts in the Beam model (event time, triggers, watermarks etc.).

## Apex-Runner prerequisites and setup

You may set up your own Hadoop cluster,  and [setup Apache Apex on top of it](http://apex.apache.org/docs/apex/apex_development_setup/) or choose any vendor-specific distribution that includes Hadoop and Apex pre-installed. Please see the [distribution information on the Apache Apex website](http://apex.apache.org/downloads.html).

## Running wordcount using Apex-Runner

Download some data for processing and put it on HDFS
```
curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > /tmp/kinglear.txt
hdfs dfs -mkdir -p /tmp/input/
hdfs dfs -put /tmp/kinglear.txt /tmp/input/
```

The output directory should not exist on HDFS. Delete it if it exists.
```
hdfs dfs -rm -r -f /tmp/output/
```

Run the wordcount example
```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--inputFile=/tmp/input/ --output=/tmp/output/ --runner=ApexRunner --embeddedExecution=false --configFile=beam-runners-apex.properties" -Papex-runner
```

This will launch an Apex application.

## Checking output

The sample program which is processing small amount of data would finish quickly. You can check contents on /tmp/output/ on HDFS
```
hdfs dfs -ls /tmp/output/
```

## Montoring progress of your job

Depending on your installation, you may be able to monitor the progress of your job on the Hadoop cluster. Alternately, you have folloing optoins:

* YARN : Using YARN web UI generally running on 8088 on the node running resource manager
* Apex cli: [Using apex cli to get running application information](http://apex.apache.org/docs/apex/apex_cli/#apex-cli-commands)

