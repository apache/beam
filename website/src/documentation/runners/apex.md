---
layout: section
title: "Apache Apex Runner"
section_menu: section-menu/runners.html
permalink: /documentation/runners/apex/
---
# Using the Apache Apex Runner

The Apex Runner executes Apache Beam pipelines using [Apache Apex](http://apex.apache.org/) as an underlying engine. The runner has broad support for the [Beam model and supports streaming and batch pipelines]({{ site.baseurl }}/documentation/runners/capability-matrix/).

[Apache Apex](http://apex.apache.org/) is a stream processing platform and framework for low-latency, high-throughput and fault-tolerant analytics applications on Apache Hadoop. Apex has a unified streaming architecture and can be used for real-time and batch processing.

## Apex Runner prerequisites

You may set up your own Hadoop cluster. Beam does not require anything extra to launch the pipelines on YARN.
An optional Apex installation may be useful for monitoring and troubleshooting.
The Apex CLI can be [built](http://apex.apache.org/docs/apex/apex_development_setup/) or
obtained as [binary build](http://www.atrato.io/blog/2017/04/08/apache-apex-cli/).
For more download options see [distribution information on the Apache Apex website](http://apex.apache.org/downloads.html).

## Running wordcount using Apex Runner

Put data for processing into HDFS:
```
hdfs dfs -mkdir -p /tmp/input/
hdfs dfs -put pom.xml /tmp/input/
```

The output directory should not exist on HDFS:
```
hdfs dfs -rm -r -f /tmp/output/
```

Run the wordcount example (*example project needs to be modified to include HDFS file provider*)
```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--inputFile=/tmp/input/pom.xml --output=/tmp/output/ --runner=ApexRunner --embeddedExecution=false --configFile=beam-runners-apex.properties" -Papex-runner
```

The application will run asynchronously. Check status with `yarn application -list -appStates ALL`

The configuration file is optional, it can be used to influence how Apex operators are deployed into YARN containers.
The following example will reduce the number of required containers by collocating the operators into the same container
and lower the heap memory per operator - suitable for execution in a single node Hadoop sandbox.

```
apex.application.*.operator.*.attr.MEMORY_MB=64
apex.stream.*.prop.locality=CONTAINER_LOCAL
apex.application.*.operator.*.attr.TIMEOUT_WINDOW_COUNT=1200
```


## Checking output

Check the output of the pipeline in the HDFS location.
```
hdfs dfs -ls /tmp/output/
```

## Montoring progress of your job

Depending on your installation, you may be able to monitor the progress of your job on the Hadoop cluster. Alternatively, you have following options:

* YARN : Using YARN web UI generally running on 8088 on the node running resource manager.
* Apex command-line interface: [Using the Apex CLI to get running application information](http://apex.apache.org/docs/apex/apex_cli/#apex-cli-commands).
