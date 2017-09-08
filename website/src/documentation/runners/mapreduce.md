---
layout: default
title: "Apache Hadoop MapReduce Runner"
permalink: /documentation/runners/mapreduce/
redirect_from: /learn/runners/mapreduce/
---
# Using the Apache Hadoop MapReduce Runner

The Apache Hadoop MapReduce Runner can be used to execute Beam pipelines using [Apache Hadoop](http://hadoop.apache.org/).

The [Beam Capability Matrix]({{ site.baseurl }}/documentation/runners/capability-matrix/) documents the currently supported capabilities of the Apache Hadoop MapReduce Runner.

## Apache Hadoop MapReduce Runner prerequisites and setup
You need to have an Apache Hadoop environment with either [Single Node Setup](https://hadoop.apache.org/docs/r1.2.1/single_node_setup.html) or [Cluster Setup](https://hadoop.apache.org/docs/r1.2.1/cluster_setup.html)

The Apache Hadoop MapReduce runner currently supports Apache Hadoop 2.8.1 version.

You can add a dependency on the latest version of the Apache Hadoop MapReduce runner by adding to your pom.xml the following:
```java
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-mapreduce</artifactId>
  <version>{{ site.release_latest }}</version>
</dependency>
```

## Deploying Apache Hadoop MapReduce with your application
To execute in a local hadoop environment, use this command:
```
$ mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Pmapreduce-runner \
    -Dexec.args="--runner=MapReduceRunner \
      --inputFile=/path/to/pom.xml \
      --output=/path/to/counts \
      --fileOutputDir=<directory for intermediate outputs>"
```

To execute in a hadoop cluster, you need to package your program along will all dependencies in a so-called fat jar.

If you follow along the [Beam Quickstart]({{ site.baseurl }}/get-started/quickstart/) this is the command that you can run:
```
$ mvn package -Pflink-runner
```

For actually running the pipeline you would use this command
```
$ yarn jar word-count-beam-bundled-0.1.jar \
    org.apache.beam.examples.WordCount \
    --runner=MapReduceRunner \
    --inputFile=/path/to/pom.xml \
      --output=/path/to/counts \
      --fileOutputDir=<directory for intermediate outputs>"
```

## Pipeline options for the Apache Hadoop MapReduce Runner

When executing your pipeline with the Apache Hadoop MapReduce Runner, you should consider the following pipeline options.

<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>MapReduceRunner</code> to run using the Apache Hadoop MapReduce.</td>
</tr>
<tr>
  <td><code>jarClass</code></td>
  <td>The jar class of the user Beam program.</td>
  <td>JarClassInstanceFactory.class</td>
</tr>
<tr>
  <td><code>fileOutputDir</code></td>
  <td>The directory for files output.</td>
  <td>"/tmp/mapreduce/"</td>
</tr>
</table>
