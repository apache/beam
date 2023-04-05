---
type: runners
title: "Apache Hadoop MapReduce Runner"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Using the Apache Hadoop MapReduce Runner

The Apache Hadoop MapReduce Runner can be used to execute Beam pipelines using [Apache Hadoop](https://hadoop.apache.org/).

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the currently supported capabilities of the Apache Hadoop MapReduce Runner.

## Apache Hadoop MapReduce Runner prerequisites and setup
You need to have an Apache Hadoop environment with either [Single Node Setup](https://hadoop.apache.org/docs/r1.2.1/single_node_setup.html) or [Cluster Setup](https://hadoop.apache.org/docs/r1.2.1/cluster_setup.html)

The Apache Hadoop MapReduce runner currently supports Apache Hadoop version 2.8.1.

You can add a dependency on the latest version of the Apache Hadoop MapReduce runner by adding the following to your pom.xml:
```
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-mapreduce</artifactId>
  <version>{{< param release_latest >}}</version>
</dependency>
```

## Deploying Apache Hadoop MapReduce with your application
To execute in a local Hadoop environment, use this command:
```
$ mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Pmapreduce-runner \
    -Dexec.args="--runner=MapReduceRunner \
      --inputFile=/path/to/pom.xml \
      --output=/path/to/counts \
      --fileOutputDir=<directory for intermediate outputs>"
```

To execute in a Hadoop cluster, package your program along with all dependencies in a fat jar.

If you are following through the [Beam Java SDK Quickstart](/get-started/quickstart-java/), you can run this command:
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
  <td>Set to <code>MapReduceRunner</code> to run using Apache Hadoop MapReduce.</td>
</tr>
<tr>
  <td><code>jarClass</code></td>
  <td>The jar class of the user Beam program.</td>
  <td>JarClassInstanceFactory.class</td>
</tr>
<tr>
  <td><code>fileOutputDir</code></td>
  <td>The directory for output files.</td>
  <td>"/tmp/mapreduce/"</td>
</tr>
</table>
