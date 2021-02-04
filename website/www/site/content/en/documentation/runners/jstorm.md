---
title: "JStorm Runner"
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
# Using the JStorm Runner

The JStorm Runner can be used to execute Beam pipelines using [JStorm](http://jstorm.io/), while providing:

* High throughput and low latency.
* At-least-once and exactly-once fault tolerance.

Like a native JStorm topology, users can execute Beam topology with local mode, standalone cluster or jstorm-on-yarn cluster.

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the currently supported capabilities of the JStorm Runner.

## JStorm Runner prerequisites and setup

The JStorm runner currently supports JStorm version 2.5.0-SNAPSHOT.

You can add a dependency on the latest version of the JStorm runner by adding the following to your pom.xml:

{{< highlight java >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-jstorm</artifactId>
  <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}

### Deploying JStorm with your application

To run against a Standalone cluster, you can package your program with all Beam dependencies into a fat jar, and then submit the topology with the following command.
```
jstorm jar WordCount.jar org.apache.beam.examples.WordCount --runner=org.apache.beam.runners.jstorm.JStormRunner
```

If you don't want to package a fat jar, you can upload the Beam dependencies onto all cluster nodes(`$JSTORM_HOME/lib/ext/beam`) first.
When you submit a topology with argument `"--external-libs beam"`, JStorm will load the Beam dependencies automatically.
```
jstorm jar WordCount.jar org.apache.beam.examples.WordCount --external-libs beam  --runner=org.apache.beam.runners.jstorm.JStormRunner
```

To learn about deploying a JStorm cluster, please refer to [JStorm cluster deploy](http://jstorm.io/QuickStart/Deploy/index.html)

## Pipeline options for the JStorm Runner

When executing your pipeline with the JStorm Runner, you should consider the following pipeline options.

<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>JStormRunner</code> to run using JStorm.</td>
</tr>
<tr>
  <td><code>topologyConfig</code></td>
  <td>System topology config of JStorm</td>
  <td>DefaultMapValueFactory.class</td>
</tr>
<tr>
  <td><code>workerNumber</code></td>
  <td>Worker number of topology</td>
  <td>1</td>
</tr>
<tr>
  <td><code>parallelism</code></td>
  <td>Global parallelism number of a component</td>
  <td>1</td>
</tr>
<tr>
  <td><code>parallelismMap</code></td>
  <td>Parallelism number of a specified composite PTransform</td>
  <td>DefaultMapValueFactory.class</td>
</tr>
<tr>
  <td><code>exactlyOnceTopology</code></td>
  <td>Indicate if it is an exactly once topology</td>
  <td>false</td>
</tr>
<tr>
  <td><code>localMode</code></td>
  <td>Indicate if the topology is running on local machine or distributed cluster</td>
  <td>false</td>
</tr>
<tr>
  <td><code>localModeExecuteTimeSec</code></td>
  <td>Executing time(sec) of topology on local mode.</td>
  <td>60</td>
</tr>
</table>

## Additional notes

### Monitoring your job
You can monitor your job with the JStorm UI, which displays all JStorm system metrics and Beam metrics.
For testing on local mode, you can retreive the Beam metrics with the metrics method of PipelineResult.
