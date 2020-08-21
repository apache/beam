---
type: runners
title: "Hazelcast Jet Runner"
aliases: /learn/runners/jet/
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

## Overview

The Hazelcast Jet Runner can be used to execute Beam pipelines using [Hazelcast
Jet](https://jet-start.sh/).

The Jet Runner and Jet are suitable for large scale continuous jobs and provide:
* Support for both batch (bounded) and streaming (unbounded) data sets
* A runtime that supports very high throughput and low event latency at the same time
* Natural back-pressure in streaming programs
* Distributed massively parallel data processing engine with in memory storage

It's important to note that the Jet Runner is currently in an *EXPERIMENTAL* state and can not make use of many of
the capabilities present in Jet:
* Jet has full Fault Tolerance support, the Jet Runner does not; if a job fails it must be restarted
* Internal performance of Jet is extremely high.
The Runner can't match it as of now because Beam pipeline optimization/surgery has not been fully implemented.

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the
supported capabilities of the Jet Runner.

## Running WordCount with the Hazelcast Jet Runner

### Generating the Beam examples project

Just follow the instruction from the [Java Quickstart page](/get-started/quickstart-java/#get-the-wordcount-code)

### Running WordCount on a Local Jet Cluster

Issue following command in the Beam examples project to start new Jet cluster and run the WordCount example on it.

```
    $ mvn package exec:java \
        -DskipTests \
        -Dexec.mainClass=org.apache.beam.examples.WordCount \
        -Dexec.args="\
            --runner=JetRunner \
            --jetLocalMode=3 \
            --inputFile=pom.xml \
            --output=counts" \
        -Pjet-runner
```

### Running WordCount on a Remote Jet Cluster

The Beam examples project, when generated from an archetype, comes from a particular released Beam version (that's what
the `archetypeVersion` property is about). Each Beam version that contains the Jet Runner (ie. from 2.14.0 onwards)
uses a certain version of Jet. Because of this, when we start a stand-alone Jet cluster and try to run Beam examples on
it we need to make sure the two are compatible. See following table for which Jet version is recommended for various
Beam versions.

<table class="table table-bordered">
<tr>
  <th>Beam Version</th>
  <th>Compatible Jet Versions</th>
</tr>
<tr>
  <td>2.20.0 or newer</td>
  <td>4.x</td>
</tr>
<tr>
  <td>2.14.0 - 2.19.0</td>
  <td>3.x</td>
</tr>
<tr>
  <td>2.13.0 or older</td>
  <td>N/A</td>
</tr>
</table>

Download latest Hazelcast Jet version compatible with the Beam you are using from
[Hazelcast Jet Website](https://jet-start.sh/download).

<nav class="version-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="version-jet3">Hazelcast Jet 3.x</li>
    <li data-type="version-jet4">Hazelcast Jet 4.x</li>
  </ul>
</nav>

Once the download has finished you need to start a Jet cluster. The simplest way to do so is to start Jet cluster
members using the `jet-start` script that comes with the downloaded Jet distribution. The members use the
<span class="version-jet3">
[auto discovery feature](https://docs.hazelcast.org/docs/3.12/manual/html-single/index.html#setting-up-clusters)
</span>
<span class="version-jet4">
[auto discovery feature](https://docs.hazelcast.org/docs/4.0/manual/html-single/#setting-up-clusters)
</span>
to form a cluster. Let's start up a cluster formed by two members:

{{< highlight class="version-jet3" >}}
$ cd hazelcast-jet
$ bin/jet-start.sh &
$ bin/jet-start.sh &
{{< /highlight >}}

{{< highlight class="version-jet4" >}}
$ cd hazelcast-jet
$ bin/jet-start &
$ bin/jet-start &
{{< /highlight >}}

Check the cluster is up and running:

{{< highlight class="version-jet3" >}}
$ bin/jet.sh cluster
{{< /highlight >}}

{{< highlight class="version-jet4" >}}
$ bin/jet cluster
{{< /highlight >}}

You should see something like:

{{< highlight class="version-jet3" >}}
State: ACTIVE
Version: 3.0
Size: 2

ADDRESS                  UUID
[192.168.0.117]:5701     76bea7ba-f032-4c25-ad04-bdef6782f481
[192.168.0.117]:5702     03ecfaa2-be16-41b6-b5cf-eea584d7fb86
{{< /highlight >}}

{{< highlight class="version-jet4" >}}
State: ACTIVE
Version: 4.0
Size: 2

ADDRESS                  UUID
[192.168.0.117]:5701     b9937bba-32aa-48ba-8e32-423aafed763b
[192.168.0.117]:5702     dfeadfb2-3ba5-4d1c-95e7-71a1a3ca4937
{{< /highlight >}}

Change directory to the Beam Examples project and issue following command to submit and execute your
Pipeline on the remote Jet cluster.
Make sure to distribute the input file (file with the words to be counted) to all machines where the
cluster runs. The word count job won't be able to read the data otherwise.

```
    $ mvn package exec:java \
        -DskipTests \
        -Dexec.mainClass=org.apache.beam.examples.WordCount \
        -Dexec.args="\
            --runner=JetRunner \
            --jetServers=192.168.0.117:5701,192.168.0.117:5702 \
            --codeJarPathname=target/word-count-beam-bundled-0.1.jar \
            --inputFile=<INPUT_FILE_AVAILABLE_ON_ALL_CLUSTER_MEMBERS> \
            --output=/tmp/counts" \
        -Pjet-runner
```

## Pipeline Options for the Jet Runner

<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>JetRunner</code> to run using Jet.</td>
</tr>
<tr>
  <td><code><span class="version-jet3">jetGroupName</span><span class="version-jet4">jetClusterName</span></code></td>
    <td>
        <span class="version-jet3">The name of the Hazelcast Group to join, in essence an ID of the Jet Cluster that
        will be used by the Runner. With groups it is possible to create multiple clusters where each cluster has its
        own group and doesn't interfere with other clusters.</span>
        <span class="version-jet4">The name of the Hazelcast Cluster that will be used by the Runner.</span>
    </td>
  <td><code>jet</code></td>
</tr>
<tr>
  <td><code>jetServers</code></td>
  <td>List of the addresses of Jet Cluster members, needed when the Runner doesn't start its own Jet Cluster,
  but makes use of an external, independently started one. Takes the form of a comma separated list of ip/hostname-port pairs,
  like this: <code>192.168.0.117:5701,192.168.0.117:5702</code></td>
  <td><code>127.0.0.1:5701</code></td>
</tr>
<tr>
  <td><code>codeJarPathname</code></td>
  <td>Also a property needed only when using external Jet Clusters, specifies the location of a fat jar
  containing all the code that needs to run on the cluster (so at least the pipeline and the runner code). The value
  is any string that is acceptad by <code>new java.io.File()</code> as a parameter.</td>
  <td>Has no default value.</td>
</tr>
<tr>
  <td><code>jetLocalMode</code></td>
  <td>The number of Jet Cluster members that should be started locally by the Runner. If it's <code>0</code>
  then the Runner will be using an external cluster. If greater, then the Runner will be using a cluster started by itself.</td>
  <td><code>0</code></td>
</tr>
<tr>
  <td><code>jetDefaultParallelism</code></td>
  <td>Local parallelism of Jet members, the number of processors of each vertex of the DAG that will be created on each
  Jet Cluster member.</td>
  <td><code>2</code></td>
</tr>
<tr>
  <td><code>jetProcessorsCooperative</code></td>
  <td>Boolean flag specifying if Jet Processors for DoFns are allowed to be cooperative (ie. use green threads instead of
  dedicated OS ones). If set to true than all such Processors will be cooperative, except when they have no outputs
  (so they are assumed to be syncs).</td>
  <td><code>false</code></td>
</tr>
</table>
