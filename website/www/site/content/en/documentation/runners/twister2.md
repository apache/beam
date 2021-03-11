---
type: runners
title: "Twister2 Runner"
aliases: /learn/runners/twister2/
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

Twister2 Runner can be used to execute Apache Beam pipelines on top of a Twister2
cluster. Twister2 Runner runs Beam pipelines as Twister2 jobs, which can be executed on
a Twister2 cluster either as a local deployment or distributed deployment using, Nomad,
Kubernetes, Slurm, etc.

The Twister2 runner is suitable for large scale batch jobs, specially jobs that
require high performance, and provide.
* Batch pipeline support.
* Support for HPC environments, supports propriety interconnects such as Infiniband.
* Distributed massively parallel data processing engine with high performance using
 Bulk Synchronous Parallel (BSP) style execution.
* Native support for Beam side-inputs.

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the
supported capabilities of the Twister2 Runner.

## Running WordCount with the Twister2 Runner

### Generating the Beam examples project

Just follow the instruction from the [Java Quickstart page](/get-started/quickstart-java/#get-the-wordcount-code)

### Running WordCount on a Twister2 Local Deployment

Issue following command in the Beam examples project to start new Twister2 Local cluster and run the WordCount example on it.

```
    $ mvn package exec:java \
        -DskipTests \
        -Dexec.mainClass=org.apache.beam.examples.WordCount \
        -Dexec.args="\
            --runner=Twister2Runner \
            --inputFile=pom.xml \
            --output=counts" \
        -Ptwister2-runner
```

### Running WordCount on a Twister2 Deployment

The Beam examples project, when generated from an archetype, comes from a particular released Beam version (that's what
the `archetypeVersion` property is about). Each Beam version that contains the Twister2 Runner (i.e. from 2.23.0 onwards)
uses a certain version of Twister2. Because of this, when we start a stand-alone Twister2 cluster and try to run Beam examples on
it we need to make sure the two are compatible. See following table for which Twister2 version is recommended for various
Beam versions.

<table class="table table-bordered">
<tr>
  <th>Beam Version</th>
  <th>Compatible Twister2 Versions</th>
</tr>
<tr>
  <td>2.23.0 or newer</td>
  <td>0.6.0</td>
</tr>
<tr>
  <td>2.22.0 or older</td>
  <td>N/A</td>
</tr>
</table>

Download latest Twister2 version compatible with the Beam you are using from
[Twister2 Website](https://twister2.org/docs/download). Twister2 currently supports
several deployment options, such as standalone, Slurm, Mesos, Nomad, etc. To learn more about the Twister2
deployments and how to get them setup visit [Twister2 Docs](https://twister2.org/docs/deployment/job-submit).

<nav class="version-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="version-twister2-0.6.0">Twister2 0.6.0</li>
  </ul>
</nav>

Issue following command in the Beam examples project to start new Twister2 job,
The "twister2Home" should point to the home directory of the Twister2 standalone
deployment.

Note: Currently file paths need to be absolute paths.

```
    $ mvn package exec:java \
        -DskipTests \
        -Dexec.mainClass=org.apache.beam.examples.WordCount \
        -Dexec.args="\
            --runner=Twister2Runner \
            --twister2Home=<PATH_TO_TWISTER2_HOME>
            --parallelism=2
            --inputFile=<PATH_TO_FILE>/pom.xml \
            --output=<PATH_TO_FILE>/counts" \
        -Ptwister2-runner
```

## Pipeline Options for the Twister2 Runner

<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>Twister2Runner</code> to run using Twister2.</td>
</tr>
<tr>
  <td><code>twister2Home</code></td>
  <td>Location of the Twister2 home directory of the deployment being used.</td>
  <td>Has no default value. Twister2 Runner will use the Local Deployment mode for execution if not set.</td>
</tr>
<tr>
  <td><code>parallelism</code></td>
  <td>Set the parallelism of the job</td>
  <td>1</td>
</tr>
<tr>
  <td><code>clusterType</code></td>
  <td>Set the type of Twister deployment being used. Valid values are <code>standalone, slurm, nomad, mesos</code>.</td>
  <td>standalone</td>
</tr>
<tr>
  <td><code>workerCPUs</code></td>
  <td>Number of CPU's assigned to a single worker. The total number of CPU's utilized would be <code>parallelism*workerCPUs</code>.</td>
  <td>2</td>
</tr>
<tr>
  <td><code>ramMegaBytes</code></td>
  <td>Memory allocated to a single worker in MegaBytes. The total allocated memory would be <code>parallelism*ramMegaBytes</code>.</td>
  <td>2048</td>
</tr>
</table>
