---
layout: section
title: "Apache Apex Runner"
section_menu: section-menu/runners.html
permalink: /documentation/runners/apex/
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
# Using the Apache Apex Runner

The Apex Runner executes Apache Beam pipelines using [Apache Apex](http://apex.apache.org/) as an underlying engine. The runner has broad support for the [Beam model and supports streaming and batch pipelines]({{ site.baseurl }}/documentation/runners/capability-matrix/).

[Apache Apex](http://apex.apache.org/) is a stream processing platform and framework for low-latency, high-throughput and fault-tolerant analytics applications on Apache Hadoop. Apex has a unified streaming architecture and can be used for real-time and batch processing.

The following instructions are for running Beam pipelines with Apex on a YARN cluster.
They are not required for Apex in embedded mode (see [quickstart]({{ site.baseurl }}/get-started/quickstart-java/)).

## Apex Runner prerequisites

You may set up your own Hadoop cluster. Beam does not require anything extra to launch the pipelines on YARN.
An optional Apex installation may be useful for monitoring and troubleshooting.
The Apex CLI can be [built](http://apex.apache.org/docs/apex/apex_development_setup/) or
obtained as binary build.
For more download options see [distribution information on the Apache Apex website](http://apex.apache.org/downloads.html).

## Running wordcount with Apex

Typically the build environment is separate from the target YARN cluster. In such case, it is necessary to build a fat jar that will include all dependencies. Ensure that `hadoop.version` in `pom.xml` matches the version of your YARN cluster and then build the jar file:
```
mvn package -Papex-runner
```

Copy the resulting `target/word-count-beam-bundled-0.1.jar` to the cluster and submit the application using:
```
java -cp word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount --inputFile=/etc/profile --output=/tmp/counts --embeddedExecution=false --configFile=beam-runners-apex.properties --runner=ApexRunner
```

If the build environment is setup as cluster client, it is possible to run the example directly:
```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--inputFile=/etc/profile --output=/tmp/counts --runner=ApexRunner --embeddedExecution=false --configFile=beam-runners-apex.properties" -Papex-runner
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

This example uses local files. To use a distributed file system (HDFS, S3 etc.),
it is necessary to augment the build to include the respective file system provider.

## Montoring progress of your job

Depending on your installation, you may be able to monitor the progress of your job on the Hadoop cluster. Alternatively, you have following options:

* YARN : Using YARN web UI generally running on 8088 on the node running resource manager.
* Apex command-line interface: [Using the Apex CLI to get running application information](http://apex.apache.org/docs/apex/apex_cli/#apex-cli-commands).

Check the output of the pipeline:
```
ls /tmp/counts*
```
