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

Apex Beam Runner ﴾Apex‐Runner﴿
=============================

Apex‐Runner is a Runner for Apache Beam which executes Beam pipelines with Apache Apex as underlying engine. The runner has broad support for the Beam model and supports streaming and batch pipelines. 

[Apache Apex](http://apex.apache.org/) is a stream processing platform and framework for low-latency, high-throughput and fault-tolerant analytics applications on Apache Hadoop. Apex is Java based and also provides its own API for application development (native compositional and declarative Java API, SQL) with a comprehensive [operator library](https://github.com/apache/apex-malhar). Apex has a unified streaming architecture and can be used for real-time and batch processing. With its stateful stream processing architecture Apex can support all of the concepts in the Beam model (event time, triggers, watermarks etc.).

##Status

Apex-Runner is relatively new. It is fully functional and can currently be used to run pipelines in embedded mode. It does not take advantage of all the performance and scalability that Apex can deliver. This is expected to be addressed with upcoming work, leveraging features like incremental checkpointing, partitioning and operator affinity from Apex. Please see [JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-apex%20AND%20resolution%20%3D%20Unresolved) and we welcome contributions!

##Getting Started

The following shows how to run the WordCount example that is provided with the source code on Apex (the example is identical with the one provided as part of the Beam examples). 

###Installing Beam

To get the latest version of Beam with Apex-Runner, first clone the Beam repository:

```
git clone https://github.com/apache/beam
```

Then switch to the newly created directory and run Maven to build the Apache Beam:

```
cd beam
mvn clean install -DskipTests
```

Now Apache Beam and the Apex Runner are installed in your local Maven repository.

###Running an Example

Download something to count:

```
curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > /tmp/kinglear.txt
```

Run the pipeline, using the Apex runner:

```
cd examples/java
mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--inputFile=/tmp/kinglear.txt --output=/tmp/wordcounts.txt --runner=ApexRunner" -Papex-runner
```

Once completed, there will be multiple output files with the base name given above:

```
$ ls /tmp/out-*
/tmp/out-00000-of-00003  /tmp/out-00001-of-00003  /tmp/out-00002-of-00003
```

##Running pipelines on an Apex YARN cluster

Coming soon.
