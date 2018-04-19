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

## Gearpump Beam Runner

The Gearpump Beam runner allows users to execute pipelines written using the Apache Beam programming API with Apache Gearpump (incubating) as an execution engine.

##Getting Started

The following shows how to run the WordCount example that is provided with the source code on Beam.

###Installing Beam

To get the latest version of Beam with Gearpump-Runner, first clone the Beam repository:

```
git clone https://github.com/apache/beam
git checkout gearpump-runner
```

Then run Gradle to build Apache Beam:

```
./gradlew :beam-runners-gearpump:build
```

###Running Wordcount Example

Download something to count:

```
curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > /tmp/kinglear.txt
```

> Note: There is an open issue to update this README for Gradle:
[BEAM-4129](https://issues.apache.org/jira/browse/BEAM-4129).

Run the pipeline, using the Gearpump runner:

```
cd examples/java
mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--inputFile=/tmp/kinglear.txt --output=/tmp/wordcounts.txt --runner=TestGearpumpRunner" -Pgearpump-runner
```

Once completed, check the output file /tmp/wordcounts.txt-00000-of-00001
