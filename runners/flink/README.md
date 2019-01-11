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

Flink Beam Runner (Flink-Runner)
-------------------------------

Flink-Runner is a Runner for Apache Beam which enables you to
run Beam dataflows with Flink. It integrates seamlessly with the Beam
API, allowing you to execute Apache Beam programs in streaming or batch mode.

## Streaming

### Full Beam Windowing and Triggering Semantics

The Flink Beam Runner supports *Event Time* allowing you to analyze data with respect to its
associated timestamp. It handles out-or-order and late-arriving elements. You may leverage the full
power of the Beam windowing semantics like *time-based*, *sliding*, *tumbling*, or *count*
windows. You may build *session* windows which allow you to keep track of events associated with
each other.

### Fault-Tolerance

The program's state is persisted by Apache Flink. You may re-run and resume your program upon
failure or if you decide to continue computation at a later time.

### Sources and Sinks

Build your own data ingestion or digestion using the source/sink interface. Re-use Flink's sources
and sinks or use the provided support for Apache Kafka.

### Seamless integration

The Flink Runner decides to use batch or streaming execution mode based on whether programs use
unbounded sources. When unbounded sources are used, it executes in streaming mode, otherwise it
uses the batch execution mode.

If you wish to explicitly enable streaming mode, please set the streaming flag in the
`PipelineOptions`:

    options.setStreaming(true);

## Batch

### Batch optimization

Flink gives you out-of-core algorithms which operate on its managed memory to perform sorting,
caching, and hash table operations. We have optimized operations like CoGroup to use Flink's
optimized out-of-core implementation.

### Fault-Tolerance

We guarantee job-level fault-tolerance which gracefully restarts failed batch jobs.

### Sources and Sinks

Build your own data ingestion or digestion using the source/sink interface or re-use Flink's sources
and sinks.

## Features

The Flink Beam Runner maintains as much compatibility with the Beam API as possible. We
support transformations on data like:

- Grouping
- Windowing
- ParDo
- CoGroup
- Flatten
- Combine
- Side inputs/outputs
- Encoding

# Getting Started

To get started using the Flink Runner, we first need to install the latest version.

## Install Flink-Runner ##

To retrieve the latest version of Flink-Runner, run the following command

    git clone https://github.com/apache/beam

Then switch to the newly created directory and run Maven to build the Beam runner:

    cd beam
    mvn clean install -DskipTests

Flink-Runner is now installed in your local maven repository.

## Executing an example

Next, let's run the classic WordCount example. It's semantically identically to
the example provided with Apache Beam. Only this time, we chose the
`FlinkRunner` to execute the WordCount on top of Flink.

Here's an excerpt from the [WordCount class file](examples/src/main/java/org/apache/beam/runners/flink/examples/WordCount.java):

```java
Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

// yes, we want to run WordCount with Flink
options.setRunner(FlinkRunner.class);

Pipeline p = Pipeline.create(options);

p.apply("ReadLines", TextIO.Read.from(options.getInput()))
    .apply(new CountWords())
    .apply(MapElements.via(new FormatAsTextFn()))
    .apply("WriteCounts", TextIO.Write.to(options.getOutput()));

p.run();
```

To execute the example, let's first get some sample data:

    cd runners/flink/examples
    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > kinglear.txt

Then let's run the included WordCount locally on your machine:

    cd runners/flink/examples
    mvn exec:java -Dexec.mainClass=org.apache.beam.runners.flink.examples.WordCount \
                  -Dinput=kinglear.txt -Doutput=wordcounts.txt

Congratulations, you have run your first Apache Beam program on top of Apache Flink!

Note, that you will find a number of `wordcounts*` output files because Flink parallelizes the
WordCount computation. You may pass an additional `-Dparallelism=1` to disable parallelization and
get a single `wordcounts.txt` file.

# Running Beam programs on a Flink cluster

You can run your Beam program on an Apache Flink cluster. Please start off by creating a new
Maven project.

    mvn archetype:generate -DgroupId=com.mycompany.beam -DartifactId=beam-test \
        -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

The contents of the root `pom.xml` should be slightly changed afterwards (explanation below):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.mycompany.beam</groupId>
  <artifactId>beam-test</artifactId>
  <version>1.0</version>

  <dependencies>
    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-runners-flink_2.10</artifactId>
      <version>0.2.0-SNAPSHOT</version>
    </dependency>

    <!-- Uncomment, if you want to use Flink's Kafka connector -->
    <!--<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka-0.8_2.10</artifactId>
      <version>1.0.3</version>
    </dependency>-->

  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>2.4.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>org.apache.beam.runners.flink.examples.WordCount</mainClass>
                </transformer>
              </transformers>
              <filters>
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/LICENSE</exclude>
                  </excludes>
                </filter>
              </filters>
            </configuration>
          </execution>
        </executions>
      </plugin>

      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
          <source>1.7</source>
          <target>1.7</target>
        </configuration>
      </plugin>

    </plugins>

  </build>

</project>
```

The following changes have been made:

1. The Flink Beam Runner was added as a dependency.

2. The Maven Shade plugin was added to build a fat jar.

A fat jar is necessary if you want to submit your Beam code to a Flink cluster. The fat jar
includes your program code but also Beam code which is necessary during runtime. Note that this
step is necessary because the Beam Runner is not part of Flink.

You can then build the jar using `mvn clean package`. Please submit the fat jar in the `target`
folder to the Flink cluster using the command-line utility like so:

    ./bin/flink run /path/to/fat.jar


# More

For more information, please visit the [Apache Flink Website](http://flink.apache.org) or contact
the [Mailinglists](http://flink.apache.org/community.html#mailing-lists).
