Flink-Dataflow
--------------

Flink-Dataflow is a Google Dataflow Runner for Apache Flink. It enables you to
run Dataflow programs with Flink as an execution engine.

# Getting Started

To get started using Google Dataflow on top of Apache Flink, we need to install the
latest version of Flink-Dataflow.

## Install Flink-Dataflow ##

To retrieve the latest version of Flink-Dataflow, run the following command

    git clone https://github.com/dataArtisans/flink-dataflow

Then switch to the newly created directory and run Maven to build the Dataflow runner:

    cd flink-dataflow
    mvn clean install -DskipTests

Flink-Dataflow is now installed in your local maven repository.

## Executing an example

Next, let's run the classic WordCount example. It's semantically identically to
the example provided with Google Dataflow. Only this time, we chose the
`FlinkPipelineRunner` to execute the WordCount on top of Flink.

Here's an excerpt from the WordCount class file:

```java
Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
// yes, we want to run WordCount with Flink
options.setRunner(FlinkPipelineRunner.class);

Pipeline p = Pipeline.create(options);

p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
		.apply(new CountWords())
		.apply(TextIO.Write.named("WriteCounts")
				.to(options.getOutput())
				.withNumShards(options.getNumShards()));

p.run();
```


To execute the example, let's first get some sample data:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > kinglear.txt

Then let's run the included WordCount locally on your machine:

    mvn exec:exec -Dinput=kinglear.txt -Doutput=wordcounts.txt

Congratulations, you have run your first Google Dataflow program on top of Apache Flink!


# Running Dataflow on Flink on a cluster

You can run your Dataflow program on an Apache Flink cluster. Please start off by creating a new
Maven project.

    mvn archetype:generate -DgroupId=com.mycompany.dataflow -DartifactId=dataflow-test \
        -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

The contents of the root `pom.xml` should be slightly changed aftewards (explanation below):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.mycompany.dataflow</groupId>
    <artifactId>dataflow-test</artifactId>
    <version>1.0</version>

    <dependencies>
        <dependency>
            <groupId>com.dataartisans</groupId>
            <artifactId>flink-dataflow</artifactId>
            <version>0.2</version>
        </dependency>
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
                                    <mainClass>WordCount</mainClass>
                                </transformer>
                            </transformers>
                            <artifactSet>
                                <excludes>
                                    <exclude>org.apache.flink:*</exclude>
                                </excludes>
                            </artifactSet>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

        </plugins>

    </build>

</project>
```

The following changes have been made:

1. The Flink Dataflow Runner was added as a dependency.

2. The Maven Shade plugin was added to build a fat jar.

A fat jar is necessary if you want to submit your Dataflow code to a Flink cluster. The fat jar
includes your program code but also Dataflow code which is necessary during runtime. Note that this
step is necessary because the Dataflow Runner is not part of Flink.

For more information, please visit the [Apache Flink Website](http://flink.apache.org) or contact
the [Mailinglists](http://flink.apache.org/community.html#mailing-lists).

# Streaming

Streaming support is currently under development. See the `streaming_new` branch for the current
work in progress version.
