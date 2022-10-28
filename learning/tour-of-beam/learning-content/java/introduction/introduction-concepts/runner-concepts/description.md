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
### Overview

Apache Beam provides a portable API layer for building sophisticated data-parallel processing `pipelines` that may be executed across a diversity of execution engines, or `runners`. The core concepts of this layer are based upon the Beam Model (formerly referred to as the Dataflow Model), and implemented to varying degrees in each Beam `runner`.

### Direct runner
The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

* enforcing immutability of elements
* enforcing encodability of elements
* elements are processed in an arbitrary order at all points
* serialization of user functions (DoFn, CombineFn, etc.)

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.

#### Specify your dependency

When using Java, you must specify your dependency on the Direct Runner in your pom.xml.

```
<dependency>
   <groupId>org.apache.beam</groupId>
   <artifactId>beam-runners-direct-java</artifactId>
   <version>2.41.0</version>
   <scope>runtime</scope>
</dependency>
```

#### Set runner

In java, you need to set runner to `args` when you start the program.

```
--runner=DirectRunner
```

### Google Cloud Dataflow runner

The Google Cloud Dataflow uses the Cloud Dataflow managed service. When you run your pipeline with the Cloud Dataflow service, the runner uploads your executable code and dependencies to a Google Cloud Storage bucket and creates a Cloud Dataflow job, which executes your pipeline on managed resources in Google Cloud Platform. The Cloud Dataflow Runner and service are suitable for large scale, continuous jobs, and provide:
* a fully managed service
* autoscaling of the number of workers throughout the lifetime of the job
* dynamic work rebalancing

Additionally, you can read [here](https://beam.apache.org/documentation/runners/dataflow/)

#### Run example

When using Java, you must specify your dependency on the Cloud Dataflow Runner in your `pom.xml`.

```
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-google-cloud-dataflow-java</artifactId>
  <version>2.42.0</version>
  <scope>runtime</scope>
</dependency>
```

Then, add the mainClass name in the Maven JAR plugin.

```
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-jar-plugin</artifactId>
  <version>${maven-jar-plugin.version}</version>
  <configuration>
    <archive>
      <manifest>
        <addClasspath>true</addClasspath>
        <classpathPrefix>lib/</classpathPrefix>
        <mainClass>YOUR_MAIN_CLASS_NAME</mainClass>
      </manifest>
    </archive>
  </configuration>
</plugin>
```

Console:
```
java -jar target/beam-examples-bundled-1.0.0.jar \
  --runner=DataflowRunner \
  --project=<YOUR_GCP_PROJECT_ID> \
  --region=<GCP_REGION> \
  --tempLocation=gs://<YOUR_GCS_BUCKET>/temp/
```

### Apache Flink runner

The Apache Flink Runner can be used to execute Beam pipelines using Apache Flink. For execution, you can choose between a cluster execution mode (e.g. Yarn/Kubernetes/Mesos) or a local embedded execution mode which is useful for testing pipelines. The Flink Runner and Flink are suitable for large scale, continuous jobs, and provide:
* A streaming-first runtime that supports both batch processing and data streaming programs
* A runtime that supports very high throughput and low event latency at the same time
* Fault-tolerance with exactly-once processing guarantees
* Natural back-pressure in streaming programs
* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms
* Integration with YARN and other components of the Apache Hadoop ecosystem

Additionally, you can read [here](https://beam.apache.org/documentation/runners/flink/)

#### Run example

##### Portable
1. Starting with Beam 2.18.0, pre-built Flink Job Service Docker images are available at Docker Hub: `Flink 1.10`, `Flink 1.11`, `Flink 1.12`, `Flink 1.13`, `Flink 1.14`.
2. Start the JobService endpoint: `docker run --net=host apache/beam_flink1.10_job_server:latest`
3. Submit the pipeline to the above endpoint by using the PortableRunner, job_endpoint set to localhost:8099 (this is the default address of the JobService). Optionally set environment_type set to LOOPBACK. For example:

```
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as p:
    ...
```

##### Non portable

When using Java, you must specify your dependency on the Cloud Dataflow Runner in your `pom.xml`.

```
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-flink-1.14</artifactId>
  <version>2.42.0</version>
</dependency>
```

Console:
```
mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --inputFile=/path/to/pom.xml \
      --output=/path/to/counts \
      --flinkMaster=<flink master url> \
      --filesToStage=target/word-count-beam-bundled-0.1.jar"
```

### Apache Spark runner

The Apache Spark Runner can be used to execute Beam pipelines using Apache Spark. The Spark Runner can execute Spark pipelines just like a native Spark application; deploying a self-contained application for local mode, running on Spark’s Standalone RM, or using YARN or Mesos.

The Spark Runner executes Beam pipelines on top of Apache Spark, providing:

* Batch and streaming (and combined) pipelines.
* The same fault-tolerance guarantees as provided by RDDs and DStreams.
* The same security features Spark provides.
* Built-in metrics reporting using Spark’s metrics system, which reports Beam Aggregators as well.
* Native support for Beam side-inputs via spark’s Broadcast variables.

Additionally, you can read [here](https://beam.apache.org/documentation/runners/spark/)

#### Run example

##### Non portable
1. Start the JobService endpoint:
    * with Docker (preferred): docker run --net=host apache/beam_spark_job_server:latest
    * or from Beam source code: ./gradlew :runners:spark:3:job-server:runShadow
2. Submit the Python pipeline to the above endpoint by using the PortableRunner, job_endpoint set to localhost:8099 (this is the default address of the JobService), and environment_type set to LOOPBACK. For example:

Console:
```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=SparkRunner --inputFile=pom.xml --output=counts" -Pspark-runner
```

##### Non Portable

When using Java, you must specify your dependency on the Cloud Dataflow Runner in your `pom.xml`.

```
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-spark-3</artifactId>
  <version>2.42.0</version>
</dependency>
```

And shading the application jar using the maven shade plugin:

```
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <configuration>
    <createDependencyReducedPom>false</createDependencyReducedPom>
    <filters>
      <filter>
        <artifact>*:*</artifact>
        <excludes>
          <exclude>META-INF/*.SF</exclude>
          <exclude>META-INF/*.DSA</exclude>
          <exclude>META-INF/*.RSA</exclude>
        </excludes>
      </filter>
    </filters>
  </configuration>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <shadedArtifactAttached>true</shadedArtifactAttached>
        <shadedClassifierName>shaded</shadedClassifierName>
        <transformers>
          <transformer
            implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
        </transformers>
      </configuration>
    </execution>
  </executions>
</plugin>
```


Console:
```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=SparkRunner --inputFile=pom.xml --output=counts" -Pspark-runner
```

### Samza runner

The Apache Samza Runner can be used to execute Beam pipelines using Apache Samza. The Samza Runner executes Beam pipeline in a Samza application and can run locally. The application can further be built into a .tgz file, and deployed to a YARN cluster or Samza standalone cluster with Zookeeper.

The Samza Runner and Samza are suitable for large scale, stateful streaming jobs, and provide:

* First class support for local state (with RocksDB store). This allows fast state access for high frequency streaming jobs.
* Fault-tolerance with support for incremental checkpointing of state instead of full snapshots. This enables Samza to scale to applications with very large state.
* A fully asynchronous processing engine that makes remote calls efficient.
* Flexible deployment model for running the applications in any hosting environment with Zookeeper.
* Features like canaries, upgrades and rollbacks that support extremely large deployments with minimal downtime.

Additionally, you can read [here](https://beam.apache.org/documentation/runners/samza/)

#### Run example

You can specify your dependency on the Samza Runner by adding the following to your `pom.xml`:

```
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-samza</artifactId>
  <version>2.42.0</version>
  <scope>runtime</scope>
</dependency>

<!-- Samza dependencies -->
<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-api</artifactId>
  <version>${samza.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-core_2.11</artifactId>
  <version>${samza.version}</version>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-kafka_2.11</artifactId>
  <version>${samza.version}</version>
  <scope>runtime</scope>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-kv_2.11</artifactId>
  <version>${samza.version}</version>
  <scope>runtime</scope>
</dependency>

<dependency>
  <groupId>org.apache.samza</groupId>
  <artifactId>samza-kv-rocksdb_2.11</artifactId>
  <version>${samza.version}</version>
  <scope>runtime</scope>
</dependency>
```

Console:
```
$ mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Psamza-runner \
    -Dexec.args="--runner=SamzaRunner \
      --inputFile=/path/to/input \
      --output=/path/to/counts"
```

### Nemo runner

The Apache Nemo Runner can be used to execute Beam pipelines using Apache Nemo. The Nemo Runner can optimize Beam pipelines with the Nemo compiler through various optimization passes and execute them in a distributed fashion using the Nemo runtime. You can also deploy a self-contained application for local mode or run using resource managers like YARN or Mesos.

The Nemo Runner executes Beam pipelines on top of Apache Nemo, providing:

* Batch and streaming pipelines
* Fault-tolerance
* Integration with YARN and other components of the Apache Hadoop ecosystem
* Support for the various optimizations provided by the Nemo optimizer

Additionally, you can read [here](https://beam.apache.org/documentation/runners/nemo/)

#### Run example

You can specify your dependency on the Samza Runner by adding the following to your `pom.xml`:

```
<dependency>
    <groupId>org.apache.nemo</groupId>
    <artifactId>nemo-compiler-frontend-beam</artifactId>
    <version>${nemo.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>${hadoop.version}</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </exclusion>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

A self-contained application might be easier to manage and allows you to fully use the functionality that Nemo provides. Simply add the dependency shown above and shade the application JAR using the Maven Shade plugin:

```
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <configuration>
    <createDependencyReducedPom>false</createDependencyReducedPom>
    <filters>
      <filter>
        <artifact>*:*</artifact>
        <excludes>
          <exclude>META-INF/*.SF</exclude>
          <exclude>META-INF/*.DSA</exclude>
          <exclude>META-INF/*.RSA</exclude>
        </excludes>
      </filter>
    </filters>
  </configuration>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <shadedArtifactAttached>true</shadedArtifactAttached>
        <shadedClassifierName>shaded</shadedClassifierName>
        <transformers>
          <transformer
            implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
        </transformers>
      </configuration>
    </execution>
  </executions>
</plugin>
```

Console:
```
$ mvn package -Pnemo-runner && java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount \
     --runner=NemoRunner --inputFile=`pwd`/pom.xml --output=counts
```

### Jet runner

The Hazelcast Jet Runner can be used to execute Beam pipelines using Hazelcast Jet.

The Jet Runner and Jet are suitable for large scale continuous jobs and provide:

* Support for both batch (bounded) and streaming (unbounded) data sets
* A runtime that supports very high throughput and low event latency at the same time
* Natural back-pressure in streaming programs
* Distributed massively parallel data processing engine with in memory storage

It’s important to note that the Jet Runner is currently in an EXPERIMENTAL state and can not make use of many of the capabilities present in Jet:

* Jet has full Fault Tolerance support, the Jet Runner does not; if a job fails it must be restarted
* Internal performance of Jet is extremely high. The Runner can’t match it as of now because Beam pipeline optimization/surgery has not been fully implemented.

Additionally, you can read [here](https://beam.apache.org/documentation/runners/jet/)

#### Run example

```
$ mvn package -P jet-runner && java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount \
     --runner=JetRunner --jetLocalMode=3 --inputFile=`pwd`/pom.xml --output=counts
```