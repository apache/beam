---
title: "WordCount quickstart for Java"
aliases:
  - /get-started/quickstart/
  - /use/quickstart/
  - /getting-started/
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

# WordCount quickstart for Java

This quickstart shows you how to set up a Java development environment and run
an [example pipeline](/get-started/wordcount-example) written with the
[Apache Beam Java SDK](/documentation/sdks/java), using a
[runner](/documentation#runners) of your choice.

If you're interested in contributing to the Apache Beam Java codebase, see the
[Contribution Guide](/contribute).

On this page:

{{< toc >}}

## Set up your development environment

1. Download and install the
  [Java Development Kit (JDK)](https://www.oracle.com/technetwork/java/javase/downloads/index.html)
  version 8, 11, or 17. Verify that the
  [JAVA_HOME](https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/envvars001.html)
  environment variable is set and points to your JDK installation.
2. Download and install [Apache Maven](https://maven.apache.org/download.cgi) by
   following the [installation guide](https://maven.apache.org/install.html)
   for your operating system.
3. Optional: If you want to convert your Maven project to Gradle, install
   [Gradle](https://gradle.org/install/).

## Get the example code

1. Generate a Maven example project that builds against the latest Beam release:
   {{< shell unix >}}
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion={{< param release_latest >}} \
    -DgroupId=org.example \
    -DartifactId=word-count-beam \
    -Dversion="0.1" \
    -Dpackage=org.apache.beam.examples \
    -DinteractiveMode=false
   {{< /shell >}}
   {{< shell powerShell >}}
mvn archetype:generate `
  -D archetypeGroupId=org.apache.beam `
  -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
  -D archetypeVersion={{< param release_latest >}} `
  -D groupId=org.example `
  -D artifactId=word-count-beam `
  -D version="0.1" `
  -D package=org.apache.beam.examples `
  -D interactiveMode=false
   {{< /shell >}}

   Maven creates a new project in the **word-count-beam** directory.

2. Change into **word-count-beam**:
   {{< shell unix >}}
cd word-count-beam/
   {{< /shell >}}
   {{< shell powerShell >}}
cd .\word-count-beam
   {{< /shell >}}
   The directory contains a **pom.xml** and a **src** directory with example
   pipelines.

3. List the example pipelines:
   {{< shell unix >}}
ls src/main/java/org/apache/beam/examples/
   {{< /shell >}}
   {{< shell powerShell >}}
dir .\src\main\java\org\apache\beam\examples
   {{< /shell >}}
   You should see the following examples:
   * **DebuggingWordCount.java** ([GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/DebuggingWordCount.java))
   * **MinimalWordCount.java** ([GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/MinimalWordCount.java))
   * **WindowedWordCount.java** ([GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WindowedWordCount.java))
   * **WordCount.java** ([GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java))

   The example used in this tutorial, **WordCount.java**, defines a
   Beam pipeline that counts words from an input file (by default, a **.txt**
   file containing Shakespeare's "King Lear"). To learn more about the examples,
   see the [WordCount Example Walkthrough](/get-started/wordcount-example).

## Optional: Convert from Maven to Gradle

The steps below explain how to convert the build from Maven to Gradle for the
following runners:
* Direct runner
* Dataflow runner

The conversion process for other runners is similar. For additional guidance,
see
[Migrating Builds From Apache Maven](https://docs.gradle.org/current/userguide/migrating_from_maven.html).

1. In the directory with the **pom.xml** file, run the automated Maven-to-Gradle
   conversion:
   {{< highlight >}}
gradle init
   {{< /highlight >}}
   You'll be asked if you want to generate a Gradle build. Enter **yes**. You'll
   also be prompted to choose a DSL (Groovy or Kotlin). For this tutorial, enter
   **2** for Kotlin.
2. Open the generated **build.gradle.kts** file and make the following changes:
   1. In `repositories`, replace `mavenLocal()` with `mavenCentral()`.
   2. In `repositories`, declare a repository for Confluent Kafka dependencies:
      {{< highlight >}}
maven {
    url = uri("https://packages.confluent.io/maven/")
}
      {{< /highlight >}}
   3. At the end of the build script, add the following conditional dependency:
      {{< highlight >}}
if (project.hasProperty("dataflow-runner")) {
    dependencies {
        runtimeOnly("org.apache.beam:beam-runners-google-cloud-dataflow-java:{{< param release_latest >}}")
    }
}
      {{< /highlight >}}
   4. At the end of the build script, add the following task:
      {{< highlight >}}
task execute (type: JavaExec) {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(System.getProperty("mainClass"))
}
      {{< /highlight >}}
4. Build your project:
   {{< highlight >}}
gradle build
   {{< /highlight >}}

## Get sample text

> If you're planning to use the DataflowRunner, you can skip this step. The
  runner will pull text directly from Google Cloud Storage.

1. In the **word-count-beam** directory, create a file called **sample.txt**.
2. Add some text to the file. For this example, use the text of Shakespeare's
   [King Lear](https://storage.cloud.google.com/apache-beam-samples/shakespeare/kinglear.txt).

## Run a pipeline

A single Beam pipeline can run on multiple Beam
[runners](/documentation#runners). The
[DirectRunner](/documentation/runners/direct) is useful for getting started,
because it runs on your machine and requires no specific setup. If you're just
trying out Beam and you're not sure what to use, use the
[DirectRunner](/documentation/runners/direct).

The general process for running a pipeline goes like this:

1.  Complete any runner-specific setup.
2.  Build your command line:
    1. Specify a runner with `--runner=<runner>` (defaults to the
       [DirectRunner](/documentation/runners/direct)).
    2. Add any runner-specific required options.
    3. Choose input files and an output location that are accessible to the
       runner. (For example, you can't access a local file if you are running
       the pipeline on an external cluster.)
3.  Run the command.

To run the WordCount pipeline:

1. Follow the setup steps for your runner:
   * [FlinkRunner](/documentation/runners/flink)
   * [SparkRunner](/documentation/runners/spark)
   * [DataflowRunner](/documentation/runners/dataflow)
   * [SamzaRunner](/documentation/runners/samza)
   * [NemoRunner](/documentation/runners/nemo)
   * [JetRunner](/documentation/runners/jet)

   The DirectRunner will work without additional setup.
2. Run the corresponding Maven or Gradle command below.

### Run WordCount using Maven

For Unix shells:

{{< runner direct >}}
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--inputFile=sample.txt --output=counts" -Pdirect-runner
{{< /runner >}}
{{< runner flink >}}
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--runner=FlinkRunner --inputFile=sample.txt --output=counts" -Pflink-runner
{{< /runner >}}
{{< runner flinkCluster >}}
mvn package exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=target/word-count-beam-bundled-0.1.jar \
                 --inputFile=sample.txt --output=/tmp/counts" -Pflink-runner
{{< /runner >}}
{{< runner spark >}}
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--runner=SparkRunner --inputFile=sample.txt --output=counts" -Pspark-runner
{{< /runner >}}
{{< runner dataflow >}}
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--runner=DataflowRunner --project=<your-gcp-project> \
                 --region=<your-gcp-region> \
                 --gcpTempLocation=gs://<your-gcs-bucket>/tmp \
                 --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://<your-gcs-bucket>/counts" \
    -Pdataflow-runner
{{< /runner >}}
{{< runner samza >}}
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--inputFile=sample.txt --output=/tmp/counts --runner=SamzaRunner" -Psamza-runner
{{< /runner >}}
{{< runner nemo >}}
mvn package -Pnemo-runner && java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount \
    --runner=NemoRunner --inputFile=`pwd`/sample.txt --output=counts
{{< /runner >}}
{{< runner jet >}}
mvn package -Pjet-runner
java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount \
    --runner=JetRunner --jetLocalMode=3 --inputFile=`pwd`/sample.txt --output=counts
{{< /runner >}}

For Windows PowerShell:

{{< runner direct >}}
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner
{{< /runner >}}
{{< runner flink >}}
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=FlinkRunner --inputFile=sample.txt --output=counts" -P flink-runner
{{< /runner >}}
{{< runner flinkCluster >}}
mvn package exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=.\target\word-count-beam-bundled-0.1.jar `
               --inputFile=C:\path\to\quickstart\sample.txt --output=C:\tmp\counts" -P flink-runner
{{< /runner >}}
{{< runner spark >}}
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=SparkRunner --inputFile=sample.txt --output=counts" -P spark-runner
{{< /runner >}}
{{< runner dataflow >}}
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=DataflowRunner --project=<your-gcp-project> `
               --region=<your-gcp-region> \
               --gcpTempLocation=gs://<your-gcs-bucket>/tmp `
               --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://<your-gcs-bucket>/counts" `
 -P dataflow-runner
{{< /runner >}}
{{< runner samza >}}
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
    -D exec.args="--inputFile=sample.txt --output=/tmp/counts --runner=SamzaRunner" -P samza-runner
{{< /runner >}}
{{< runner nemo >}}
mvn package -P nemo-runner -DskipTests
java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount `
    --runner=NemoRunner --inputFile=`pwd`/sample.txt --output=counts
{{< /runner >}}
{{< runner jet >}}
mvn package -P jet-runner
java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount `
    --runner=JetRunner --jetLocalMode=3 --inputFile=$pwd/sample.txt --output=counts
{{< /runner >}}

### Run WordCount using Gradle

For Unix shells:

{{< runner direct>}}
gradle clean execute -DmainClass=org.apache.beam.examples.WordCount \
    --args="--inputFile=sample.txt --output=counts"
{{< /runner >}}
{{< runner flink>}}
TODO: document Flink on Gradle: https://github.com/apache/beam/issues/21498
{{< /runner >}}
{{< runner flinkCluster>}}
TODO: document FlinkCluster on Gradle: https://github.com/apache/beam/issues/21499
{{< /runner >}}
{{< runner spark >}}
TODO: document Spark on Gradle: https://github.com/apache/beam/issues/21502
{{< /runner >}}
{{< runner dataflow >}}
gradle clean execute -DmainClass=org.apache.beam.examples.WordCount \
    --args="--project=<your-gcp-project> --inputFile=gs://apache-beam-samples/shakespeare/* \
    --output=gs://<your-gcs-bucket>/counts --runner=DataflowRunner" -Pdataflow-runner
{{< /runner >}}
{{< runner samza>}}
TODO: document Samza on Gradle: https://github.com/apache/beam/issues/21500
{{< /runner >}}
{{< runner nemo>}}
TODO: document Nemo on Gradle: https://github.com/apache/beam/issues/21503
{{< /runner >}}
{{< runner jet>}}
TODO: document Jet on Gradle: https://github.com/apache/beam/issues/21501
{{< /runner >}}

## Inspect the results

After the pipeline has completed, you can view the output. There might be
multiple output files prefixed by `count`. The number of output files is decided
by the runner, giving it the flexibility to do efficient, distributed execution.

1. View the output files in a Unix shell:
   {{< runner direct >}}
ls counts*
   {{< /runner >}}
   {{< runner flink >}}
ls counts*
   {{< /runner >}}
   {{< runner flinkCluster >}}
ls /tmp/counts*
   {{< /runner >}}
   {{< runner spark >}}
ls counts*
   {{< /runner >}}
   {{< runner dataflow >}}
gsutil ls gs://<your-gcs-bucket>/counts*
   {{< /runner >}}
   {{< runner samza >}}
ls /tmp/counts*
   {{< /runner >}}
   {{< runner nemo >}}
ls counts*
   {{< /runner >}}
   {{< runner jet >}}
ls counts*
   {{< /runner >}}
   The output files contain unique words and the number of occurrences of each
   word.
2. View the output content in a Unix shell:
   {{< runner direct >}}
more counts*
   {{< /runner >}}
   {{< runner flink >}}
more counts*
   {{< /runner >}}
   {{< runner flinkCluster >}}
more /tmp/counts*
   {{< /runner >}}
   {{< runner spark >}}
more counts*
   {{< /runner >}}
   {{< runner dataflow >}}
gsutil cat gs://<your-gcs-bucket>/counts*
   {{< /runner >}}
   {{< runner samza >}}
more /tmp/counts*
   {{< /runner >}}
   {{< runner nemo >}}
more counts*
   {{< /runner >}}
   {{< runner jet >}}
more counts*
   {{< /runner >}}
   The order of elements is not guaranteed, to allow runners to optimize for
   efficiency. But the output should look something like this:
   ```
   ...
   Think: 3
   slower: 1
   Having: 1
   revives: 1
   these: 33
   wipe: 1
   arrives: 1
   concluded: 1
   begins: 3
   ...
   ```

## Next Steps

* Learn more about the [Beam SDK for Java](/documentation/sdks/java/)
  and look through the
  [Java SDK API reference](https://beam.apache.org/releases/javadoc).
* Walk through the WordCount examples in the
  [WordCount Example Walkthrough](/get-started/wordcount-example).
* Take a self-paced tour through our
  [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite
  [Videos and Podcasts](/get-started/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any
issues!
