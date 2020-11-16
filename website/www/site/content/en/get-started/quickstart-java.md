---
title: "Beam Quickstart for Java"
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

# Apache Beam Java SDK Quickstart

This Quickstart will walk you through executing your first Beam pipeline to run [WordCount](/get-started/wordcount-example), written using Beam's [Java SDK](/documentation/sdks/java), on a [runner](/documentation#runners) of your choice.

If you're interested in contributing to the Apache Beam Java codebase, see the [Contribution Guide](/contribute).

{{< toc >}}

## Set up your Development Environment

1. Download and install the [Java Development Kit (JDK)](https://www.oracle.com/technetwork/java/javase/downloads/index.html) version 8. Verify that the [JAVA_HOME](https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/envvars001.html) environment variable is set and points to your JDK installation.

1. Download and install [Apache Maven](https://maven.apache.org/download.cgi) by following Maven's [installation guide](https://maven.apache.org/install.html) for your specific operating system.

1. Optional: Install [Gradle](https://gradle.org/install/) if you would like to convert your Maven project into Gradle.

## Get the WordCount Code

The easiest way to get a copy of the WordCount pipeline is to use the following command to generate a simple Maven project that contains Beam's WordCount examples and builds against the most recent Beam release:

{{< highlight class="shell-unix" >}}
$ mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion={{< param release_latest >}} \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false
{{< /highlight >}}

{{< highlight class="shell-PowerShell" >}}
PS> mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion={{< param release_latest >}} `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false
{{< /highlight >}}

This will create a directory `word-count-beam` that contains a simple `pom.xml` and a series of example pipelines that count words in text files.

{{< highlight class="shell-unix" >}}
$ cd word-count-beam/

$ ls
pom.xml	src

$ ls src/main/java/org/apache/beam/examples/
DebuggingWordCount.java	WindowedWordCount.java	common
MinimalWordCount.java	WordCount.java
{{< /highlight >}}

{{< highlight class="shell-PowerShell" >}}
PS> cd .\word-count-beam

PS> dir

...

Mode                LastWriteTime         Length Name
----                -------------         ------ ----
d-----        7/19/2018  11:00 PM                src
-a----        7/19/2018  11:00 PM          16051 pom.xml

PS> dir .\src\main\java\org\apache\beam\examples

...
Mode                LastWriteTime         Length Name
----                -------------         ------ ----
d-----        7/19/2018  11:00 PM                common
d-----        7/19/2018  11:00 PM                complete
d-----        7/19/2018  11:00 PM                subprocess
-a----        7/19/2018  11:00 PM           7073 DebuggingWordCount.java
-a----        7/19/2018  11:00 PM           5945 MinimalWordCount.java
-a----        7/19/2018  11:00 PM           9490 WindowedWordCount.java
-a----        7/19/2018  11:00 PM           7662 WordCount.java
{{< /highlight >}}

For a detailed introduction to the Beam concepts used in these examples, see the [WordCount Example Walkthrough](/get-started/wordcount-example). Here, we'll just focus on executing `WordCount.java`.

## Optional: Convert from Maven to Gradle Project

Ensure you are in the same directory as the `pom.xml` file generated from the previous step. Automatically convert your project from Maven to Gradle by running:
{{< highlight >}}
$ gradle init
{{< /highlight >}}

After you have converted the project to Gradle:

1. Edit the generated `build.gradle` file by adding `mavenCentral()` under `repositories`:
{{< highlight >}}
repositories {
    mavenCentral()
    maven {
        url = uri('https://repository.apache.org/content/repositories/snapshots/')
    }

    maven {
        url = uri('http://repo.maven.apache.org/maven2')
    }
}
{{< /highlight >}}
1. Add the following task in `build.gradle` to allow you to execute pipelines with Gradle:
{{< highlight >}}
task execute (type:JavaExec) {
    main = System.getProperty("mainClass")
    classpath = sourceSets.main.runtimeClasspath
    systemProperties System.getProperties()
    args System.getProperty("exec.args", "").split()
}
{{< /highlight >}}
1. Rebuild your project by running:
{{< highlight >}}
$ gradle build
{{< /highlight >}}

## Run WordCount

A single Beam pipeline can run on multiple Beam [runners](/documentation#runners), including the [FlinkRunner](/documentation/runners/flink), [SparkRunner](/documentation/runners/spark), [NemoRunner](/documentation/runners/nemo), [JetRunner](/documentation/runners/jet), or [DataflowRunner](/documentation/runners/dataflow). The [DirectRunner](/documentation/runners/direct) is a common runner for getting started, as it runs locally on your machine and requires no specific setup.

After you've chosen which runner you'd like to use:

1.  Ensure you've done any runner-specific setup.
1.  Build your commandline by:
    1. Specifying a specific runner with `--runner=<runner>` (defaults to the [DirectRunner](/documentation/runners/direct))
    1. Adding any runner-specific required options
    1. Choosing input files and an output location are accessible on the chosen runner. (For example, you can't access a local file if you are running the pipeline on an external cluster.)
1.  Run your first WordCount pipeline.

### Run WordCount Using Maven

For Unix shells:

{{< highlight class="runner-direct" >}}
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner
{{< /highlight >}}

{{< highlight class="runner-flink-local" >}}
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=FlinkRunner --inputFile=pom.xml --output=counts" -Pflink-runner
{{< /highlight >}}

{{< highlight class="runner-flink-cluster" >}}
$ mvn package exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=target/word-count-beam-bundled-0.1.jar \
                  --inputFile=/path/to/quickstart/pom.xml --output=/tmp/counts" -Pflink-runner

You can monitor the running job by visiting the Flink dashboard at http://<flink master>:8081
{{< /highlight >}}

{{< highlight class="runner-spark" >}}
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=SparkRunner --inputFile=pom.xml --output=counts" -Pspark-runner
{{< /highlight >}}

{{< highlight class="runner-dataflow" >}}
Make sure you complete the setup steps at /documentation/runners/dataflow/#setup

$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=DataflowRunner --project=<your-gcp-project> \
                  --region=<your-gcp-region> \
                  --gcpTempLocation=gs://<your-gcs-bucket>/tmp \
                  --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://<your-gcs-bucket>/counts" \
     -Pdataflow-runner
{{< /highlight >}}

{{< highlight class="runner-samza-local" >}}
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=/tmp/counts --runner=SamzaRunner" -Psamza-runner
{{< /highlight >}}

{{< highlight class="runner-nemo" >}}
$ mvn package -Pnemo-runner && java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount \
     --runner=NemoRunner --inputFile=`pwd`/pom.xml --output=counts
{{< /highlight >}}

{{< highlight class="runner-jet" >}}
$ mvn package -Pjet-runner
$ java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount \
     --runner=JetRunner --jetLocalMode=3 --inputFile=`pwd`/pom.xml --output=counts
{{< /highlight >}}

For Windows PowerShell:

{{< highlight class="runner-direct" >}}
PS> mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=pom.xml --output=counts" -P direct-runner
{{< /highlight >}}

{{< highlight class="runner-flink-local" >}}
PS> mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=FlinkRunner --inputFile=pom.xml --output=counts" -P flink-runner
{{< /highlight >}}

{{< highlight class="runner-flink-cluster" >}}
PS> mvn package exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=.\target\word-count-beam-bundled-0.1.jar `
               --inputFile=C:\path\to\quickstart\pom.xml --output=C:\tmp\counts" -P flink-runner

You can monitor the running job by visiting the Flink dashboard at http://<flink master>:8081
{{< /highlight >}}

{{< highlight class="runner-spark" >}}
PS> mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=SparkRunner --inputFile=pom.xml --output=counts" -P spark-runner
{{< /highlight >}}

{{< highlight class="runner-dataflow" >}}
Make sure you complete the setup steps at /documentation/runners/dataflow/#setup

PS> mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--runner=DataflowRunner --project=<your-gcp-project> `
               --region=<your-gcp-region> \
               --gcpTempLocation=gs://<your-gcs-bucket>/tmp `
               --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://<your-gcs-bucket>/counts" `
 -P dataflow-runner
{{< /highlight >}}

{{< highlight class="runner-samza-local" >}}
PS> mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
     -D exec.args="--inputFile=pom.xml --output=/tmp/counts --runner=SamzaRunner" -P samza-runner
{{< /highlight >}}

{{< highlight class="runner-nemo" >}}
PS> mvn package -P nemo-runner -DskipTests
PS> java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount `
      --runner=NemoRunner --inputFile=`pwd`/pom.xml --output=counts
{{< /highlight >}}

{{< highlight class="runner-jet" >}}
PS> mvn package -P jet-runner
PS> java -cp target/word-count-beam-bundled-0.1.jar org.apache.beam.examples.WordCount `
      --runner=JetRunner --jetLocalMode=3 --inputFile=$pwd/pom.xml --output=counts
{{< /highlight >}}

### Run WordCount Using Gradle

For Unix shells (Instructions currently only available for Direct, Spark, and Dataflow):

{{< highlight class="runner-direct">}}
$ gradle clean execute -DmainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner
{{< /highlight >}}

{{< highlight class="runner-apex">}}
We are working on adding the instruction for this runner!
{{< /highlight >}}

{{< highlight class="runner-flink-local">}}
We are working on adding the instruction for this runner!
{{< /highlight >}}

{{< highlight class="runner-flink-cluster">}}
We are working on adding the instruction for this runner!
{{< /highlight >}}

{{< highlight class="runner-spark" >}}
$ gradle clean execute -DmainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--inputFile=pom.xml --output=counts" -Pspark-runner
{{< /highlight >}}

{{< highlight class="runner-dataflow" >}}
$ gradle clean execute -DmainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--project=<your-gcp-project> --inputFile=gs://apache-beam-samples/shakespeare/* \
    --output=gs://<your-gcs-bucket>/counts" -Pdataflow-runner
{{< /highlight >}}

{{< highlight class="runner-samza-local">}}
We are working on adding the instruction for this runner!
{{< /highlight >}}

{{< highlight class="runner-nemo">}}
We are working on adding the instruction for this runner!
{{< /highlight >}}

{{< highlight class="runner-jet">}}
We are working on adding the instruction for this runner!
{{< /highlight >}}

## Inspect the results

Once the pipeline has completed, you can view the output. You'll notice that there may be multiple output files prefixed by `count`. The exact number of these files is decided by the runner, giving it the flexibility to do efficient, distributed execution.

{{< highlight class="runner-direct" >}}
$ ls counts*
{{< /highlight >}}

{{< highlight class="runner-flink-local" >}}
$ ls counts*
{{< /highlight >}}

{{< highlight class="runner-flink-cluster" >}}
$ ls /tmp/counts*
{{< /highlight >}}

{{< highlight class="runner-spark" >}}
$ ls counts*
{{< /highlight >}}

{{< highlight class="runner-dataflow" >}}
$ gsutil ls gs://<your-gcs-bucket>/counts*
{{< /highlight >}}

{{< highlight class="runner-samza-local" >}}
$ ls /tmp/counts*
{{< /highlight >}}

{{< highlight class="runner-nemo" >}}
$ ls counts*
{{< /highlight >}}

{{< highlight class="runner-jet" >}}
$ ls counts*
{{< /highlight >}}

When you look into the contents of the file, you'll see that they contain unique words and the number of occurrences of each word. The order of elements within the file may differ because the Beam model does not generally guarantee ordering, again to allow runners to optimize for efficiency.

{{< highlight class="runner-direct" >}}
$ more counts*
api: 9
bundled: 1
old: 4
Apache: 2
The: 1
limitations: 1
Foundation: 1
...
{{< /highlight >}}

{{< highlight class="runner-flink-local" >}}
$ more counts*
The: 1
api: 9
old: 4
Apache: 2
limitations: 1
bundled: 1
Foundation: 1
...
{{< /highlight >}}

{{< highlight class="runner-flink-cluster" >}}
$ more /tmp/counts*
The: 1
api: 9
old: 4
Apache: 2
limitations: 1
bundled: 1
Foundation: 1
...
{{< /highlight >}}

{{< highlight class="runner-spark" >}}
$ more counts*
beam: 27
SF: 1
fat: 1
job: 1
limitations: 1
require: 1
of: 11
profile: 10
...
{{< /highlight >}}


{{< highlight class="runner-dataflow" >}}
$ gsutil cat gs://<your-gcs-bucket>/counts*
feature: 15
smother'st: 1
revelry: 1
bashfulness: 1
Bashful: 1
Below: 2
deserves: 32
barrenly: 1
...
{{< /highlight >}}

{{< highlight class="runner-samza-local" >}}
$ more /tmp/counts*
api: 7
are: 2
can: 2
com: 14
end: 14
for: 14
has: 2
...
{{< /highlight >}}

{{< highlight class="runner-nemo" >}}
$ more counts*
cluster: 2
handler: 1
plugins: 9
exclusions: 14
finalName: 2
Adds: 2
java: 7
xml: 1
...
{{< /highlight >}}

{{< highlight class="runner-jet" >}}
$ more counts*
FlinkRunner: 1
cleanupDaemonThreads: 2
sdks: 4
unit: 1
Apache: 3
IO: 2
copyright: 1
governing: 1
overrides: 1
YARN: 1
...
{{< /highlight >}}

## Next Steps

* Learn more about the [Beam SDK for Java](/documentation/sdks/java/)
  and look through the [Java SDK API reference](https://beam.apache.org/releases/javadoc).
* Walk through these WordCount examples in the [WordCount Example Walkthrough](/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/documentation/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
