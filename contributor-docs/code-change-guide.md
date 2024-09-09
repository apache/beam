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

# Beam code change guide

Last Updated: Apr 18, 2024

This guide is for Beam users and developers who want to change or test Beam code.
Specifically, this guide provides information about:

- Testing code changes locally

- Building Beam artifacts with modified Beam code and using the modified code for pipelines

The guide contains the following sections:

- **[Repository structure](#repository-structure)**: A description of the Apache Beam GitHub
  repository, including steps for setting up your [Gradle project](#gradle-quick-start) and
  for verifying the configuration of your [develoment environment](#environment-setup).

- **[Java Guide](#java-development-guide)**: Guidance for setting up a Java environment,
  running and writing integration tests, and running a pipeline with modified
  Beam code.

- **[Python Guide](#python-development-guide)**: Guidance for configuring your console
  for Python development, running unit and integration tests, and running a pipeline
  with modified Beam code.

## Repository structure

The Apache Beam GitHub repository (Beam repo) is, for the most part, a "mono repo".
It contains everything in the Beam project, including the SDK, test
infrastructure, dashboards, the [Beam website](https://beam.apache.org),
and the [Beam Playground](https://play.beam.apache.org).

### Code paths

The following example code paths in the Beam repo are relevant for SDK development.

#### Java

Java code paths are mainly found in two directories: `sdks/java` and `runners`.
The following list provides notes about the contents of these directories and
some of the subdirectories.

* `sdks/java` - Java SDK
  * `sdks/java/core` - Java core
  * `sdks/java/harness` - SDK harness (entrypoint of SDK container)

* `runners` - Java runner supports, including the following items:
  * `runners/direct-java` - Java direct runner
  * `runners/flink-java` - Java Flink runner
  * `runners/google-cloud-dataflow-java` - Dataflow runner (job submission, translation, and so on)
    * `runners/google-cloud-dataflow-java/worker` - Worker for Dataflow jobs that don't use Runner v2

#### Non-Java SDKs

For SDKs in other languages, the `sdks/LANG` directory contains the relevant files.
The following list provides notes about the contents of some of the subdirectories.

* `sdks/python` - Setup file and scripts to trigger test-suites
  * `sdks/python/apache_beam` - The Beam package
    * `sdks/python/apache_beam/runners/worker` - SDK worker harness entrypoint and state sampler
    * `sdks/python/apache_beam/io` - I/O connectors
    * `sdks/python/apache_beam/transforms` - Most core components
    * `sdks/python/apache_beam/ml` - Beam ML code
    * `sdks/python/apache_beam/runners` - Runner implementations and wrappers
    * ...

* `sdks/go` - Go SDK

* `.github/workflow` - GitHub action workflows, such as the tests that run during a pull request. Most
  workflows run a single Gradle command. To learn which command to run locally during development,
  during tests, check which command is running.

### Gradle quick start

The Beam repo is a single Gradle project that contains all components, including Java, Python,
Go, and the website. Before you begin development, familiarize yourself with the Gradle
project structure by reviewing
[Structuring Projects with Gradle](https://docs.gradle.org/current/userguide/multi_project_builds.html)
in the Gradle documentation.

#### Gradle key concepts

Grade uses the following key concepts:

* **project**: a folder that contains the `build.gradle` file
* **task**: an action defined in the `build.gradle` file
* **plugin**: predefined tasks and hierarchies; runs in the project's `build.gradle` file

Common tasks for a Java project or subproject include:

- `compileJava` - compiles the Java source files
- `compileTestJava` - compiles the Java test source files
- `test` - runs unit tests
- `integrationTest` - runs integration tests

To run a Gradle task, use the command `./gradlew -p <PROJECT_PATH> <TASK>` or the command `./gradlew :<PROJECT>:<PATH>:<TASK>`. For example:

```
./gradlew -p sdks/java/core compileJava

./gradlew :sdks:java:harness:test
```

#### Beam-specific Gradle project configuration

For Apache Beam, one plugin manages everything: `buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin`.
The `BeamModulePlugin` is used for the following tasks:

* Manage Java dependencies
* Configure projects such as Java, Python, Go, Proto, Docker, Grpc, and Avro
  * For Java, use `applyJavaNature`; for Python, use `applyPythonNature`
  * Define common custom tasks for each type of project
    * `test`: run Java unit tests
    * `spotlessApply`: format Java code

In every Java project or subproject, the `build.gradle` file starts with the following code:

```groovy

apply plugin: 'org.apache.beam.module'

applyJavaNature( ... )
```

### Environment setup

To set up a local development environment, first review the [Contribution guide](../CONTRIBUTING.md).
If you plan to use Dataflow, you need to set up `gcloud` credentials. To set up `gcloud` credentials, see
[Create a Dataflow pipeline using Java](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java)
in the Google Cloud documentation.

Depending on the languages involved, your `PATH` file needs to have the following elements configured.

* A Java environment that uses a supported Java version, preferably Java 8.
  * This environment is needed for all development, because Beam is a Gradle project that uses JVM.
  * Recommended: To manage Java versions, use [sdkman](https://sdkman.io/install).

* A Python environment that uses any supported Python version.
  * This environment is needed for Python SDK development.
  * Recommended: To manage Python versions, use [`pyenv`](https://github.com/pyenv/pyenv) and
    a [virtual environment](https://docs.python.org/3/library/venv.html).

* A Go environment that uses latest Go version.
  * This environment is needed for Go SDK development.
  * This environment is also needed for SDK container changes for all SDKs, because
    the container entrypoint scripts are written in Go.

* A Docker environment. This environment is needed for the following tasks:
  * SDK container changes.
  * Some cross-language functionality (if you run an SDK container image; not required in Beam 2.53.0 and later verions).
  * Portable runners, such as using job server.

The following list provides examples of when you need specific environemnts.

- When you test the code change in `sdks/java/io/google-cloud-platform`, you need a Java environment.
- When you test the code change in `sdks/java/harness`, you need a Java environment, a Go
  environment, and Docker environment. You need the Docker environment to compile and build the Java SDK harness container image.
- When you test the code change in `sdks/python/apache_beam`, you need a Python environment.

## Java development guide

This section provides guidance for setting up your environment to modify or test Java code.

### IDE (IntelliJ) setup

To set up IntelliJ, follow these steps. The IDE isn't required for changing the code and testing.
You can run tests can by using a Gradle command line, as described in the Console setup section.

1. From IntelliJ, open `/beam` (**Important:** Open the repository root directory, not
  `sdks/java`).

2. Wait for indexing. Indexing might take a few minutes.

Because Gradle is a self-contained build tool, if the prerequisites are met, the environment setup is complete.

To verify whether the load is successful, follow these steps:

1. Find the file `examples/java/build.gradle`.
2. Next to the wordCount task, a **Run** button is present. Click **Run**. The wordCount example compiles and runs.

<img width="631" alt="image" src="https://github.com/apache/beam/assets/8010435/f5203e8e-0f9c-4eaa-895b-e16f68a808a2">

### Console setup

To run tests by using the Gradle command line (shell), in the command-line environment, run the following command.
This command compiles the Apache Beam SDK, the WordCount pipeline, and a Hello-world program for
data processing. It then runs the pipeline on the Direct Runner.

```shell
$ cd beam
$ ./gradlew :examples:java:wordCount
```

When the command completes successfully, the following text appears in the Gradle build log:

```
...
BUILD SUCCESSFUL in 2m 32s
96 actionable tasks: 9 executed, 87 up-to-date
3:41:06 PM: Execution finished 'wordCount'.
```

In addition, the following text appears in the output file:

```shell

$ head /tmp/output.txt*
==> /tmp/output.txt-00000-of-00003 <==
should: 38
bites: 1
depraved: 1
gauntlet: 1
battle: 6
sith: 2
cools: 1
natures: 1
hedge: 1
words: 9

==> /tmp/output.txt-00001-of-00003 <==
elements: 1
Advise: 2
fearful: 2
towards: 4
ready: 8
pared: 1
left: 8
safe: 4
canst: 7
warrant: 2

==> /tmp/output.txt-00002-of-00003 <==
chanced: 1
...
```

### Run a unit test

This section explains how to run unit tests locally after you make a code change in the Java SDK, for example, in `sdks/java/io/jdbc`.

Tests are stored in the `src/test/java` folder of each project. Unit tests have the filename `.../**Test.java`. Integration tests have the filename `.../**IT.java`.

* To run all unit tests under a project, use the following command:
  ```
  ./gradlew :sdks:java:harness:test
  ```
  Find the  JUnit report in an HTML file in the file path `<invoked_project>/build/reports/tests/test/index.html`.

* To run a specific test, use the following commands:

  ```
  ./gradlew :sdks:java:harness:test --tests org.apache.beam.fn.harness.CachesTest
  ./gradlew :sdks:java:harness:test --tests *CachesTest
  ./gradlew :sdks:java:harness:test --tests *CachesTest.testClearableCache
  ```

* To run tests using IntelliJ, click the ticks to run either a whole test class or a specific test. To debug the test, set breakpoints.

  <img width="452" alt="image" src="https://github.com/apache/beam/assets/8010435/7ae2a65c-a104-48a2-8bad-ff8c52dd1943">

* These steps don't apply to `sdks:java:core` tests. To invoke those unit tests, use the command `:runners:direct-java:needsRunnerTest`. Java core doesn't depend on a runner. Therefore, unit tests that run a pipeline require the Direct Runner.

To run integration tests, use the Direct Runner.

### Run integration tests

Integration tests have the filename `.../**IT.java`. They use [`TestPipeline`](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/TestPipeline.java). Set options by using `TestPipelineOptions`.

Integration tests differ from standard pipelines in the following ways:

* By default, they block on run (on `TestDataflowRunner`).
* They have a default timeout of 15 minutes.
* The pipeline options are set in the system property `beamTestPipelineOptions`.

To configure the test pipeline, you need to set the property `-DbeamTestPipelineOptions=[...]`. This property sets the pipeline option that the test uses, for example,

```
-DbeamTestPipelineOptions='["--runner=TestDataflowRunner","--project=mygcpproject","--region=us-central1","--stagingLocation=gs://mygcsbucket/path"]'
```

For some projects, `beamTestPipelineOptions` is explicitly configured in `build.gradle`.
Checkout the sources of the corresponding build file for setting. For example,
in `sdks/java/io/google-cloud-platform/build.gradle`, it sets `beamTestPipelineOptions`
from project properties 'gcpProject', 'gcpTempRoot', etc, and when not assigned,
it defaults to `apache-beam-testing` GCP project. To run the test in your own project,
assign these project properties with command line:

```
./gradlew :sdks:java:io:google-cloud-platform:integrationTest -PgcpProject=<mygcpproject> -PgcpTempRoot=<gs://mygcsbucket/path>
```

Some other projects (e.g. `sdks/java/io/jdbc`, `sdks/java/io/kafka`) does not
assemble (overwrite) `beamTestPipelineOptions` in `build.gradle`, then just set
it explicitly with `-DbeamTestPipelineOptions='[...]'`, as aforementioned.

#### Write integration tests

To set up a `TestPipeline` object in an integration test, use the following code:

```java
@Rule public TestPipeline pipelineWrite = TestPipeline.create();

@Test
public void testSomething() {
  pipeline.apply(...);

  pipeline.run().waitUntilFinish();
}
```

The task that runs the test needs to specify the runner. The following examples demonstrate how to specify the runner:

* To run a Google Cloud I/O integration test on the Direct Runner, use the
  command `:sdks:java:io:google-cloud-platform:integrationTest`.
* To run integration tests on the standard Dataflow runner, use the command
  `:runners:google-cloud-dataflow-java:googleCloudPlatformLegacyWorkerIntegrationTest`.
* To run integration test on Dataflow runner v2, use the command
  `:runners:google-cloud-dataflow-java:googleCloudPlatformRunnerV2IntegrationTest`.

To see how to run your workflow locally, refer to the Gradle command that the GitHub Action workflow runs.

The following commands demonstrate an example invocation:

```
./gradlew :runners:google-cloud-dataflow-java:examplesJavaRunnerV2IntegrationTest \
-PdisableSpotlessCheck=true -PdisableCheckStyle=true -PskipCheckerFramework \
-PgcpProject=<your_gcp_project> -PgcpRegion=us-central1 \
-PgcsTempRoot=gs://<your_gcs_bucket>/tmp
```

### Run your pipeline with modified beam code

To apply code changes to your pipeline, we recommend that you start with a separate branch.

* If you're making a pull request or want to test a change with the dev branch, start from
  Beam HEAD ([master](https://github.com/apache/beam/tree/master)).

* If you're making a patch on released Beam (2._xx_.0), start from a tag, such as
  [v2.55.0](https://github.com/apache/beam/tree/v2.55.0). Then, in the Beam repo,
  use the following command to compile the project that includes the code change.
  This example modifies `sdks/java/io/kafka`.

  ```
  ./gradlew -Ppublishing -p sdks/java/io/kafka publishToMavenLocal
  ```
  By default, this command publishes the artifact with modified code to the Maven Local
  repository (`~/.m2/repository`). The change is picked up when the user pipeline runs.

If your code change is made in a development branch, such as on Beam master or a PR, the
artifact is produced under version `2.xx.0-SNAPSHOT` instead of on a release tag. To pick
up this dependency, you need to make additional configurations in your pipeline project.
The following examples provide guidance for making configurations in Maven and Gradle.

Follow these steps for Maven projects.

1. Recommended: Use the WordCount `maven-archetype` as a template to set up your project (https://beam.apache.org/get-started/quickstart-java/).

2. To add a snapshot repository, include the following elements:
    ```xml
    <repository>
      <id>Maven-Snapshot</id>
      <name>maven snapshot repository</name>
      <url>https://repository.apache.org/content/groups/snapshots/</url>
    </repository>
    ```

3. In the `pom.xml` file, modify the value of `beam.version`:
    ```xml
    <properties>
    <beam.version>2.XX.0-SNAPSHOT</beam.version>
    ```

Follow these steps for Gradle projects.

1. In the `build.gradle` file, add the following code:

    ```groovy
    repositories {
    maven { url "https://repository.apache.org/content/groups/snapshots" }
    }
    ```

2. Set the Beam dependency versions to the following value: `2.XX.0-SNAPSHOT`.

This configuration directs the build system to download Beam nightly builds from the Maven
Snapshot Repository. The local build that you edited isn't downloaded. You usually don't
need to build all Beam artifacts locally. If you do need to build all Beam artifacts locally,
use the following command for all projects `./gradlew -Ppublishing  publishToMavenLocal`.

The following situations require additional consideration.

If you're using the standard Dataflow runner (not Runner v2), and the worker harness has changed, do the following:

1. Use the following command to compile `dataflowWorkerJar`:

    ```
    ./gradlew :runners:google-cloud-dataflow-java:worker:shadowJar
    ```
    The jar is located in the build output.

2. Use the following command to pass `pipelineOption`:

    ```
    --dataflowWorkerJar=/.../beam-runners-google-cloud-dataflow-java-legacy-worker-2.XX.0-SNAPSHOT.jar
    ```

If you're using Dataflow Runner v2 and `sdks/java/harness` or its dependencies (like `sdks/java/core`) have changed, do the following:

1. Use the following command to build the SDK harness container:

    ```shell
    ./gradlew :sdks:java:container:java8:docker # java8, java11, java17, etc
  docker tag apache/beam_java8_sdk:2.49.0.dev \
    "us.gcr.io/apache-beam-testing/beam_java11_sdk:2.49.0-custom"  # change to your container registry
  docker push "us.gcr.io/apache-beam-testing/beam_java11_sdk:2.49.0-custom"
    ```

2. Run the pipeline with the following options:

  ```
  --experiments=use_runner_v2 \
  --sdkContainerImage="us.gcr.io/apache-beam-testing/beam_java11_sdk:2.49.0-custom"
  ```

#### Snapshot Version Containers

By default, a Snapshot version for an SDK under development will use the containers published to the [apache-beam-testing project's container registry](https://us.gcr.io/apache-beam-testing/github-actions). For example, the most recent snapshot container for Java 17 can be found [here](https://us.gcr.io/apache-beam-testing/github-actions/beam_java17_sdk).

When a version is entering the [release candidate stage](https://github.com/apache/beam/blob/master/contributor-docs/release-guide.md), one final SNAPSHOT version will be published.
This SNAPSHOT version will use the final containers published on [DockerHub](https://hub.docker.com/search?q=apache%2Fbeam).

**NOTE:** During the release process, there may be some downtime where a container is not available for use for a SNAPSHOT version. To avoid this, it is recommended to either switch to the latest SNAPSHOT version available or to use [custom containers](https://beam.apache.org/documentation/runtime/environments/#custom-containers). You should also only rely on snapshot versions for important workloads if absolutely necessary.

Certain runners may override this snapshot behavior; for example, the Dataflow runner overrides all SNAPSHOT containers into a [single registry](https://console.cloud.google.com/gcr/images/cloud-dataflow/GLOBAL/v1beta3). The same downtime will still be incurred, however, when switching to the final container

## Python guide

The Beam Python SDK is distributed as a single wheel, which is more straightforward than the Java SDK.

### Console setup

These instructions explain how to configure your console (shell) for Python development. In this example, the working directory is set to `sdks/python`.

1. Recommended: Install the Python interpreter by using `pyenv`. Use the following commands:

  1. `install prerequisites`
  2. `curl https://pyenv.run | bash`
  3. `pyenv install 3.X` (a supported Python version; see `python_version` in [project property](https://github.com/apache/beam/blob/master/gradle.properties)

2. Use the following commands to set up and activate the virtual environment:

  1. `pyenv virtualenv 3.X ENV_NAME`
  2. `pyenv activate ENV_NAME`

3. Install the `apache_beam` package in editable mode:
  `pip install -e .[gcp, test]`

4. For development that uses an SDK container image, do the following:

  1. Install Docker Desktop.
  2. Install Go.

5. If you're going to submit PRs, use the following command to precommit the hook for Python code changes (nobody likes lint failures!!):

  ```shell
  # enable pre-commit
  (env) $ pip install pre-commit
  (env) $ pre-commit install

  # disable pre-commit
  (env) $ pre-commit uninstall
  ```

### Run a unit test

Although the tests can be triggered with a Gradle command, that method sets up a new `virtualenv` and installs dependencies before each run, which takes minutes. Therefore, it's useful to have a persistent `virtualenv`.

Unit tests have the filename `**_test.py`.

To run all tests in a file, use the following command:

```shell
pytest -v  apache_beam/io/textio_test.py
```

To run all tests in a class, use the following command:

```shell
pytest -v  apache_beam/io/textio_test.py::TextSourceTest
```

To run a specific test, use the following command:

```shell
pytest -v  apache_beam/io/textio_test.py::TextSourceTest::test_progress
```

### Run an integration test

Integration tests have the filename `**_it_test.py`.

To run an integration test on the Direct Runner, use the following command:

```shell
python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDirectRunner’
```

If you're preparing a PR, for test-suites to run in PostCommit Python, add tests paths under [`batchTests`](https://github.com/apache/beam/blob/2012107a0fa2bb3fedf1b5aedcb49445534b2dad/sdks/python/test-suites/direct/common.gradle#L44) in the `common.gradle` file.

To run an integration test on the Dataflow Runner, follow these steps:

1. To build the SDK tarball, use the following command:

  ```
  cd sdks/python
  pip install build && python -m build --sdist
  ```
  The tarball file is generated in the `sdks/python/sdist/` directory.

2. To specify the tarball file, use the `--test-pipeline-options` parameter. Use the location `--sdk_location=dist/apache-beam-2.53.0.dev0.tar.gz`. The following example shows the complete command:

  ```shell
  python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_location=dist/apache-beam-2.35.0.dev0.tar.gz
                           --region=us-central1’
  ```

3. If you're preparing a PR, to include integration tests in the Python PostCommit test
   suite's Dataflow task, use the marker `@pytest.mark.it_postcommit`.


#### Build containers for modified SDK code

To build containers for modified SDK code, follow these steps.

1. Run the following command:
  ```shell
  ./gradlew :sdks:python:container:py39:docker \
  -Pdocker-repository-root=<gcr.io/location> -Pdocker-tag=<tag>
  ```
2. Push the containers.
3. Specify the container location by using the `--sdk_container_image` option.

The following example shows a complete command:

```shell
python -m pytest  -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_container_image=us.gcr.io/apache-beam-testing/beam-sdk/beam:dev
                           --region=us-central1’
```

#### Specify additional test dependencies

This section provides two options for specifying additional test dependencies.

Use the `--requirements_file` options. The following example demonstrates how to use the `--requirements_file` options:

```shell
python -m pytest  -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_location=us.gcr.io/apache-beam-testing/beam-sdk/beam:dev
                           --region=us-central1
                           –requirements_file=requirements.txt’
```

If you're using the Dataflow runner, use [custom containers](https://cloud.google.com/dataflow/docs/guides/using-custom-containers).
You can use the [official Beam SDK container image](https://gcr.io/apache-beam-testing/beam-sdk) as a base and then apply your changes.

### Run your pipeline with modified beam code

To run your pipeline with modified beam code, follow these steps:

1. Build the Beam SDK tarball. Under `sdks/python`, run `python -m build --sdist`. For more details,
  see [Run an integration test](#run-an-integration-test) on this page.

2. Install the Apache Beam Python SDK in your Python virtual environment with the necessary
  extensions. Use a command similar to the following example: `pip install /path/to/apache-beam.tar.gz[gcp]`.

3. Initiate your Python script. To run your pipeline, use a command similar to the following example:

  ```shell
  python my_pipeline.py --runner=DataflowRunner --sdk_location=/path/to/apache-beam.tar.gz --project=my_project --region=us-central1 --temp_location=gs://my-bucket/temp ...
  ```

Tips for using the Dataflow runner:

* The Python worker installs the Apache Beam SDK before processing work items. Therefore, you don't usually need to provide a custom worker container. If your Google Cloud VM doesn't have internet access and transient dependencies are changed from the officially released container images, you do need to provide a custom worker container. In this case, see [Build containers for modified SDK code](#build-containers-for-modified-sdk-code) on this page.

* Installing the Beam Python SDK from source can be slow (3.5 minutes for  a`n1-standard-1` machine). As an alternative, if the host machine uses amd64 architecture, you can build a wheel instead of a tarball by using a command similar to `./gradle :sdks:python:bdistPy311linux` (for Python 3.11). To pass the built wheel, use the `--sdk_location` option. That installation completes in seconds.

#### Caveat - `save_main_session`

* `NameError` when running `DoFn` on remote runner
* Global imports, functions, and variables in main pipeline module are not serialized by default
* Use `--save_main_session` pipeline option to enable it

<!-- TODO # Go Guide -->

<!-- # Cross-language Guide -->

## Appendix

### Directories of snapshot builds

* https://repository.apache.org/content/groups/snapshots/org/apache/beam/ Java SDK build (nightly)
* https://gcr.io/apache-beam-testing/beam-sdk Beam SDK container build (Java, Python, Go, every 4 hrs)
* https://gcr.io/apache-beam-testing/beam_portability Portable runner (Flink, Spark) job server container (nightly)
* gs://beam-python-nightly-snapshots Python SDK build (nightly)
