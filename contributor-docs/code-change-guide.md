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

Last Updated: Apr 18, 2024

This guide is for Beam users and developers changing and testing Beam code.
Specifically, this guide provides information about:

1. Testing code changes locally

2. Building Beam artifacts with modified Beam code and using the modified code for pipelines

# Repository structure

The Apache Beam GitHub repository (Beam repo) is, for the most part, a "mono repo":
it contains everything in the Beam project, including the SDK, test
infrastructure, dashboards, the [Beam website](https://beam.apache.org),
the [Beam Playground](https://play.beam.apache.org), and so on.

## Gradle quick start

The Beam repo is a single Gradle project that contains all components, including Python,
Go, the website, etc. It is useful to familiarize yourself with the Gradle project structure:
https://docs.gradle.org/current/userguide/multi_project_builds.html

### Gradle key concepts

Grade uses the following key concepts:

* **project**: a folder that contains the `build.gradle` file
* **task**: an action defined in the `build.gradle` file
* **plugin**: runs in the project's `build.gradle` and contains predefined tasks and hierarchies

For example, common tasks for a Java project or subproject include:

- `compileJava`
- `compileTestJava`
- `test`
- `integrationTest`

To run a Gradle task, the command is `./gradlew -p <project path> <task>` or `./gradlew :project:path:task_name`. For example:

```
./gradlew -p sdks/java/core compileJava

./gradlew :sdks:java:harness:test
```

### Gradle project configuration: Beam specific

* A **huge** plugin `buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin` manages everything.

In each java project or subproject, the `build.gradle` file starts with:

```groovy

apply plugin: 'org.apache.beam.module'

applyJavaNature( ... )
```

Relevant usage of `BeamModulePlugin` includes:
* Manage Java dependencies
* Configure projects (Java, Python, Go, Proto, Docker, Grpc, Avro, an so on)
  * Java -> `applyJavaNature`; Python -> `applyPythonNature`, and so on
  * Define common custom tasks for each type of project
    * `test`: run Java unit tests
    * `spotlessApply`: format java code

## Code paths

The following are example code paths relevant for SDK development:

Java code paths are mainly found in two directories:

* `sdks/java` Java SDK
  * `sdks/java/core` Java core
  * `sdks/java/harness` SDK harness (entrypoint of SDK container)

* `runners` Java runner supports. For example,
  * `runners/direct-java` Java direct runner
  * `runners/flink-java` Java Flink runner
  * `runners/google-cloud-dataflow-java` Dataflow runner (job submission, translation, etc)
    * `runners/google-cloud-dataflow-java/worker` Worker on Dataflow legacy runner

For SDKS in other language, all relevant files are in `sdks/LANG`, for example,

* `sdks/python` contains the setup file and scripts to trigger test-suites
  * `sdks/python/apache_beam` actual beam package
    * `sdks/python/apache_beam/runners/worker` SDK worker harness entrypoint, state sampler
    * `sdks/python/apache_beam/io` I/O connectors
    * `sdks/python/apache_beam/transforms` most "core" components
    * `sdks/python/apache_beam/ml` Beam ML
    * `sdks/python/apache_beam/runners` runner implementations and wrappers
    * ...

* `sdks/go` Go SDK

* `.github/workflow` GitHub action workflows (for example, tests run under PR). Most
  workflows run a single Gradle command. Check which command is running for
  a test so that you can run the same command locally during development.

## Environment setup

To set up local development environments, first see the [Contributing guide](../CONTRIBUTING.md) .
If you plan to use Dataflow, see the [Google Cloud documentation](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java) to setup `gcloud` credentials.

To check if your environment is set up, follow these steps:

Depending on the languages involved, your `PATH` needs to have the following elements configured.

* A Java environment (any supported Java version, Java8 preferably as of 2024).
  * This environment is needed for all development, because Beam is a Gradle project that uses JVM.
  * Recommended: Use [sdkman](https://sdkman.io/install) to manage Java versions.
* A Python environment (any supported Python version)
  * Needed for Python SDK development
  * Recommended: Use [`pyenv`](https://github.com/pyenv/pyenv) and
    a [virtual environment](https://docs.python.org/3/library/venv.html) to manage Python versions.
* A Go environment. Install the latest Go version.
  * Needed for Go SDK development and SDK container change (for all SDKs), because
  the container entrypoint scripts are written in Go.
* A Docker environment.
  * Needed for SDK container changes, some cross-language functionality (if you run a
    SDK container image; not required since Beam 2.53.0), portable runners (using job server), etc.

For example:
- When you test the code change in `sdks/java/io/google-cloud-platform`, you need a Java environment.
- When you test the code change in `sdks/java/harness`, you need a Java environment, a Go
  environment, and Docker environment. You need the Docker environment to compile and build the Java SDK harness container image.
- When you test the code change in `sdks/python/apache_beam`, you need a Python environment.

# Java guide
This section provides guidance for setting up your Java environment.
## IDE (IntelliJ) setup

1. From IntelliJ, open `/beam` (**Important:** Open the repository root directory, instead of
  `sdks/java`).

2. Wait for indexing. Indexing might take a few minutes.

If the prerequisites are met, the environment set up is complete, because Gradle is a self-contained build tool.

To verify whether the load is successful, find the file `examples/java/build.gradle`. Next to the wordCount task,
a **Run** button is present. Click **Run**. The wordCount example compiles and runs.

<img width="631" alt="image" src="https://github.com/apache/beam/assets/8010435/f5203e8e-0f9c-4eaa-895b-e16f68a808a2">

**Note:** The IDE is not required for changing the code and testing.
You can run tests can by using a Gradle command line, as described in the Console (shell) setup section.

## Console (shell) setup

To run tests by using the Gradle command line, in the command-line environment, run the following command:

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

*What does this command do?*

This command compiles the beam SDK and the WordCount pipeline, a Hello-world program for
data processing, then runs the pipeline on the Direct Runner.

## Run a unit test

This section explains how to run unit tests locally after you make a code change in the Java SDK (for example, in `sdks/java/io/jdbc`).

Tests are under the `src/test/java` folder of each project. Unit tests have the filename `.../**Test.java`. Integration tests have the filename `.../**IT.java`.

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

* To run tests using IntelliJ, click the ticks to run a whole test class or a specific test. You can set breakpoints to debug the test.
  <img width="452" alt="image" src="https://github.com/apache/beam/assets/8010435/7ae2a65c-a104-48a2-8bad-ff8c52dd1943">

* These steps don't apply to `sdks:java:core` tests. Instead, invoke the unit tests by using `:runners:direct-java:needsRunnerTest`. Java core doesn't depend on a runner. Therefore, unit tests that run a pipeline require the Direct Runner.


To run integration tests, use the Direct Runner.

## Run integration tests (*IT.java)

Integration tests use [`TestPipeline`](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/TestPipeline.java). Set options by using `TestPipelineOptions`.

Integration tests differ from standard pipelines in the following ways:
* By default, they block on run (on TestDataflowRunner).
* They have a default timeout of 15 minutes.
* The pipeline options are set in the system property `beamTestPipelineOptions`.

Note the final difference, because you need to configure the test by setting `-DbeamTestPipelineOptions=[...]`. This property is where you set the runner to use.

The following example demonstrates how to run an integration test by using the command line. This example includes the options required to run the pipeline on the Dataflow runner:
```
-DbeamTestPipelineOptions='["--runner=TestDataflowRunner","--project=mygcpproject","--region=us-central1","--stagingLocation=gs://mygcsbucket/path"]'
```

### Write an integration test

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
  * To run a Google Cloud I/O integration test on the Direct Runner, use `:sdks:java:io:google-cloud-platform:integrationTest`
  * To run integration tests on the standard Dataflow runner, use `:runners:google-cloud-dataflow-java:googleCloudPlatformLegacyWorkerIntegrationTest`
  * To run integration test on Dataflow runner v2, use `:runners:google-cloud-dataflow-java:googleCloudPlatformRunnerV2IntegrationTest`

To see how to run your workflow locally, refer to the Gradle command that the GitHub Action workflow runs.

Example invocation:
```
./gradlew :runners:google-cloud-dataflow-java:examplesJavaRunnerV2IntegrationTest \
-PdisableSpotlessCheck=true -PdisableCheckStyle=true -PskipCheckerFramework \
-PgcpProject=<your_gcp_project> -PgcpRegion=us-central1 \
-PgcsTempRoot=gs://<your_gcs_bucket>/tmp
```

## Run your pipeline with modified beam code

To apple code changes to your pipeline, we recommend that you start with a separate branch.

* If you're making a pull request or want to test a change with the dev branch, start from Beam HEAD ([master](https://github.com/apache/beam/tree/master)).

* If you're making a patch on released Beam (2._xx_.0), start from a tag (for example, [v2.55.0](https://github.com/apache/beam/tree/v2.55.0)), then in the Beam repo, compile the project involving the code change with the following command. This example modifies `sdks/java/io/kafka`.

  ```
  ./gradlew -Ppublishing -p sdks/java/io/kafka publishToMavenLocal
  ```
  By default, this command publishes the artifact with modified code to the Maven Local repository (`~/.m2/repository`). The change is picked up when the user pipeline runs.

If your code change is made in a development branch, such as on Beam master or a PR, instead of on a release tag, the artifact is produced under version `2.xx.0-SNAPSHOT`. You need to make additional configurations in your pipeline project in order to pick up this dependency. The following examples provide guidance for making configurations in Maven and Gradle.

* Follow these steps for Maven projects.
  1. Recommended: Use the WordCount `maven-archetype` as a template to set up your project (https://beam.apache.org/get-started/quickstart-java/).
  2. To add a snapshot repository, include the following elements:
    ```xml
    <repository>
      <id>Maven-Snapshot</id>
      <name>maven snapshot repository</name>
      <url>https://repository.apache.org/content/groups/snapshots/</url>
    </repository>
  3. In the `pom.xml` file, modify the value of `beam.version`:
    ```xml
    <properties>
    <beam.version>2.XX.0-SNAPSHOT</beam.version>
    ```

* Follow these steps for Gradle projects.
  1. In the `build.gradle` file, add the following code:
    ```groovy
    repositories {
    maven { url "https://repository.apache.org/content/groups/snapshots" }
    }
    ```
  2. Set the beam dependency versions to the following value: `2.XX.0-SNAPSHOT`.

This configuration directs the build system to download Beam nightly builds from the Maven Snapshot Repository. The local build that you edited isn't downloaded. You don't need to build all Beam artifacts locally. If you do need to build all Beam artifacts locally, use the following command for all projects `./gradlew -Ppublishing  publishToMavenLocal`.

The following situations require additional consideration.

* If you're using the standard Dataflow runner (not Runner v2), and the worker harness has changed, do the following:
  1. Use the following command to compile `dataflowWorkerJar`:
    ```
    ./gradlew :runners:google-cloud-dataflow-java:worker:shadowJar
    ```
    The jar is located in the build output.
  2. Use the following command to pass `pipelineOption`:
    ```
    --dataflowWorkerJar=/.../beam-runners-google-cloud-dataflow-java-legacy-worker-2.XX.0-SNAPSHOT.jar
    ```

* If you're using Dataflow Runner v2 and `sdks/java/harness` or its dependency (like `sdks/java/core`) have changed, do the following:
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

# Python guide

The Beam Python SDK is distributed as a single wheel, which is more straightforward than the Java SDK. Python development is consequently less complicated.

## Console (shell) setup

These instructions explain how to configure your console. In this example, the working directory is set to `sdks/python`.

1. Recommended: Install the Python interpreter by using `pyenv`. Use the following commands:
  a. install prerequisites
  b. `curl https://pyenv.run | bash`
  c. `pyenv install 3.X` (a supported Python version, refer to python_version in [project property](https://github.com/apache/beam/blob/master/gradle.properties)
2. Use the following commands to set up and activate the virtual environment:
  a. `pyenv virtualenv 3.X ENV_NAME`
  b. `pyenv activate ENV_NAME`
3. Install the `apache_beam` package in editable mode:
  `pip install -e .[gcp, test]`
4. For development that uses an SDK container image, do the following:
  a. Install Docker Desktop
  b. Install Go
5. If you're going to submit PRs, precommit the hook for Python code changes (nobody likes lint failures!!):
  ```shell
  # enable pre-commit
  (env) $ pip install pre-commit
  (env) $ pre-commit install

  # disable pre-commit
  (env) $ pre-commit uninstall
  ```

## Run a unit test

**Note** Although the tests can be triggered with a Gradle command, that method sets up a fresh `virtualenv` and installs dependencies before each run, which takes minutes. Therefore, it's useful to have a persistent `virtualenv`.

Unit tests have the filename `**_test.py`. Integration tests have the filename `**_it_test.py`.

* To run all tests in a file, use the following command:

```shell
pytest -v  apache_beam/io/textio_test.py
```

* To run all tests in a class, use the following command:

```shell
pytest -v  apache_beam/io/textio_test.py::TextSourceTest
```

* To run a specific test, use the following command:
```shell
pytest -v  apache_beam/io/textio_test.py::TextSourceTest::test_progress
```

## Run an integration test

To run an integration test on the Direct Runner, use the following command:

```shell
python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDirectRunner’
```

If you are preparing a PR, add tests paths [here](https://github.com/apache/beam/blob/2012107a0fa2bb3fedf1b5aedcb49445534b2dad/sdks/python/test-suites/direct/common.gradle#L44) for test-suites to run in PostCommit Python.

To run an integration test on the Dataflow Runner, follow these steps:
  1. To build the SDK tarball, use the following command:
  ```
  cd sdks/python
  pip install build && python -m build –sdist
  ```
  The tarball file is generated in the `sdks/python/sdist/` directory.

  2. Use the `--test-pipeline-options` parameter to specify the tarball file. Use the location `--sdk_location=dist/apache-beam-2.53.0.dev0.tar.gz`. The following example shows the complete command:
  ```shell
  python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_location=dist/apache-beam-2.35.0.dev0.tar.gz
                           --region=us-central1’
  ```

  3. If you are preparing a PR, to include integration tests in the Python PostCommit test suite's Dataflow task, use the marker `@pytest.mark.it_postcommit`.


### Build containers for modified SDK code

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

### Specify additional test dependencies

This section explains how to specify additional test dependencies.

Option 1: Use the `--requirements_file` options. The following example demonstrates how to use this option:

```shell
python -m pytest  -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_location=us.gcr.io/apache-beam-testing/beam-sdk/beam:dev
                           --region=us-central1
                           –requirements_file=requirements.txt’
```

Option 2: If you're using the Dataflow runner, use [custom containers](https://cloud.google.com/dataflow/docs/guides/using-custom-containers).

It is convenient to use the [official Beam SDK container image](https://gcr.io/apache-beam-testing/beam-sdk) as a base and then apply your changes.

## Run your pipeline with modified beam code

To run your pipeline with modified beam code, follow these steps:

1. Build the Beam SDK tarball as described previously (under `sdks/python`, run `python -m build –sdist`).

2. Install the Beam SDK in your Python virtual environment with the necessary extensions, for example `pip install /path/to/apache-beam.tar.gz[gcp]`.

3. Initiate your Python script. To run your pipeline, use a command similar to the following example:
  ```shell
  python my_pipeline.py --runner=DataflowRunner --sdk_location=/path/to/apache-beam.tar.gz --project=my_project --region=us-central1 --temp_location=gs://my-bucket/temp ...
  ```

Tips for using the Dataflow runner:

* The Python worker installs the Apache Beam SDK before processing work items. Therefore, you don't usually need to provide a custom worker container. If your Google Cloud VM doesn't have internet access and transient dependencies are changed from the officially released container images, you do need to provide a custom worker container. In this case, see the section "Build containers for modified SDK code."

* Installing the Beam Python SDK from source can be slow (3.5 minutes for  a`n1-standard-1` machine). As an alternative, if the host machine uses amd64 architecture, you can build a wheel instead of a tarball by using a command similar to `./gradle :sdks:python:bdistPy311linux` (for Python 3.11). Pass the built wheel using the `--sdk_location` option. That installation completes in seconds.

### Caveat - `save_main_session`

* `NameError` when running `DoFn` on remote runner
* Global imports, functions, and variables in main pipeline module are not serialized by default
* Use `--save_main_session` pipeline option to enable it

<!-- TODO # Go Guide -->

<!-- # Cross-language Guide -->

# Appendix

## Directories of snapshot builds

* https://repository.apache.org/content/groups/snapshots/org/apache/beam/ Java SDK build (nightly)
* https://gcr.io/apache-beam-testing/beam-sdk Beam SDK container build (Java, Python, Go, every 4 hrs)
* https://gcr.io/apache-beam-testing/beam_portability Portable runner (Flink, Spark) job server container (nightly)
* gs://beam-python-nightly-snapshots Python SDK build (nightly)
