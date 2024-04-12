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

This guide is for Beam users and developers changing and testing Beam codes.
Specifically,

1. Testing the code changes locally,

2. Build Beam artifacts with modified Beam code and use it for pipelines.

# Repository structure

The Apache Beam GitHub repository (Beam repo) is pretty much a "mono repo",
containing everything of the Beam project, from the SDK itself, to test
infrastructure, dashboards, [Beam website](https://beam.apache.org),
[Beam Playground](https://play.beam.apache.org), etc.

## Gradle quick start

The Beam repo is a single Gradle project (for all components, including python,
go, website, etc). It is useful to get familiar with the concept of Gradle project structure:
https://docs.gradle.org/current/userguide/multi_project_builds.html

### Gradle key concept

* project: folder with build.gradle
* task: defined in build.gradle
* plugin: run in project build.gradle, pre-defined tasks and hierarchies

For example, common tasks for a java (sub)project:

- compileJava
- compileTestJava
- test
- integrationTest

To run a Gradle task, the command is `./gradlew -p <project path> <task>` or equivalently, `./gradlew :project:path:task_name`, e.g.

```
./gradlew -p sdks/java/core compileJava

./gradlew :sdks:java:harness:test
```

### Gradle project configuration: Beam specific

* A **huge** plugin `buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin` manages everything.

Then, for example, in each java (sub)project, `build.gradle` starts with

```groovy

apply plugin: 'org.apache.beam.module'

applyJavaNature( ... )
```

Relevant usage of BeamModulePlugin:
* Manage Java dependencies
* Configuring projects (Java, Python, Go, Proto, Docker, Grpc, Avro, etc)
  * java -> applyJavaNature; python -> applyPythonNature, etc
  * Define common custom tasks for each type of projects
    * test : run Java unit tests
    * spotlessApply : format java code

## Code paths

Example code paths relevant for SDK development:

* `sdks/java` Java SDK
  * `sdks/java/core` Java core
  * `sdks/java/harness` SDK harness (entrypoint of SDK container)

* `runners` runner supports, written in Java. For example,
  * `runners/direct-java` Java direct runner
  * `runners/flink-java` Java Flink runner
  * `runners/google-cloud-dataflow-java` Dataflow runner (job submission, translation, etc)
    * `runners/google-cloud-dataflow-java/`worker Worker on Dataflow legacy runner

* `sdks/python` contains setup file and scripts to trigger test-suites
  * `sdks/python/apache_beam` actual beam package
    * `sdks/python/apache_beam/runners/worker` SDK worker harness entrypoint, state sampler
    * `sdks/python/apache_beam/io` IO connectors
    * `sdks/python/apache_beam/transforms` most "core" components
    * `sdks/python/apache_beam/ml` Beam ML
    * `sdks/python/apache_beam/runners` runner implementations and wrappers
    * ...

* `sdks/go` Go SDK

* `.github/workflow` GitHub Action workflows (e.g. tests run under PR). Most
  workflows just run a single Gradle command. Checking which command running for
  a test so one can run the same command locally during the development.

## Environment setup

Please refer to [Contributing guide](../CONTRIBUTING.md) for setting up local
development environments first. If intended to use Dataflow, refer to [Google cloud doc](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java) to setup gcloud credentials.

To check if your environment has setup,

In your PATH, it should have

* A Java environment (any supported Java version, Java8 preferably as of 2024).
  * Needed for all development as Beam is a Gradle project (which uses JVM)
  * Recommended: Use [sdkman]((https://sdkman.io/install)) to manage Java versions
* A Python environment (any supported Python version)
  * Needed for Python SDK development
  * Recommended: Use [pyenv](https://github.com/pyenv/pyenv) and
    [virtual environment](https://docs.python.org/3/library/venv.html) to manage Python versions
* A Go environment. Install the latest Go
  * Needed for Go SDK development and SDK container change (for all SDKs), as
  the container entrypoint scripts are written in Go.
* A docker environment.
  * Needed for SDK container change, some cross-language functionality (if run a
    SDK container image), portable runners (using job server), etc

For example
- testing the code change in `sdks/java/io/google-cloud-platform`: need a Java environment
- testing the code change in `sdks/java/harness`: need a Java environment, a Go
  environment and Docker environment (to compile and build Java SDK harness container image)
- testing the code change in `sdks/python/apache_beam`: need a Python environment

# Java Guide

## IDE (IntelliJ) Setup

1. From IntelliJ, open `/beam` (**Important** repository root dir, instead of
  `sdks/java`, etc)

2. Wait for indexing (takes minutes)

It should just work (if prerequisites met) as Gradle is a self-contained build tool

To check the load is successful, find `examples/java/build.gradle`, there should
be a "Run" button besides wordCount task. Click the button, it should compile
and run the wordCount example.

<img width="631" alt="image" src="https://github.com/apache/beam/assets/8010435/f5203e8e-0f9c-4eaa-895b-e16f68a808a2">

**Note** IDE is not required for changing the code and testing. Again, as a
Gradle project, tests can be executed via a Gradle command line, see below.

## Console (shell) setup

Equivalent command line:

```shell
$ cd beam
$ ./gradlew :examples:java:wordCount
```

Upon finishing, one should see the following Gradle build log:

```
...
BUILD SUCCESSFUL in 2m 32s
96 actionable tasks: 9 executed, 87 up-to-date
3:41:06 PM: Execution finished 'wordCount'.
```

and checking the output file:

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

It compiles the beam SDK and the word count pipeline (Hello-world program for
data processing), then run the pipeline on Direct Runner.

## Run a unit test

Now, suppose you have made a code change in Java SDK (e.g. in `sdks/java/io/jdbc`),
and want to run relevant unit tests locally to verify the change.

Unit tests are under `src/test/java` folder of each project with filename `.../**Test.java` for unit tests and, `.../**IT.java` for integration tests. For example,

* Run all unit tests under a project
  ```
  ./gradlew :sdks:java:harness:test
  ```
  Then JUnit report (in html web page) can be found under `<invoked_project>/build/reports/tests/test/index.html`

* Run a specific test
  ```
  ./gradlew :sdks:java:harness:test --tests org.apache.beam.fn.harness.CachesTest
  ./gradlew :sdks:java:harness:test --tests *CachesTest
  ./gradlew :sdks:java:harness:test --tests *CachesTest.testClearableCache
  ```

* Run a specific test with IntelliJ
  <img width="452" alt="image" src="https://github.com/apache/beam/assets/8010435/7ae2a65c-a104-48a2-8bad-ff8c52dd1943">
  Just click the ticks to run a whole test class or a specific test. One can also set breakpoints and debug the test.

* Exception: `sdks:java:core`. Many unit tests are invoked by task  `:runners:direct-java:needsRunnerTest` instead.

  Reason: Java core does not depend on a runner. For unit test that executes a pipeline, it needs direct runner.

* Run an integration test on direct runner

## Run an integration test (*IT.java)

An integration test uses `TestPipeline` where its option is set by `TestPipelineOptions` ([code path](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/TestPipeline.java))

Differences from normal Pipeline:
* Block on run (on TestDataflowRunner) by default
* Has a default timeout (15 min)
* Pipeline options set in System property `beamTestPipelineOptions`

The last point is important: the test is configured by setting `-DbeamTestPipelineOptions=[...]`, including which runner it will be run.

Example command line, presenting a minimum option needed for running on Dataflow runner:
```
-DbeamTestPipelineOptions='["--runner=TestDataflowRunner","--project=mygcpproject","--region=us-central1","--stagingLocation=gs://mygcsbucket/path"]'
```

### Write an integration test

* Setup a TestPipeline in an integration test
  ```java

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();

  @Test
  public void testSomething() {
    pipeline.apply(...);

    pipeline.run().waitUntilFinish();
  }
  ```

* It is up to the task that runs the test to specify the runner. Common entries:
  * `:sdks:java:io:google-cloud-platform:integrationTest` run GCP IO integration test on direct runner
  * `:runners:google-cloud-dataflow-java:googleCloudPlatformLegacyWorkerIntegrationTest` run integration tests on Dataflow Legacy runner: * `:runners:google-cloud-dataflow-java:googleCloudPlatformRunnerV2IntegrationTest` run integration test on Dataflow runner v2

In general, Refer to the gradle command that the GHA workflow runs to find how to run it locally.

Example invocation:
```
./gradlew :runners:google-cloud-dataflow-java:examplesJavaRunnerV2IntegrationTest \
-PdisableSpotlessCheck=true -PdisableCheckStyle=true -PskipCheckerFramework \
-PgcpProject=<your_gcp_project> -PgcpRegion=us-central1 \
-PgcsTempRoot=gs://<your_gcs_bucket>/tmp
```

## Run your pipeline with modified beam code

Depending on the purpose, it is recommended to start with different branch to apply your code change:

* If making a pull request, or want to test a change with dev branch---start from Beam HEAD ([master](https://github.com/apache/beam/tree/master)).

* If making a patch on released Beam (2.xx.0), start from tag (e.g. [v2.55.0](https://github.com/apache/beam/tree/v2.55.0))

Then,
* Under beam repo: Compile the project involving the code change with (e.g. if modified sdks/java/io/kafka)
  ```
  ./gradlew -Ppublishing -p sdks/java/io/kafka publishToMavenLocal
  ```
  This will publish the artifact with modified code to Maven Local repository (~/.m2/repository) by default, and it **will be picked up when executing user pipeline**.

If your code change is made on dev branches (like on beam master, or a PR), instead on a release tag, this will produce an artifact under version `2.xx.0-SNAPSHOT`. Additional configs need to be done in **your pipeline project**, in order to pick up this dependency:

* For Maven Project
  1. (Recommended) Use wordcount maven-archetype as Template to setup your project (https://beam.apache.org/get-started/quickstart-java/)
  2. Add snapshot repository
    ```xml
    <repository>
      <id>Maven-Snapshot</id>
      <name>maven snapshot repository</name>
      <url>https://repository.apache.org/content/groups/snapshots/</url>
    </repository>
  3. Modify beam.version in pom.xml
    ```xml
    <properties>
    <beam.version>2.XX.0-SNAPSHOT</beam.version>
    ```

* For Gradle Project
  1. in build.gradle, add
    ```groovy
    repositories {
    maven { url "https://repository.apache.org/content/groups/snapshots" }
    }
    ```
  2. set the beam dependency versions to be `2.XX.0-SNAPSHOT`

This config directs the build system to download Beam NIGHTLY builds from Maven Snapshot Repository other than the one built locally (the one you just made change), so you don't need to build all Beam artifacts locally (otherwise, a command running for all projects `./gradlew -Ppublishing  publishToMavenLocal` is needed)

Special cases:

* [Dataflow Legacy runner] If Dataflow Legacy runner worker harness has changed
  1. Compile the dataflowWorkerJar
    ```
    ./gradlew :runners:google-cloud-dataflow-java:worker:shadowJar
    ```
    The jar is located in the build output
  2. Pass the pipelineOption
    ```
    --dataflowWorkerJar=/.../beam-runners-google-cloud-dataflow-java-legacy-worker-2.XX.0-SNAPSHOT.jar
    ```

* [Dataflow runner v2] If `sdks/java/harness` or its dependency (like `sdks/java/core`) are changed:
  1. Build SDK harness container
    ```shell
    ./gradlew :sdks:java:container:java8:docker # java8, java11, java17, etc
  docker tag apache/beam_java8_sdk:2.49.0.dev \
    "us.gcr.io/apache-beam-testing/beam_java11_sdk:2.49.0-custom"  # change to your container registry
  docker push "us.gcr.io/apache-beam-testing/beam_java11_sdk:2.49.0-custom"
    ```
  2. Run a Runner v2 pipeline with the following options:
  ```
  --experiments=use_runner_v2 \
  --sdkContainerImage="us.gcr.io/apache-beam-testing/beam_java11_sdk:2.49.0-custom"
  ```

# Python Guide

Beam Python SDK is distributed as a single wheel (which is much simpler than Java SDK), and the Python development is consequently more straightforward.

## Console (shell) setup

In the following, working directory is set to `sdks/python`.

1. Install python interpreter with pyenv (recommended)
  a. install prerequisites
  b. `curl https://pyenv.run | bash`
  c. `pyenv install 3.X` (a supported Python version, refer to python_version in [project property](https://github.com/apache/beam/blob/master/gradle.properties))
2. Setup and activate virtual environment
  a. `pyenv virtualenv 3.X ENV_NAME`
  b. `pyenv activate ENV_NAME`
3. Install apache_beam package in editable mode
  `pip install -e .[gcp, test]`
4. For development envolving SDK container image,
  a. Install Docker Desktop
  b. Install Go
5. (If you're going to submit PRs) Precommit hook for Python code changes (nobody likes lint failures!!)
  ```shell
  # enable pre-commit
  (env) $ pip install pre-commit
  (env) $ pre-commit install

  # disable pre-commit
  (env) $ pre-commit uninstall
  ```

## Run a unit test

**Note** Although the tests can be triggered via a Gradle command (as what CI does), it setups a fresh virtualenv and installs deependencies before each run, which takes minutes, so it is useful to have a persistent virtualenv ready (See the previous Section).

Unit tests have filename `**_test.py`; integration tests have filename `**_it_test.py`.

* Running all tests in a file

```shell
pytest -v  apache_beam/io/textio_test.py
```

* Running all tests in a class

```shell
pytest -v  apache_beam/io/textio_test.py::TextSourceTest
```

* Run a specific test
```shell
pytest -v  apache_beam/io/textio_test.py::TextSourceTest::test_progress
```

## Run an integration test

* On Direct Runner:

```shell
python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDirectRunner’
```

3. If you are preparing a PR, add tests paths [here](https://github.com/apache/beam/blob/2012107a0fa2bb3fedf1b5aedcb49445534b2dad/sdks/python/test-suites/direct/common.gradle#L44) for test-suites to run it in PostCommit Python.

* On DataFlow Runner
  1. First build the SDK tarball
  ```
  cd sdks/python
  pip install build && python -m build –sdist
  ```
  This will generates a tarball file in `sdks/python/sdist/`.

  2. in `--test-pipeline-options`, specify the tarball file to `--sdk_location=dist/apache-beam-2.53.0.dev0.tar.gz`. Example full invocation command:
  ```shell
  python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_location=dist/apache-beam-2.35.0.dev0.tar.gz
                           --region=us-central1’
  ```

  3. If you are preparing a PR, use marker `@pytest.mark.it_postcommit` to include integration test in Python PostCommit test-suite's Dataflow task


### Building containers for modified SDK code

1. Command:
  ```shell
  ./gradlew :sdks:python:container:py39:docker \
  -Pdocker-repository-root=<gcr.io/location> -Pdocker-tag=<tag>
  ```
2. Push containers
3. Specify location with `--sdk_container_image`

Example invocation command:

```shell
python -m pytest  -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_container_image=us.gcr.io/apache-beam-testing/beam-sdk/beam:dev
                           --region=us-central1’
```

### Specifying additional test dependencies

Option 1: Use the --requirements_file flag, e.g.

```shell
python -m pytest  -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_location=us.gcr.io/apache-beam-testing/beam-sdk/beam:dev
                           --region=us-central1
                           –requirements_file=requirements.txt’
```

Option 2: (for Dataflow runner) Use [custom containers](https://cloud.google.com/dataflow/docs/guides/using-custom-containers)

Custom container can be based on official Beam SDK container image (See Appendix for url)

## Run your pipeline with modified beam code

1. Firstly, build Beam SDK tarball as instructed above (under `sdks/python`, run `python -m build –sdist` )

2. Install the Beam SDK under your Python virtual environment with necessary extensions (e.g. `pip install /path/to/apache-beam.tar.gz[gcp]`)

3. Initiate your Python script running the Pipeline with command like
  ```shell
  python my_pipeline.py --runner=DataflowRunner --sdk_location=/path/to/apache-beam.tar.gz --project=my_project --region=us-central1 --temp_location=gs://my-bucket/temp ...
  ```

Tips for Dataflow runner:

* Python worker will install the provided Apache Beam SDK before processing work items, so usually no need to provide custom worker container unless your GCP VM does not have Internet access and transient dependencies are changed from the officially released container images. In this case, refer to "Building containers for modified SDK code" instruction above

* Installing Beam Python SDK from source is known to be slow (3.5 min for `n1-standard-1` machine). Alternatively, if the host machine is amd64 architecture, one can build a wheel (instead of a tarball) with command with command (e.g. `./gradle :sdks:python:bdistPy311linux` for Python 3.11) and pass the built wheel to `--sdk_location`. The installation should then just take seconds.

### Caveat - `save_main_session`

* `NameError` when running DoFn on remote runner
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
