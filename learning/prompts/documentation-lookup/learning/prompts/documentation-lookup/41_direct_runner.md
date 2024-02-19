Prompt: How to run Apache Beam pipelines locally?

Response:
You can run Apache Beam pipelines locally on your machine using the [Apache Beam Direct Runner](https://beam.apache.org/documentation/runners/direct/). It is designed for testing and development, ensuring that pipelines adhere closely to the Apache Beam model and are robust across different Beam runners. This option is suitable for testing and debugging pipelines, allowing quick iteration on code with immediate result visualization and unit testing using preferred local debugging tools.

The Direct Runner performs additional checks to ensure users do not rely on semantics not guaranteed by the model. These checks include:
* Enforcing immutability of elements.
* Enforcing encodability of elements.
* Ensuring elements are processed in an arbitrary order at all points.
* Ensuring serialization of user functions (`DoFn`, `CombineFn`, etc.).

To execute your Apache Beam pipeline locally using the Direct Runner, follow these steps:

***1. Specify Dependencies (Java Only):***

When using the Apache Beam Java SDK, specify your dependency on the Direct Runner in the `pom.xml` file of your Java project directory:

```java
<dependency>
   <groupId>org.apache.beam</groupId>
   <artifactId>beam-runners-direct-java</artifactId>
   <version>2.54.0</version>
   <scope>runtime</scope>
</dependency>
```

Make sure to include all necessary dependencies to create a self-contained application and compile your Java code into a JAR file.

***2. Configure Pipeline Options:***

While you can configure your pipeline by creating a `PipelineOptions` object and setting fields directly, the Beam SDKs include a command-line parser for setting fields using command-line arguments.

To read options from the command line, construct your `PipelineOptions` object as shown:

Java SDK:

```java
PipelineOptions options =
    PipelineOptionsFactory.fromArgs(args).withValidation().create();
```

Python SDK:

```python
from apache_beam.options.pipeline_options import PipelineOptions
beam_options = PipelineOptions()
```

This method enables specifying options as command-line arguments in the format `--<option>=<value>`. Set the `runner` to `direct` or `DirectRunner`. Default values for other pipeline options are usually sufficient. For additional options, refer to the reference documentation for the `DirectOptions` interface for [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.options.pipeline_options.html#apache_beam.options.pipeline_options.DirectOptions) and [Java](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/runners/direct/DirectOptions.html).

***3. Run the Pipeline:***

Execute your pipeline on the Direct Runner from the command line.

For Python SDK, use the `python -m` syntax to invoke the Python module containing your pipeline code:

```python
python -m your_module_name
```

For Java SDK, run your Java JAR file using the `java -jar` command:

```java
java -jar your-project.jar
```

***Considerations:***

1. The Direct Runner is not intended for production pipelines as it prioritizes correctness over performance.
2. The Direct Runner must fit all user data in memory, unlike other runners that can spill data to disk. Run it with small datasets that can fit within available memory.
3. For unbounded data sources or sinks, set the `streaming` option to `true`. Streaming support for Python Direct Runner is limited. For the list of known issues, refer to [the GitHub issue #24528](https://github.com/apache/beam/issues/24528) in the Apache Beam repository.
4. The ​​Python [`FnApiRunner`](https://beam.apache.org/contribute/runner-guide/#the-fn-api) supports multi-threading and multi-processing modes.
5. You can control the number of threads or subprocesses by setting the `targetParallelism` option for Java or the `direct_num_workers` option for Python.

For testing Apache Beam pipelines with the Direct Runner, refer to:
[Testing Unbounded Pipelines in Apache Beam](https://beam.apache.org/blog/2016/10/20/test-stream.html) discusses using Java classes [`PAssert`](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/testing/PAssert.html) and [`TestStream`](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/testing/TestStream.html).
The [Apache Beam WordCount Walkthrough](https://beam.apache.org/get-started/wordcount-example/#testing-your-pipeline-with-asserts) contains an example of logging and testing a pipeline with [`PAssert`](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/testing/PAssert.html) (Java) and [`assert_that`](https://beam.apache.org/releases/pydoc/current/apache_beam.testing.util.html#apache_beam.testing.util.assert_that) (Python).
