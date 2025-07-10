Prompt:
How to run Apache Beam pipelines locally?

Response:
You can run Apache Beam pipelines locally on your machine using the Apache Beam Direct Runner. It is designed for testing and development, ensuring that pipelines adhere closely to the Apache Beam model and are robust across different Beam runners. This option is suitable for testing and debugging pipelines, allowing quick iteration on code with immediate result visualization and unit testing using preferred local debugging tools.

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

This method enables specifying options as command-line arguments in the format `--<option>=<value>`. Set the `runner` to `direct` or `DirectRunner`. Default values for other pipeline options are usually sufficient. For additional options, refer to the reference documentation for the `DirectOptions` interface for Python and Java.

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

For additional information and considerations, refer to the Apache Beam documentation on the Direct Runner.
