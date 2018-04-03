---
layout: section
title: "Beam WordCount Examples"
permalink: get-started/wordcount-example/
section_menu: section-menu/get-started.html
redirect_from: /use/wordcount-example/
---

# Apache Beam WordCount Examples

* TOC
{:toc}

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>

The WordCount examples demonstrate how to set up a processing pipeline that can
read text, tokenize the text lines into individual words, and perform a
frequency count on each of those words. The Beam SDKs contain a series of these
four successively more detailed WordCount examples that build on each other. The
input text for all the examples is a set of Shakespeare's texts.

Each WordCount example introduces different concepts in the Beam programming
model. Begin by understanding MinimalWordCount, the simplest of the examples.
Once you feel comfortable with the basic principles in building a pipeline,
continue on to learn more concepts in the other examples.

* **MinimalWordCount** demonstrates the basic principles involved in building a
  pipeline.
* **WordCount** introduces some of the more common best practices in creating
  re-usable and maintainable pipelines.
* **DebuggingWordCount** introduces logging and debugging practices.
* **WindowedWordCount** demonstrates how you can use Beam's programming model
  to handle both bounded and unbounded datasets.

## MinimalWordCount example

MinimalWordCount demonstrates a simple pipeline that uses the Direct Runner to
read from a text file, apply transforms to tokenize and count the words, and
write the data to an output text file.

{:.language-java}
This example hard-codes the locations for its input and output files and doesn't
perform any error checking; it is intended to only show you the "bare bones" of
creating a Beam pipeline. This lack of parameterization makes this particular
pipeline less portable across different runners than standard Beam pipelines. In
later examples, we will parameterize the pipeline's input and output sources and
show other best practices.

```java
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.MinimalWordCount
```

```py
python -m apache_beam.examples.wordcount_minimal --input YOUR_INPUT_FILE --output counts
```

{:.language-java}
To view the full code in Java, see
**[MinimalWordCount](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/MinimalWordCount.java).**

{:.language-py}
To view the full code in Python, see
**[wordcount_minimal.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount_minimal.py).**

**Key Concepts:**

* Creating the Pipeline
* Applying transforms to the Pipeline
* Reading input (in this example: reading text files)
* Applying ParDo transforms
* Applying SDK-provided transforms (in this example: Count)
* Writing output (in this example: writing to a text file)
* Running the Pipeline

The following sections explain these concepts in detail, using the relevant code
excerpts from the MinimalWordCount pipeline.

### Creating the pipeline

In this example, the code first creates a `PipelineOptions` object. This object
lets us set various options for our pipeline, such as the pipeline runner that
will execute our pipeline and any runner-specific configuration required by the
chosen runner. In this example we set these options programmatically, but more
often, command-line arguments are used to set `PipelineOptions`.

You can specify a runner for executing your pipeline, such as the
`DataflowRunner` or `SparkRunner`. If you omit specifying a runner, as in this
example, your pipeline executes locally using the `DirectRunner`. In the next
sections, we will specify the pipeline's runner.

```java
 // Create a PipelineOptions object. This object lets us set various execution
 // options for our pipeline, such as the runner you wish to use. This example
 // will run with the DirectRunner by default, based on the class path configured
 // in its dependencies.
 PipelineOptions options = PipelineOptionsFactory.create();
```

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_options
%}```

The next step is to create a `Pipeline` object with the options we've just
constructed. The Pipeline object builds up the graph of transformations to be
executed, associated with that particular pipeline.

```java
Pipeline p = Pipeline.create(options);
```

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_create
%}```

### Applying pipeline transforms

The MinimalWordCount pipeline contains several transforms to read data into the
pipeline, manipulate or otherwise transform the data, and write out the results.
Transforms can consist of an individual operation, or can contain multiple
nested transforms (which is a [composite transform]({{ site.baseurl }}/documentation/programming-guide#composite-transforms)).

Each transform takes some kind of input data and produces some output data. The
input and output data is often represented by the SDK class `PCollection`.
`PCollection` is a special class, provided by the Beam SDK, that you can use to
represent a data set of virtually any size, including unbounded data sets.

![The MinimalWordCount pipeline data flow.](
  {{ "/images/wordcount-pipeline.png" | prepend: site.baseurl }}){: width="800px"}

*Figure 1: The MinimalWordCount pipeline data flow.*

The MinimalWordCount pipeline contains five transforms:

1.  A text file `Read` transform is applied to the `Pipeline` object itself, and
    produces a `PCollection` as output. Each element in the output `PCollection`
    represents one line of text from the input file. This example uses input
    data stored in a publicly accessible Google Cloud Storage bucket ("gs://").

    ```java
    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
    ```

    ```py
    {% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_read
    %}```

2.  This transform splits the lines in `PCollection<String>`, where each element
    is an individual word in Shakespeare's collected texts.
    As an alternative, it would have been possible to use a
    [ParDo]({{ site.baseurl }}/documentation/programming-guide/#pardo)
    transform that invokes a `DoFn` (defined in-line as an anonymous class) on
    each element that tokenizes the text lines into individual words. The input
    for this transform is the `PCollection` of text lines generated by the
    previous `TextIO.Read` transform. The `ParDo` transform outputs a new
    `PCollection`, where each element represents an individual word in the text.

    ```java
        .apply("ExtractWords", FlatMapElements
            .into(TypeDescriptors.strings())
            .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
    ```

    ```py
    # The Flatmap transform is a simplified version of ParDo.
    {% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_pardo
    %}```

3.  The SDK-provided `Count` transform is a generic transform that takes a
    `PCollection` of any type, and returns a `PCollection` of key/value pairs.
    Each key represents a unique element from the input collection, and each
    value represents the number of times that key appeared in the input
    collection.

    In this pipeline, the input for `Count` is the `PCollection` of individual
    words generated by the previous `ParDo`, and the output is a `PCollection`
    of key/value pairs where each key represents a unique word in the text and
    the associated value is the occurrence count for each.

    ```java
    .apply(Count.<String>perElement())
    ```

    ```py
    {% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_count
    %}```

4.  The next transform formats each of the key/value pairs of unique words and
    occurrence counts into a printable string suitable for writing to an output
    file.

    The map transform is a higher-level composite transform that encapsulates a
    simple `ParDo`. For each element in the input `PCollection`, the map
    transform applies a function that produces exactly one output element.

    ```java
    .apply("FormatResults", MapElements
        .into(TypeDescriptors.strings())
        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
    ```

    ```py
    {% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_map
    %}```

5.  A text file write transform. This transform takes the final `PCollection` of
    formatted Strings as input and writes each element to an output text file.
    Each element in the input `PCollection` represents one line of text in the
    resulting output file.

    ```java
    .apply(TextIO.write().to("wordcounts"));
    ```

    ```py
    {% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_write
    %}```

Note that the `Write` transform produces a trivial result value of type `PDone`,
which in this case is ignored.

### Running the pipeline

Run the pipeline by calling the `run` method, which sends your pipeline to be
executed by the pipeline runner that you specified in your `PipelineOptions`.

```java
p.run().waitUntilFinish();
```

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_minimal_run
%}```

Note that the `run` method is asynchronous. For a blocking execution, call the
<span class="language-java">`waitUntilFinish`</span>
<span class="language-py">`wait_until_finish`</span> method on the result object
returned by the call to `run`.

## WordCount example

This WordCount example introduces a few recommended programming practices that
can make your pipeline easier to read, write, and maintain. While not explicitly
required, they can make your pipeline's execution more flexible, aid in testing
your pipeline, and help make your pipeline's code reusable.

This section assumes that you have a good understanding of the basic concepts in
building a pipeline. If you feel that you aren't at that point yet, read the
above section, [MinimalWordCount](#minimalwordcount-example).

**To run this example in Java:**

{:.runner-direct}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner
```

{:.runner-apex}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=counts --runner=ApexRunner" -Papex-runner
```

{:.runner-flink-local}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=FlinkRunner --inputFile=pom.xml --output=counts" -Pflink-runner
```

{:.runner-flink-cluster}
```
$ mvn package exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=target/word-count-beam-bundled-0.1.jar \
                  --inputFile=/path/to/quickstart/pom.xml --output=/tmp/counts" -Pflink-runner

You can monitor the running job by visiting the Flink dashboard at http://<flink master>:8081
```

{:.runner-spark}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=SparkRunner --inputFile=pom.xml --output=counts" -Pspark-runner
```

{:.runner-dataflow}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--runner=DataflowRunner --gcpTempLocation=gs://YOUR_GCS_BUCKET/tmp \
                  --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://YOUR_GCS_BUCKET/counts" \
     -Pdataflow-runner
```

To view the full code in Java, see
**[WordCount](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java).**

**To run this example in Python:**

{:.runner-direct}
```
python -m apache_beam.examples.wordcount --input YOUR_INPUT_FILE --output counts
```

{:.runner-apex}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-local}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-cluster}
```
This runner is not yet available for the Python SDK.
```

{:.runner-spark}
```
This runner is not yet available for the Python SDK.
```

{:.runner-dataflow}
```
# As part of the initial setup, install Google Cloud Platform specific extra components.
pip install apache-beam[gcp]
python -m apache_beam.examples.wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
                                         --output gs://YOUR_GCS_BUCKET/counts \
                                         --runner DataflowRunner \
                                         --project YOUR_GCP_PROJECT \
                                         --temp_location gs://YOUR_GCS_BUCKET/tmp/
```

To view the full code in Python, see
**[wordcount.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py).**

**New Concepts:**

* Applying `ParDo` with an explicit `DoFn`
* Creating Composite Transforms
* Using Parameterizable `PipelineOptions`

The following sections explain these key concepts in detail, and break down the
pipeline code into smaller sections.

### Specifying explicit DoFns

When using `ParDo` transforms, you need to specify the processing operation that
gets applied to each element in the input `PCollection`. This processing
operation is a subclass of the SDK class `DoFn`. You can create the `DoFn`
subclasses for each `ParDo` inline, as an anonymous inner class instance, as is
done in the previous example (MinimalWordCount). However, it's often a good
idea to define the `DoFn` at the global level, which makes it easier to unit
test and can make the `ParDo` code more readable.

```java
// In this example, ExtractWordsFn is a DoFn that is defined as a static class:

static class ExtractWordsFn extends DoFn<String, String> {
    ...

    @ProcessElement
    public void processElement(ProcessContext c) {
        ...
    }
}
```

```py
# In this example, the DoFns are defined as classes:

{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_wordcount_dofn
%}```

### Creating composite transforms

If you have a processing operation that consists of multiple transforms or
`ParDo` steps, you can create it as a subclass of `PTransform`. Creating a
`PTransform` subclass allows you to encapsulate complex transforms, can make
your pipeline's structure more clear and modular, and makes unit testing easier.

In this example, two transforms are encapsulated as the `PTransform` subclass
`CountWords`. `CountWords` contains the `ParDo` that runs `ExtractWordsFn` and
the SDK-provided `Count` transform.

When `CountWords` is defined, we specify its ultimate input and output; the
input is the `PCollection<String>` for the extraction operation, and the output
is the `PCollection<KV<String, Long>>` produced by the count operation.

```java
public static class CountWords extends PTransform<PCollection<String>,
    PCollection<KV<String, Long>>> {
  @Override
  public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

    // Convert lines of text into individual words.
    PCollection<String> words = lines.apply(
        ParDo.of(new ExtractWordsFn()));

    // Count the number of times each word occurs.
    PCollection<KV<String, Long>> wordCounts =
        words.apply(Count.<String>perElement());

    return wordCounts;
  }
}

public static void main(String[] args) throws IOException {
  Pipeline p = ...

  p.apply(...)
   .apply(new CountWords())
   ...
}
```

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_wordcount_composite
%}```

### Using parameterizable PipelineOptions

You can hard-code various execution options when you run your pipeline. However,
the more common way is to define your own configuration options via command-line
argument parsing. Defining your configuration options via the command-line makes
the code more easily portable across different runners.

Add arguments to be processed by the command-line parser, and specify default
values for them. You can then access the options values in your pipeline code.

```java
public static interface WordCountOptions extends PipelineOptions {
  @Description("Path of the file to read from")
  @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
  String getInputFile();
  void setInputFile(String value);
  ...
}

public static void main(String[] args) {
  WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
  Pipeline p = Pipeline.create(options);
  ...
}
```

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:examples_wordcount_wordcount_options
%}```

## DebuggingWordCount example

The DebuggingWordCount example demonstrates some best practices for
instrumenting your pipeline code.

**To run this example in Java:**

{:.runner-direct}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.DebuggingWordCount \
     -Dexec.args="--output=counts" -Pdirect-runner
```

{:.runner-apex}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.DebuggingWordCount \
     -Dexec.args="--output=counts --runner=ApexRunner" -Papex-runner
```

{:.runner-flink-local}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.DebuggingWordCount \
     -Dexec.args="--runner=FlinkRunner --output=counts" -Pflink-runner
```

{:.runner-flink-cluster}
```
$ mvn package exec:java -Dexec.mainClass=org.apache.beam.examples.DebuggingWordCount \
     -Dexec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=target/word-count-beam-bundled-0.1.jar \
                  --output=/tmp/counts" -Pflink-runner

You can monitor the running job by visiting the Flink dashboard at http://<flink master>:8081
```

{:.runner-spark}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.DebuggingWordCount \
     -Dexec.args="--runner=SparkRunner --output=counts" -Pspark-runner
```

{:.runner-dataflow}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.DebuggingWordCount \
   -Dexec.args="--runner=DataflowRunner --gcpTempLocation=gs://<your-gcs-bucket>/tmp \
                --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://<your-gcs-bucket>/counts" \
     -Pdataflow-runner
```

To view the full code in Java, see
[DebuggingWordCount](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/DebuggingWordCount.java).

**To run this example in Python:**

{:.runner-direct}
```
python -m apache_beam.examples.wordcount_debugging --input YOUR_INPUT_FILE --output counts
```

{:.runner-apex}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-local}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-cluster}
```
This runner is not yet available for the Python SDK.
```

{:.runner-spark}
```
This runner is not yet available for the Python SDK.
```

{:.runner-dataflow}
```
# As part of the initial setup, install Google Cloud Platform specific extra components.
pip install apache-beam[gcp]
python -m apache_beam.examples.wordcount_debugging --input gs://dataflow-samples/shakespeare/kinglear.txt \
                                         --output gs://YOUR_GCS_BUCKET/counts \
                                         --runner DataflowRunner \
                                         --project YOUR_GCP_PROJECT \
                                         --temp_location gs://YOUR_GCS_BUCKET/tmp/
```

To view the full code in Python, see
**[wordcount_debugging.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount_debugging.py).**

**New Concepts:**

* Logging
* Testing your Pipeline via `PAssert`

The following sections explain these key concepts in detail, and break down the
pipeline code into smaller sections.

### Logging

Each runner may choose to handle logs in its own way.

```java
// This example uses .trace and .debug:

public class DebuggingWordCount {

  public static class FilterTextFn extends DoFn<KV<String, Long>, KV<String, Long>> {
    ...

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (...) {
        ...
        LOG.debug("Matched: " + c.element().getKey());
      } else {
        ...
        LOG.trace("Did not match: " + c.element().getKey());
      }
    }
  }
}
```

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py tag:example_wordcount_debugging_logging
%}```


#### Direct Runner

When executing your pipeline with the `DirectRunner`, you can print log
messages directly to your local console. <span class="language-java">If you use
the Beam SDK for Java, you must add `Slf4j` to your class path.</span>

#### Cloud Dataflow Runner

When executing your pipeline with the `DataflowRunner`, you can use Stackdriver
Logging. Stackdriver Logging aggregates the logs from all of your Cloud Dataflow
job's workers to a single location in the Google Cloud Platform Console. You can
use Stackdriver Logging to search and access the logs from all of the workers
that Cloud Dataflow has spun up to complete your job. Logging statements in your
pipeline's `DoFn` instances will appear in Stackdriver Logging as your pipeline
runs.

You can also control the worker log levels. Cloud Dataflow workers that execute
user code are configured to log to Stackdriver Logging by default at "INFO" log
level and higher. You can override log levels for specific logging namespaces by
specifying: `--workerLogLevelOverrides={"Name1":"Level1","Name2":"Level2",...}`.
For example, by specifying `--workerLogLevelOverrides={"org.apache.beam.examples":"DEBUG"}`
when executing a pipeline using the Cloud Dataflow service, Stackdriver Logging
will contain only "DEBUG" or higher level logs for the package in addition to
the default "INFO" or higher level logs.

The default Cloud Dataflow worker logging configuration can be overridden by
specifying `--defaultWorkerLogLevel=<one of TRACE, DEBUG, INFO, WARN, ERROR>`.
For example, by specifying `--defaultWorkerLogLevel=DEBUG` when executing a
pipeline with the Cloud Dataflow service, Cloud Logging will contain all "DEBUG"
or higher level logs. Note that changing the default worker log level to TRACE
or DEBUG significantly increases the amount of logs output.

#### Apache Spark Runner

> **Note:** This section is yet to be added. There is an open issue for this
> ([BEAM-792](https://issues.apache.org/jira/browse/BEAM-792)).

#### Apache Flink Runner

> **Note:** This section is yet to be added. There is an open issue for this
> ([BEAM-791](https://issues.apache.org/jira/browse/BEAM-791)).

#### Apache Apex Runner

> **Note:** This section is yet to be added. There is an open issue for this
> ([BEAM-2285](https://issues.apache.org/jira/browse/BEAM-2285)).

### Testing your pipeline with asserts

<span class="language-java">`PAssert`</span><span class="language-py">`assert_that`</span>
is a set of convenient PTransforms in the style of Hamcrest's collection
matchers that can be used when writing pipeline level tests to validate the
contents of PCollections. Asserts are best used in unit tests with small data
sets.

{:.language-java}
The following example verifies that the set of filtered words matches our
expected counts. The assert does not produce any output, and the pipeline only
succeeds if all of the expectations are met.

{:.language-py}
The following example verifies that two collections contain the same values. The
assert does not produce any output, and the pipeline only succeeds if all of the
expectations are met.

```java
public static void main(String[] args) {
  ...
  List<KV<String, Long>> expectedResults = Arrays.asList(
        KV.of("Flourish", 3L),
        KV.of("stomach", 1L));
  PAssert.that(filteredWords).containsInAnyOrder(expectedResults);
  ...
}
```

```py
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

with TestPipeline() as p:
  assert_that(p | Create([1, 2, 3]), equal_to([1, 2, 3]))
```

{:.language-java}
See [DebuggingWordCountTest](https://github.com/apache/beam/blob/master/examples/java/src/test/java/org/apache/beam/examples/DebuggingWordCountTest.java)
for an example unit test.


## WindowedWordCount example

The WindowedWordCount example counts words in text just as the previous
examples did, but introduces several advanced concepts.

**New Concepts:**

* Unbounded and bounded pipeline input modes
* Adding timestamps to data
* Windowing
* Reusing PTransforms over windowed PCollections

The following sections explain these key concepts in detail, and break down the
pipeline code into smaller sections.

**To run this example in Java:**

{:.runner-direct}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WindowedWordCount \
     -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner
```

{:.runner-apex}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WindowedWordCount \
     -Dexec.args="--inputFile=pom.xml --output=counts --runner=ApexRunner" -Papex-runner
```

{:.runner-flink-local}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WindowedWordCount \
     -Dexec.args="--runner=FlinkRunner --inputFile=pom.xml --output=counts" -Pflink-runner
```

{:.runner-flink-cluster}
```
$ mvn package exec:java -Dexec.mainClass=org.apache.beam.examples.WindowedWordCount \
     -Dexec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=target/word-count-beam-bundled-0.1.jar \
                  --inputFile=/path/to/quickstart/pom.xml --output=/tmp/counts" -Pflink-runner

You can monitor the running job by visiting the Flink dashboard at http://<flink master>:8081
```

{:.runner-spark}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WindowedWordCount \
     -Dexec.args="--runner=SparkRunner --inputFile=pom.xml --output=counts" -Pspark-runner
```

{:.runner-dataflow}
```
$ mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WindowedWordCount \
   -Dexec.args="--runner=DataflowRunner --gcpTempLocation=gs://YOUR_GCS_BUCKET/tmp \
                --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://YOUR_GCS_BUCKET/counts" \
     -Pdataflow-runner
```

To view the full code in Java, see
**[WindowedWordCount](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WindowedWordCount.java).**

**To run this example in Python:**

This pipeline writes its results to a BigQuery table `--output_table`
parameter. using the format `PROJECT:DATASET.TABLE` or
`DATASET.TABLE`.

{:.runner-direct}
```
python -m apache_beam.examples.windowed_wordcount --input YOUR_INPUT_FILE --output_table PROJECT:DATASET.TABLE
```

{:.runner-apex}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-local}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-cluster}
```
This runner is not yet available for the Python SDK.
```

{:.runner-spark}
```
This runner is not yet available for the Python SDK.
```

{:.runner-dataflow}
```
# As part of the initial setup, install Google Cloud Platform specific extra components.
pip install apache-beam[gcp]
python -m apache_beam.examples.windowed_wordcount --input YOUR_INPUT_FILE \
                                         --output_table PROJECT:DATASET.TABLE \
                                         --runner DataflowRunner \
                                         --project YOUR_GCP_PROJECT \
                                         --temp_location gs://YOUR_GCS_BUCKET/tmp/
```

To view the full code in Python, see
**[windowed_wordcount.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/windowed_wordcount.py).**

### Unbounded and bounded pipeline input modes

Beam allows you to create a single pipeline that can handle both bounded and
unbounded types of input. If your input has a fixed number of elements, it's
considered a 'bounded' data set. If your input is continuously updating, then
it's considered 'unbounded' and you must use a runner that supports streaming.

If your pipeline's input is bounded, then all downstream PCollections will also be
bounded. Similarly, if the input is unbounded, then all downstream PCollections
of the pipeline will be unbounded, though separate branches may be independently
bounded.

Recall that the input for this example is a set of Shakespeare's texts, which is
a finite set of data. Therefore, this example reads bounded data from a text
file:

```java
public static void main(String[] args) throws IOException {
    Options options = ...
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> input = pipeline
      .apply(TextIO.read().from(options.getInputFile()))

```

```py
# This feature is not yet available in the Beam SDK for Python.
```

### Adding timestamps to data

Each element in a `PCollection` has an associated [timestamp]({{ site.baseurl }}/documentation/programming-guide#element-timestamps).
The timestamp for each element is initially assigned by the source that creates
the `PCollection`. Some sources that create unbounded PCollections can assign
each new element a timestamp that corresponds to when the element was read or
added. You can manually assign or adjust timestamps with a `DoFn`; however, you
can only move timestamps forward in time.

In this example the input is bounded. For the purpose of the example, the `DoFn`
method named `AddTimestampsFn` (invoked by `ParDo`) will set a timestamp for
each element in the `PCollection`.

```java
.apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));
```

```py
# This feature is not yet available in the Beam SDK for Python.
```

Below is the code for `AddTimestampFn`, a `DoFn` invoked by `ParDo`, that sets
the data element of the timestamp given the element itself. For example, if the
elements were log lines, this `ParDo` could parse the time out of the log string
and set it as the element's timestamp. There are no timestamps inherent in the
works of Shakespeare, so in this case we've made up random timestamps just to
illustrate the concept. Each line of the input text will get a random associated
timestamp sometime in a 2-hour period.

```java
static class AddTimestampFn extends DoFn<String, String> {
  private final Instant minTimestamp;
  private final Instant maxTimestamp;

  AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
    this.minTimestamp = minTimestamp;
    this.maxTimestamp = maxTimestamp;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Instant randomTimestamp =
      new Instant(
          ThreadLocalRandom.current()
          .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));

    /**
     * Concept #2: Set the data element with that timestamp.
     */
    c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
  }
}
```

```py
# This feature is not yet available in the Beam SDK for Python.
```

### Windowing

Beam uses a concept called **Windowing** to subdivide a `PCollection` into
bounded sets of elements. PTransforms that aggregate multiple elements process
each `PCollection` as a succession of multiple, finite windows, even though the
entire collection itself may be of infinite size (unbounded).

The WindowedWordCount example applies fixed-time windowing, wherein each
window represents a fixed time interval. The fixed window size for this example
defaults to 1 minute (you can change this with a command-line option).

```java
PCollection<String> windowedWords = input
  .apply(Window.<String>into(
    FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));
```

```py
# This feature is not yet available in the Beam SDK for Python.
```

### Reusing PTransforms over windowed PCollections

You can reuse existing PTransforms that were created for manipulating simple
PCollections over windowed PCollections as well.

```java
PCollection<KV<String, Long>> wordCounts = windowedWords.apply(new WordCount.CountWords());
```

```py
# This feature is not yet available in the Beam SDK for Python.
```

## StreamingWordCount example

The StreamingWordCount example is a streaming pipeline that reads Pub/Sub
messages from a Pub/Sub subscription or topic, and performs a frequency count on
the words in each message. Similar to WindowedWordCount, this example applies
fixed-time windowing, wherein each window represents a fixed time interval. The
fixed window size for this example is 15 seconds. The pipeline outputs the
frequency count of the words seen in each 15 second window.

**New Concepts:**

* Reading an unbounded data set
* Writing unbounded results

**To run this example in Java:**

> **Note:** StreamingWordCount is not yet available for the Java SDK.

**To run this example in Python:**

{:.runner-direct}
```
python -m apache_beam.examples.streaming_wordcount \
  --input_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_INPUT_TOPIC" \
  --output_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_OUTPUT_TOPIC" \
  --streaming
```

{:.runner-apex}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-local}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-cluster}
```
This runner is not yet available for the Python SDK.
```

{:.runner-spark}
```
This runner is not yet available for the Python SDK.
```

{:.runner-dataflow}
```
# As part of the initial setup, install Google Cloud Platform specific extra components.
pip install apache-beam[gcp]
python -m apache_beam.examples.streaming_wordcount \
  --runner DataflowRunner \
  --project YOUR_GCP_PROJECT \
  --temp_location gs://YOUR_GCS_BUCKET/tmp/ \
  --input_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_INPUT_TOPIC" \
  --output_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_OUTPUT_TOPIC" \
  --streaming
```

To view the full code in Python, see
**[streaming_wordcount.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/streaming_wordcount.py).**


### Reading an unbounded data set

This example uses an unbounded data set as input. The code reads Pub/Sub
messages from a Pub/Sub subscription or topic using
[`beam.io.ReadStringsFromPubSub`]({{ site.baseurl }}/documentation/sdks/pydoc/{{ site.release_latest }}/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadStringsFromPubSub).

```java
  // This example is not currently available for the Beam SDK for Java.
```
```py
  # Read from Pub/Sub into a PCollection.
  if known_args.input_subscription:
    lines = p | beam.io.ReadStringsFromPubSub(
        subscription=known_args.input_subscription)
  else:
    lines = p | beam.io.ReadStringsFromPubSub(topic=known_args.input_topic)
```
### Writing unbounded results

When the input is unbounded, the same is true of the output `PCollection`. As
such, you must make sure to choose an appropriate I/O for the results. Some I/Os
support only bounded output, while others support both bounded and unbounded
outputs.

This example uses an unbounded `PCollection` and streams the results to
Google Pub/Sub. The code formats the results and writes them to a Pub/Sub topic
using [`beam.io.WriteStringsToPubSub`]({{ site.baseurl }}/documentation/sdks/pydoc/{{ site.release_latest }}/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.WriteStringsToPubSub).

```java
  // This example is not currently available for the Beam SDK for Java.
```
```py
  # Write to Pub/Sub
  output | beam.io.WriteStringsToPubSub(known_args.output_topic)
```

