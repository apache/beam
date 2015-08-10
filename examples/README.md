# Google Cloud Dataflow SDK for Java Examples

The examples included in this module serve to demonstrate the basic
functionality of Google Cloud Dataflow, and act as starting points for
the development of more complex pipelines.

In addition to `WordCount`, further examples are included. They are organized into "Cookbook"
and "Complete" subpackages. The "cookbook" examples show how to define common data analysis patterns
when you're building a Dataflow pipeline. The "complete" directory contains some end-to-end examples
that tell more complete stories than the patterns in the "cookbook" directory.


## WordCount

A good starting point for new users is our set of Word Count examples in the top-level
[examples directory](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples).
The canonical 'word count' task runs over input text file(s) and computes how many times
each word occurs in the input.  These examples, and an accompanying
[walkthrough](https://cloud.google.com/dataflow/examples/wordcount-example), demonstrate a series of
four successively more detailed word count example pipelines that perform this task.

The
[MinimalWordCount](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/MinimalWordCount.java)
example shows a basic word count [Pipeline](https://cloud.google.com/dataflow/model/pipelines),
and introduces how to [read](https://cloud.google.com/dataflow/model/reading-and-writing-data)
from text files; shows how to
[Count](https://cloud.google.com/dataflow/model/library-transforms) a
[Pcollection](https://cloud.google.com/dataflow/model/pcollection); basic use of a
[ParDo](https://cloud.google.com/dataflow/model/par-do); and how to write data to Google Cloud
Storage as text files.

The
[WordCount](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WordCount.java)
example shows how to execute a Pipeline both locally and using the Dataflow service; how to use
command-line arguments to set pipeline options; and introduces some pipeline design concepts:
creating custom [PTransforms](https://cloud.google.com/dataflow/model/composite-transforms)
(composite transforms); and using [ParDo](https://cloud.google.com/dataflow/model/par-do) with
static
[DoFns](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/DoFn)
defined out-of-line.

The
[DebuggingWordCount](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/DebuggingWordCount.java)
example shows how to
[log to Cloud Logging](https://cloud.google.com/dataflow/pipelines/logging), so that your log
messages can be viewed from the Dataflow Monitoring UI; controlling Dataflow worker log levels;
creating a
[custom aggregator](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Aggregator);
and testing your Pipeline via
[DataflowAssert](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/testing/DataflowAssert).


Then, the
[WindowedWordCount](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WindowedWordCount.java)
example shows how to run over either unbounded or bounded input collections; how to use a
[PubSub](https://cloud.google.com/pubsub) topic as an
[input source](https://cloud.google.com/dataflow/model/reading-and-writing-data#PubsubIO); how
to do [windowing](https://cloud.google.com/dataflow/model/windowing), and use data element
timestamps; and how to write to BigQuery.


## 'Cookbook' examples

The ['Cookbook'](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook)
directory, which shows common and useful patterns, includes the following examples:

 <ul>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/BigQueryTornadoes.java">BigQueryTornadoes</a>
  &mdash; An example that reads the public samples of weather data from Google
  BigQuery, counts the number of tornadoes that occur in each month, and
  writes the results to BigQuery. Demonstrates reading/writing BigQuery,
  counting a <code>PCollection</code>, and user-defined <code>PTransforms</code>.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/CombinePerKeyExamples.java">CombinePerKeyExamples</a>
  &mdash; An example that reads the public &quot;Shakespeare&quot; data, and for
  each word in the dataset that exceeds a given length, generates a string
  containing the list of play names in which that word appears.
  Demonstrates the <code>Combine.perKey</code>
  transform, which lets you combine the values in a key-grouped
  <code>PCollection</code>.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/DatastoreWordCount.java">DatastoreWordCount</a>
  &mdash; An example that shows you how to read from Google Cloud Datastore.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/DeDupExample.java">DeDupExample</a>
  &mdash; An example that uses Shakespeare's plays as plain text files, and
  removes duplicate lines across all the files. Demonstrates the
  <code>RemoveDuplicates</code>, <code>TextIO.Read</code>,
  and <code>TextIO.Write</code> transforms, and how to wire transforms together.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/FilterExamples.java">FilterExamples</a>
  &mdash; An example that shows different approaches to filtering, including
  selection and projection. It also shows how to dynamically set parameters
  by defining and using new pipeline options, and use how to use a value derived
  by a pipeline. Demonstrates the <code>Mean</code> transform,
  <code>Options</code> configuration, and using pipeline-derived data as a side
  input.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/JoinExamples.java">JoinExamples</a>
  &mdash; An example that shows how to join two collections. It uses a
  sample of the <a href="http://goo.gl/OB6oin">GDELT &quot;world event&quot;
  data</a>, joining the event <code>action</code> country code against a table
  that maps country codes to country names. Demonstrates the <code>Join</code>
  operation, and using multiple input sources.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/MaxPerKeyExamples.java">MaxPerKeyExamples</a>
  &mdash; An example that reads the public samples of weather data from BigQuery,
  and finds the maximum temperature (<code>mean_temp</code>) for each month.
  Demonstrates the <code>Max</code> statistical combination transform, and how to
  find the max-per-key group.
  </li>
  </ul>

## 'Complete' examples

The
['Complete'](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete)
directory contains examples that tell more complete end-to-end stories, and are more like the
actual pipelines that you would build than are the 'cookbook' examples.  It includes the
following examples:

<ul>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/AutoComplete.java">AutoComplete</a>
  &mdash; An example that computes the most popular hash tags for every
  prefix, which can be used for auto-completion. Demonstrates how to use the
  same pipeline in both streaming and batch, combiners, and composite
  transforms.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/StreamingWordExtract.java">StreamingWordExtract</a>
  &mdash; A streaming pipeline example that inputs lines of text from a Cloud
  Pub/Sub topic, splits each line into individual words, capitalizes those
  words, and writes the output to a BigQuery table.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/TfIdf.java">TfIdf</a>
  &mdash; An example that computes a basic TF-IDF search table for a directory or
  Cloud Storage prefix. Demonstrates joining data, side inputs, and logging.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/TopWikipediaSessions.java">TopWikipediaSessions</a>
  &mdash; An example that reads Wikipedia edit data from Cloud Storage and
  computes the user with the longest string of edits separated by no more than
  an hour within each month. Demonstrates using Cloud Dataflow
  <code>Windowing</code> to perform time-based aggregations of data.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/TrafficMaxLaneFlow.java">TrafficMaxLaneFlow</a>
  &mdash; A streaming Cloud Dataflow example using BigQuery output in the
  <code>traffic sensor</code> domain. Demonstrates the Cloud Dataflow streaming
  runner, sliding windows, Cloud Pub/Sub topic ingestion, the use of the
  <code>AvroCoder</code> to encode a custom class, and custom
  <code>Combine</code> transforms.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/TrafficRoutes.java">TrafficRoutes</a>
  &mdash; A streaming Cloud Dataflow example using BigQuery output in the
  <code>traffic sensor</code> domain. Demonstrates the Cloud Dataflow streaming
  runner, <code>GroupByKey</code>, keyed state, sliding windows, and Cloud
  Pub/Sub topic ingestion.
  </li>
  </ul>

## Running the Examples

After building and installing the `SDK` and `Examples` modules, as explained in this
[README](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/README.md),
you can execute the `WordCount` and other example pipelines using the
`DirectPipelineRunner` on your local machine:

    mvn compile exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--inputFile=<INPUT FILE PATTERN> --output=<OUTPUT FILE>"

You can use the `BlockingDataflowPipelineRunner` to execute the `WordCount` example on
Google Cloud Dataflow Service using managed resources in the Google Cloud Platform.
Start by following the general Cloud Dataflow
[Getting Started](https://cloud.google.com/dataflow/getting-started) instructions.
You should have a Google Cloud Platform project that has a Cloud Dataflow API enabled,
a Google Cloud Storage bucket that will serve as a staging location, and installed and
authenticated Google Cloud SDK. In this case, invoke the example as follows:

    mvn compile exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--project=<YOUR CLOUD PLATFORM PROJECT ID> \
    --stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=BlockingDataflowPipelineRunner"

Your Cloud Storage location should be entered in the form of
`gs://bucket/path/to/staging/directory`. The Cloud Platform project refers to
your project id (not the project number or the descriptive name).

Alternatively, you may choose to bundle all dependencies into a single JAR and
execute it outside of the Maven environment. For example, after building and
installing as usual, you can execute the following commands to create the
bundled JAR of the `Examples` module and execute it both locally and in Cloud
Platform:

    mvn package

    java -cp examples/target/google-cloud-dataflow-java-examples-all-bundled-manual_build.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --inputFile=<INPUT FILE PATTERN> --output=<OUTPUT FILE>

    java -cp examples/target/google-cloud-dataflow-java-examples-all-bundled-manual_build.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --project=<YOUR CLOUD PLATFORM PROJECT ID> \
    --stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=BlockingDataflowPipelineRunner

Other examples can be run similarly by replacing the `WordCount` class path with the example classpath, e.g.
`com.google.cloud.dataflow.examples.cookbook.BigQueryTornadoes`,
and adjusting runtime options under the `Dexec.args` parameter, as specified in
the example itself. If you are running the streaming pipeline examples, see the
additional setup instruction, below.

Note that when running Maven on Microsoft Windows platform, backslashes (`\`)
under the `Dexec.args` parameter should be escaped with another backslash. For
example, input file pattern of `c:\*.txt` should be entered as `c:\\*.txt`.

