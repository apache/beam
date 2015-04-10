# Google Cloud Dataflow SDK for Java Examples

The examples included in this module serve to demonstrate the basic
functionality of Google Cloud Dataflow, and act as starting points for
the development of more complex pipelines.

A good starting point for new users is our [`WordCount`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WordCount.java)
example, which runs over the provided input text file(s) and computes how many
times each word occurs in the input.

Besides WordCount, the following examples are included:

 <ul>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/AutoComplete.java">AutoComplete</a>
  &mdash;An example that computes the most popular hash tags for a for every
  prefix, which can be used for auto-completion. Demonstrates how to use the
  same pipeline in both streaming and batch, combiners, and composite
  transforms.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/BigQueryTornadoes.java">BigQueryTornadoes</a>
  &mdash;An example that reads the public samples of weather data from Google
  BigQuery, counts the number of tornadoes that occur in each month, and
  writes the results to BigQuery. Demonstrates reading/writing BigQuery,
  counting a <code>PCollection</code>, and user-defined <code>PTransforms</code>.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/CombinePerKeyExamples.java">CombinePerKeyExamples</a>
  &mdash;An example that reads the public &quot;Shakespeare&quot; data, and for
  each word in the dataset that exceeds a given length, generates a string
  containing the list of play names in which that word appears. Output is saved
  in a Google BigQuery table. Demonstrates the <code>Combine.perKey</code>
  transform, which lets you combine the values in a key-grouped
  <code>PCollection</code>; also  how to use an <code>Aggregator</code> to track
  information in the Google Developers Console.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/DatastoreWordCount.java">DatastoreWordCount</a>
  &mdash;An example that shows you how to use Google Cloud Datastore IO to read
  from Cloud Datastore.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/DeDupExample.java">DeDupExample</a>
  &mdash;An example that uses Shakespeare's plays as plain text files, and
  removes duplicate lines across all the files. Demonstrates the
  <code>RemoveDuplicates</code>, <code>TextIO.Read</code>,
  <code>RemoveDuplicates</code>, and <code>TextIO.Write</code> transforms, and
  how to wire transforms together.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/FilterExamples.java">FilterExamples</a>
  &mdash;An example that shows different approaches to filtering, including
  selection and projection. It also shows how to dynamically set parameters
  by defining and using new pipeline options, and use how to use a value derived
  by a pipeline. Demonstrates the <code>Mean</code> transform,
  <code>Options</code> configuration, and using pipeline-derived data as a side
  input.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/JoinExamples.java">JoinExamples</a>
  &mdash;An example that shows how to do a join on two collections. It uses a
  sample of the <a href="http://goo.gl/OB6oin">GDELT &quot;world event&quot;
  data</a>, joining the event <code>action</code> country code against a table
  that maps country codes to country names. Demonstrated the <code>Join</code>
  operation, and using multiple input sources.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/MaxPerKeyExamples.java">MaxPerKeyExamples</a>&mdash;An example that reads the public samples of weather data from BigQuery,
  and finds the maximum temperature (<code>mean_temp</code>) for each month.
  Demonstates the <code>Max</code> statistical combination transform, and how to
  find the max-per-key group.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/PubsubFileInjector.java">PubsubFileInjector</a>&mdash;A batch Cloud Dataflow pipeline for injecting a set of Cloud Storage
  files into a Google Cloud Pub/Sub topic, line by line. This example can be
  useful for testing streaming pipelines.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/StreamingWordExtract.java">StreamingWordExtract</a>&mdash;An streaming pipeline example that inputs lines of text from a Cloud
  Pub/Sub topic, splits each line into individual words, capitalizes those
  words, and writes the output to a BigQuery table.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/TfIdf.java">TfIdf</a>
  &mdash;An example that computes a basic TF-IDF search table for a directory or
  Cloud Storage prefix. Demonstrates joining data, side inputs, and logging.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/TopWikipediaSessions.java">TopWikipediaSessions</a>
  &mdash;An example that reads Wikipedia edit data from Cloud Storage and
  computes the user with the longest string of edits separated by no more than
  an hour within each month. Demonstrates using Cloud Dataflow
  <code>Windowing</code> to perform time-based aggregations of data.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/TrafficMaxLaneFlow.java">TrafficMaxLaneFlow</a>
  &mdash;A streaming Cloud Dataflow example using BigQuery output in the
  <code>traffic sensor</code> domain. Demonstrates the Cloud Dataflow streaming
  runner, sliding windows, Cloud Pub/Sub topic ingestion, the use of the
  <code>AvroCoder</code> to encode a custom class, and custom
  <code>Combine</code> transforms.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/TrafficRoutes.java">TrafficRoutes</a>
  &mdash;A streaming Cloud Dataflow example using BigQuery output in the
  <code>traffic sensor</code> domain. Demonstrates the Cloud Dataflow streaming
  runner, <code>GroupByKey</code>, keyed state, sliding windows, and Cloud
  Pub/Sub topic ingestion.
  </li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WindowingWordCount.java">WindowingWordCount</a>
  &mdash;An example that applies windowing to &quot;Shakespeare&quot; data in a
  wordcount pipeline.
  </li>
  </ul>

## How to Run the Examples

After building and installing the Cloud Dataflow `SDK` and `Examples` modules as
explained in this [README](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/README.md),
you can execute the `WordCount` and other example pipelines using the
`DirectPipelineRunner` on your local machine:

    mvn exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--input=<INPUT FILE PATTERN> --output=<OUTPUT FILE>"

If you have been whitelisted for Alpha access to the Cloud Dataflow Service and
followed the [developer setup](https://cloud.google.com/dataflow/java-sdk/getting-started#DeveloperSetup)
steps, you can use the `BlockingDataflowPipelineRunner` to execute the
`WordCount` example in the Google Cloud Platform. In this case, you specify your
project name, pipeline runner, and the staging location in
[Google Cloud Storage](https://cloud.google.com/storage/), as follows:

    mvn exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--project=<YOUR CLOUD PLATFORM PROJECT NAME> \
    --stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=BlockingDataflowPipelineRunner"

Your Cloud Storage location should be entered in the form of
`gs://bucket/path/to/staging/directory`. The Cloud Platform project refers to
its name (not number), which has been whitelisted for Cloud Dataflow. Refer to
[Google Cloud Platform](https://cloud.google.com/) for general instructions on
getting started with Cloud Platform.

Alternatively, you may choose to bundle all dependencies into a single JAR and
execute it outside of the Maven environment. For example, after building and
installing as usual, you can execute the following commands to create the
bundled JAR of the `Examples` module and execute it both locally and in Cloud
Platform:

    mvn bundle:bundle -pl examples

    java -cp examples/target/google-cloud-dataflow-java-examples-all-bundled-manual_build.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --input=<INPUT FILE PATTERN> --output=<OUTPUT FILE>

    java -cp examples/target/google-cloud-dataflow-java-examples-all-bundled-manual_build.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --project=<YOUR CLOUD PLATFORM PROJECT NAME> \
    --stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=BlockingDataflowPipelineRunner

Other examples can be run similarly by replacing the `WordCount` class name with
`BigQueryTornadoes`, `DatastoreWordCount`, `TfIdf`, `TopWikipediaSessions`, etc.
and adjusting runtime options under the `Dexec.args` parameter, as specified in
the example itself. If you are running the streaming pipeline examples, see the
additional setup instruction, below.

Note that when running Maven on Microsoft Windows platform, backslashes (`\`)
under the `Dexec.args` parameter should be escaped with another backslash. For
example, input file pattern of `c:\*.txt` should be entered as `c:\\*.txt`.

<p class="note"><b>Note:</b> We are working on improving the experience around
running some of our streaming examples. Please stay tuned for much easier
instructions in the near future!</p>

### Running the Streaming "Traffic" Streaming Examples###

The `TrafficMaxLaneFlow` and `TrafficRoutes` pipelines, when run in
streaming mode (with the `--streaming=true` option), require the
publication of *traffic sensor* data to a
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs) topic.
By default, they use a separate batch pipeline and a pre-defined Cloud Pub/Sub
topic to publish previously gathered traffic sensor data that is "streamed" into
the pipeline.

The default traffic sensor data `--inputFile` is downloaded from

    curl -O \
    http://storage.googleapis.com/aju-sd-traffic/unzipped/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv

This file contains real traffic sensor data from San Diego freeways. See
<a href="http://storage.googleapis.com/aju-sd-traffic/freeway_detector_config/Freeways-Metadata-2010_01_01/copyright(san%20diego).txt">this file</a>
for copyright information.

**Note:** If you set `--streaming=false`, these traffic pipelines will run in batch mode,
using the timestamps applied to the original dataset to process the data in
a batch. For further information on how these pipelines operate, see
<a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/TrafficMaxLaneFlow.java">TrafficMaxLaneFlow</a>
and <a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/TrafficRoutes.java">TrafficRoutes</a>.
