# Google Cloud Dataflow SDK for Java Examples

The examples included in this module serve to demonstrate the basic
functionality of Google Cloud Dataflow, and act as starting points for
the development of more complex pipelines.

A good starting point for new users is our [`WordCount`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WordCount.java)
example, which runs over the provided input text file(s) and computes how many
times each word occurs in the input.

Besides `WordCount`, the following examples are included:

 <ul>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/AutoComplete.java">AutoComplete</a>
  &mdash; An example that computes the most popular hash tags for every
  prefix, which can be used for auto-completion. Demonstrates how to use the
  same pipeline in both streaming and batch, combiners, and composite
  transforms.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/BigQueryTornadoes.java">BigQueryTornadoes</a>
  &mdash; An example that reads the public samples of weather data from Google
  BigQuery, counts the number of tornadoes that occur in each month, and
  writes the results to BigQuery. Demonstrates reading/writing BigQuery,
  counting a <code>PCollection</code>, and user-defined <code>PTransforms</code>.</li>
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/CombinePerKeyExamples.java">CombinePerKeyExamples</a>
  &mdash; An example that reads the public &quot;Shakespeare&quot; data, and for
  each word in the dataset that exceeds a given length, generates a string
  containing the list of play names in which that word appears. Output is saved
  in a Google BigQuery table. Demonstrates the <code>Combine.perKey</code>
  transform, which lets you combine the values in a key-grouped
  <code>PCollection</code>; also how to use an <code>Aggregator</code> to track
  information in the Google Developers Console.
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
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/PubsubFileInjector.java">PubsubFileInjector</a>
  &mdash; A batch Cloud Dataflow pipeline for injecting a set of Cloud Storage
  files into a Google Cloud Pub/Sub topic, line by line. This example can be
  useful for testing streaming pipelines.
  </li>
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
  <li><a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WindowedWordCount.java">WindowedWordCount</a>
  &mdash; An example that applies windowing to &quot;Shakespeare&quot; data in a
  `WordCount` pipeline.
  </li>
  </ul>

## Running the Examples

After building and installing the `SDK` and `Examples` modules, as explained in this
[README](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/README.md),
you can execute the `WordCount` and other example pipelines using the
`DirectPipelineRunner` on your local machine:

    mvn exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--inputFile=<INPUT FILE PATTERN> --output=<OUTPUT FILE>"

You can use the `BlockingDataflowPipelineRunner` to execute the `WordCount` example on
Google Cloud Dataflow Service using managed resources in the Google Cloud Platform.
Start by following the general Cloud Dataflow
[Getting Started](https://cloud.google.com/dataflow/getting-started) instructions.
You should have a Google Cloud Platform project that has a Cloud Dataflow API enabled,
a Google Cloud Storage bucket that will serve as a staging location, and installed and
authenticated Google Cloud SDK. In this case, invoke the example as follows:

    mvn exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--project=<YOUR CLOUD PLATFORM PROJECT NAME> \
    --stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=BlockingDataflowPipelineRunner"

Your Cloud Storage location should be entered in the form of
`gs://bucket/path/to/staging/directory`. The Cloud Platform project refers to
its name (not number).

Alternatively, you may choose to bundle all dependencies into a single JAR and
execute it outside of the Maven environment. For example, after building and
installing as usual, you can execute the following commands to create the
bundled JAR of the `Examples` module and execute it both locally and in Cloud
Platform:

    mvn bundle:bundle -pl examples

    java -cp examples/target/google-cloud-dataflow-java-examples-all-bundled-manual_build.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --inputFile=<INPUT FILE PATTERN> --output=<OUTPUT FILE>

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

### Running the "Traffic" Streaming Examples

The `TrafficMaxLaneFlow` and `TrafficRoutes` pipelines, when run in
streaming mode (with the `--streaming=true` option), require the
publication of *traffic sensor* data to a
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs) topic.
You can run the example with default settings using the following command:

    mvn exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.TrafficMaxLaneFlow \
    -Dexec.args="--project=<YOUR CLOUD PLATFORM PROJECT NAME> \
    --stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=DataflowPipelineRunner \
    --streaming=true"

By default, they use a separate batch pipeline to publish previously gathered
traffic sensor data to the Cloud Pub/Sub topic, which is used as an input source
for the streaming pipeline.

The default traffic sensor data `--inputFile` is downloaded from

    curl -O \
    http://storage.googleapis.com/aju-sd-traffic/unzipped/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv

This file contains real traffic sensor data from San Diego freeways. See
<a href="http://storage.googleapis.com/aju-sd-traffic/freeway_detector_config/Freeways-Metadata-2010_01_01/copyright(san%20diego).txt">this file</a>
for copyright information.

You may override the default `--inputFile` with an alternative complete
data set (~2GB). It is provided in the Google Cloud Storage bucket
`gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv`.

You may also set `--inputFile` to an empty string, which will disable
the automatic Pub/Sub injection, and allow you to use separate tool to control
the input to this example. An example code, which publishes traffic sensor data
to a Pub/Sub topic, is provided in [`traffic_pubsub_generator.py`](https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/tree/master/gce-cmdline-publisher)

**Note:** If you set `--streaming=false`, these traffic pipelines will run in batch mode,
using the timestamps applied to the original dataset to process the data in
a batch. For further information on how these pipelines operate, see
<a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/TrafficMaxLaneFlow.java">TrafficMaxLaneFlow</a>
and <a href="https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/TrafficRoutes.java">TrafficRoutes</a>.
