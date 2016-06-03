
# "Complete" Examples

This directory contains end-to-end example pipelines that perform complex data processing tasks. They include:

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

See the [documentation](https://cloud.google.com/dataflow/getting-started) and the [Examples
README](../../../../../../../../../README.md) for
information about how to run these examples.
