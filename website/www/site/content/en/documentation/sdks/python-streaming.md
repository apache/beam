---
type: languages
title: "Apache Beam Python Streaming Pipelines"
---
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

# Python Streaming Pipelines

Python streaming pipeline execution became available (with some
[limitations](#unsupported-features)) starting with Beam SDK version 2.5.0.


## Why use streaming execution?

Beam creates an unbounded PCollection if your pipeline reads from a streaming or
continously-updating data source (such as Cloud Pub/Sub). A runner must
process an unbounded PCollection using a streaming job that runs continuously,
as the entire collection is never available for processing at any one time.
[Size and boundedness](/documentation/programming-guide/#size-and-boundedness)
has more information about bounded and unbounded collections.


## Modifying a pipeline to use stream processing

To modify a batch pipeline to support streaming, you must make the following
code changes:

* Use an I/O connector that supports reading from an unbounded source.
* Use an I/O connector that supports writing to an unbounded source.
* Choose a [windowing strategy](/documentation/programming-guide/index.html#windowing).

The Beam SDK for Python includes two I/O connectors that support unbounded
PCollections: Google Cloud Pub/Sub (reading and writing) and Google BigQuery
(writing).

The following snippets show the necessary code changes to modify the batch
WordCount example to support streaming:

These batch WordCount snippets are from
[wordcount.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py).
This code uses the TextIO I/O connector to read from and write to a bounded
collection.

```
  lines = p | 'read' >> ReadFromText(known_args.input)
  ...

  counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(six.text_type))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_ones))
  ...

  output = counts | 'format' >> beam.Map(format_result)

  # Write the output using a "Write" transform that has side effects.
  output | 'write' >> WriteToText(known_args.output)
```

These streaming WordCount snippets are from
[streaming_wordcount.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/streaming_wordcount.py).
This code uses an I/O connector that reads from and writes to an unbounded
source (Cloud Pub/Sub) and specifies a fixed windowing strategy.

```
  lines = p | beam.io.ReadFromPubSub(topic=known_args.input_topic)
  ...

  counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(six.text_type))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | beam.WindowInto(window.FixedWindows(15, 0))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_ones))

  ...

  output = counts | 'format' >> beam.Map(format_result)

  # Write to Pub/Sub
  output | beam.io.WriteStringsToPubSub(known_args.output_topic)
```

## Running a streaming pipeline

To run the example streaming WordCount pipeline, you must have a Cloud Pub/Sub
input topic and output topic. To create, subscribe to, and pull from a topic for
testing purposes, you can use the commands in the [Cloud Pub/Sub quickstart](https://cloud.google.com/pubsub/docs/quickstart-cli).

The following simple bash script feeds lines of an input text file to your input
topic:

```
cat <YOUR_LOCAL_TEXT_FILE> | while read line; do gcloud pubsub topics publish <YOUR_INPUT_TOPIC_NAME> --message "$line"; done
```

Alternately, you can read from a publicly available Cloud Pub/Sub stream, such
as `projects/pubsub-public-data/topics/taxirides-realtime`. However, you must
create your own output topic to test writes.

The following commands run the
[streaming_wordcount.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/streaming_wordcount.py)
example streaming pipeline. Specify your Cloud Pub/Sub project and input topic
(`--input_topic`), output Cloud Pub/Sub project and topic (`--output_topic`).

{{< highlight class="runner-direct" >}}
# DirectRunner requires the --streaming option
python -m apache_beam.examples.streaming_wordcount \
  --input_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_INPUT_TOPIC" \
  --output_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_OUTPUT_TOPIC" \
  --streaming
{{< /highlight >}}

{{< highlight class="runner-flink" >}}
See https://beam.apache.org/documentation/runners/flink/ for more information.
{{< /highlight >}}

{{< highlight class="runner-spark" >}}
See https://beam.apache.org/documentation/runners/spark/ for more information.
{{< /highlight >}}

{{< highlight class="runner-dataflow" >}}
# As part of the initial setup, install Google Cloud Platform specific extra components.
pip install apache-beam[gcp]

# DataflowRunner requires the --streaming option
python -m apache_beam.examples.streaming_wordcount \
  --runner DataflowRunner \
  --project YOUR_GCP_PROJECT \
  --region YOUR_GCP_REGION \
  --temp_location gs://YOUR_GCS_BUCKET/tmp/ \
  --input_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_INPUT_TOPIC" \
  --output_topic "projects/YOUR_PUBSUB_PROJECT_NAME/topics/YOUR_OUTPUT_TOPIC" \
  --streaming
{{< /highlight >}}

Check your runner's documentation for any additional runner-specific information
about executing streaming pipelines:

- [DirectRunner streaming execution](/documentation/runners/direct/#streaming-execution)
- [DataflowRunner streaming execution](/documentation/runners/dataflow/#streaming-execution)
- [Portable Flink runner](/documentation/runners/flink/)

## Unsupported features

Python streaming execution does not currently support the following features:

- Custom source API
- User-defined custom merging `WindowFn` (with fnapi)
- For portable runners, see [portability support table](https://s.apache.org/apache-beam-portability-support-table).
