---
type: languages
title: "Python multi-language pipelines quickstart"
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

# Python multi-language pipelines quickstart

This page provides instructions for running the updated multi-language examples for the Python SDK. For more details, see this step-by-step [guide](python-custom-multi-language-pipelines-guide.md) on creating and running custom multi-language transforms.

The code shown in this quickstart is available in a [collection of runnable examples](https://github.com/apache/beam/tree/master/examples/multi-language).

To build and run a multi-language Python pipeline, you need a Python environment with the Beam SDK installed. If you don’t have an environment set up, first complete the [Apache Beam Python SDK Quickstart](/get-started/quickstart-py/).

A *multi-language pipeline* is a pipeline that’s built in one Beam SDK language and uses transform(s) from another Beam SDK language. These “other-language” transforms are called [*cross-language transforms*](../glossary.md#cross-language-transforms). The idea is to make pipeline components easier to share across the Beam SDKs, and to grow the pool of available transforms for all the SDKs. In the examples below, the multi-language pipeline is built with the Beam Python SDK, and the cross-language transforms are built with the Beam Java SDK.

## Create a cross-language transform

Here's a Java transform provider, [ExtractWordsProvider](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/schematransforms/ExtractWordsProvider.java), that is uniquely identified with the URN `"beam:schematransform:org.apache.beam:extract_words:v1"`. Given a Configuration object, it will provide a transform:

```java
@AutoService(SchemaTransformProvider.class)
public class ExtractWordsProvider extends TypedSchemaTransformProvider<Configuration> {

    @Override
    public String identifier() {
        return "beam:schematransform:org.apache.beam:extract_words:v1";
    }

    @Override
    protected SchemaTransform from(Configuration configuration) {
        return new ExtractWordsTransform(configuration);
    }
}
```
> **NOTE**: To ensure that your URN doesn't run into confilcts with URNs from other transforms, follow the URN conventions described [here](../programming-guide.md#1314-defining-a-urn).


The config object is a simple Java object (POJO) that has fields required by the transform. AutoValue is encouraged, and the `@DefaultSchema` annotation helps Beam do some necessary conversions in the background:
```java
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  protected abstract static class Configuration {
    public static Builder builder() {
      return new AutoValue_ExtractWordsProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("List of words to drop.")
    public abstract List<String> getDrop();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDrop(List<String> drop);

      public abstract Configuration build();
    }
  }
```

Beam uses this configuration to generate a Python transform with the following signature:
```python
Extract(drop=["foo", "bar"])
```

The transform can be any implementation of your choice, as long as it meets the requirements of a [SchemaTransform](../glossary.md#schematransform). For this example, the transform does the following:

```java
  static class ExtractWordsTransform extends SchemaTransform {
    private static final Schema OUTPUT_SCHEMA = Schema.builder().addStringField("word").build();
    private final List<String> drop;

    ExtractWordsTransform(Configuration configuration) {
      this.drop = configuration.getDrop();
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      return PCollectionRowTuple.of(
          "output",
          input
              .getSinglePCollection()
              .apply(
                  ParDo.of(
                      new DoFn<Row, Row>() {
                        @ProcessElement
                        public void process(@Element Row element, OutputReceiver<Row> receiver) {
                          // Split the line into words.
                          String line = Preconditions.checkStateNotNull(element.getString("line"));
                          String[] words = line.split("[^\\p{L}]+", -1);
                          Arrays.stream(words)
                              .filter(w -> !drop.contains(w))
                              .forEach(
                                  word ->
                                      receiver.output(
                                          Row.withSchema(OUTPUT_SCHEMA)
                                              .withFieldValue("word", word)
                                              .build()));
                        }
                      }))
              .setRowSchema(OUTPUT_SCHEMA));
    }
  }
```

This example uses other Java transform providers as well, [JavaCountProvider](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/schematransforms/JavaCountProvider.java) and [WriteWordsProvider](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/schematransforms/WriteWordsProvider.java), but they follow the same pattern.

## Choose an expansion service

When building a job for a multi-language pipeline, Beam uses an [expansion service](../glossary#expansion-service) to expand [composite transforms](../glossary#composite-transform). You must have at least one expansion service per remote SDK.

Before running a multi-language pipeline, you need to build an expansion service that can access your Java transform. It’s often easier to create a single shaded JAR that contains both. Both Python and Java dependencies will be staged for the runner by the Python SDK.

Assuming you've built a JAR named **java-multilang-bundled-0.1.jar**, you can start the service with a command like the following, where `12345` is the port on which the expansion service will run:

```
java -jar java-multilang-bundled-0.1.jar 12345
```

For instructions on running an example expansion service, see [this README](https://github.com/apache/beam/blob/master/examples/multi-language/README.md#instructions-for-running-the-pipelines).

## Create a Python pipeline

Your Python pipeline can now use the [**ExternalTransformProvider**](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external_transform_provider.html#apache_beam.transforms.external_transform_provider.ExternalTransformProvider) API to configure your cross-language transform. Here’s an example constructed from [wordcount_external.py](https://github.com/apache/beam/blob/master/examples/multi-language/python/wordcount_external.py):

First, determine the transform identifiers you are looking for:

```python
EXTRACT_IDENTIFIER = "beam:schematransform:org.apache.beam:extract_words:v1"
COUNT_IDENTIFIER = "beam:schematransform:org.apache.beam:count:v1"
WRITE_IDENTIFIER = "beam:schematransform:org.apache.beam:write_words:v1"
```

Then, initialize the `ExternalTransformProvider` with your expansion service. This can take two parameters:

* `expansion_services`: an expansion service, or list of expansion services
* `urn_pattern`: (optional) a regex pattern to match valid transforms

```python
provider = ExternalTransformProvider("localhost:" + expansion_service_port)
```

Next, retrieve each portable transform:
```python
Extract = provider.get_urn(EXTRACT_IDENTIFIER)
Count = provider.get_urn(COUNT_IDENTIFIER)
Write = provider.get_urn(WRITE_IDENTIFIER)
```

Finally, build your multi-language Python pipeline using a mix of native and portable transforms:
```python
with beam.Pipeline(options=pipeline_options) as p:
_ = (p
     | 'Read' >> beam.io.ReadFromText(input_path)
     | 'Prepare Rows' >> beam.Map(lambda line: beam.Row(line=line))
     | 'Extract Words' >> Extract(drop=["king", "palace"])
     | 'Count Words' >> Count()
     | 'Format Text' >> beam.Map(lambda row: beam.Row(line="%s: %s" % (
          row.word, row.count))).with_output_types(
          RowTypeConstraint.from_fields([('line', str)]))
     | 'Write' >> Write(file_path_prefix=output_path))
```

## Run the pipeline

The exact commands for running the Python pipeline will vary based on your environment. Assuming that your pipeline is coded in a file named **wordcount_external.py**, the steps should be similar to those below. For more information, see [the comments in addprefix.py](https://github.com/apache/beam/blob/41d585f82b10195f758d14e3a54076ea1f05aa75/examples/multi-language/python/addprefix.py#L18-L40).

### Run with direct runner

In the following command, `input1` is a file containing lines of text:

```
$ python wordcount_external.py \
      --runner DirectRunner \
      --input <INPUT FILE> \
      --output <OUTPUT FILE> \
      --expansion_service_port <PORT>
```

### Run with Dataflow runner

The following script runs the multi-language pipeline on Dataflow, using example text from a Cloud Storage bucket. You'll need to adapt the script to your environment.

```
#!/bin/bash
export GCP_PROJECT=<project>
export GCS_BUCKET=<bucket>
export TEMP_LOCATION=gs://$GCS_BUCKET/tmp
export GCP_REGION=<region>
export JOB_NAME="wordcount-external-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="1"

gsutil rm gs://$GCS_BUCKET/wordcount-external/*

python wordcount_external.py \
      --runner DataflowRunner \
      --temp_location $TEMP_LOCATION \
      --project $GCP_PROJECT \
      --region $GCP_REGION \
      --job_name $JOB_NAME \
      --num_workers $NUM_WORKERS \
      --input "gs://dataflow-samples/shakespeare/kinglear.txt" \
      --output "gs://$GCS_BUCKET/wordcount-external/output" \
      --expansion_service_port <PORT>
```
