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

# (Legacy) Python multi-language pipelines quickstart

> Note: it's encouraged to use the newer SchemaTransform framework; check out the updated [quickstart](python-multi-language-pipelines-2.md) and [guide](python-custom-multi-language-pipelines-guide.md)

This page provides a high-level overview of creating multi-language pipelines with the Apache Beam SDK for Python. For a more comprehensive treatment of the topic, see [Multi-language pipelines](/documentation/programming-guide/#multi-language-pipelines).

The code shown in this quickstart is available in a [collection of runnable examples](https://github.com/apache/beam/tree/master/examples/multi-language).

To build and run a multi-language Python pipeline, you need a Python environment with the Beam SDK installed. If you don’t have an environment set up, first complete the [Apache Beam Python SDK Quickstart](/get-started/quickstart-py/).

A *multi-language pipeline* is a pipeline that’s built in one Beam SDK language and uses one or more transforms from another Beam SDK language. These “other-language” transforms are called *cross-language transforms*. The idea is to make pipeline components easier to share across the Beam SDKs, and to grow the pool of available transforms for all the SDKs. In the examples below, the multi-language pipeline is built with the Beam Python SDK, and the cross-language transforms are built with the Beam Java SDK.

## Create a cross-language transform

Here's a simple Java transform, [JavaPrefix](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/JavaPrefix.java), that adds a prefix to an input string:

```java
public class JavaPrefix extends PTransform<PCollection<String>, PCollection<String>> {

  final String prefix;

  public JavaPrefix(String prefix) {
    this.prefix = prefix;
  }

  class AddPrefixDoFn extends DoFn<String, String> {

    @ProcessElement
    public void process(@Element String input, OutputReceiver<String> o) {
      o.output(prefix + input);
    }
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input
        .apply(
            "AddPrefix",
            ParDo.of(new AddPrefixDoFn()));
  }
}
```

To make this available as a cross-language transform, you have to add a config object and a builder.

> **Note:** Starting with Beam 2.34.0, Python SDK users can use some Java transforms without writing additional Java code. To learn more, see [Creating cross-language Java transforms](/documentation/programming-guide/#1311-creating-cross-language-java-transforms).

The config object is a simple Java object (POJO) that has fields required by the transform. Here's an example, [JavaPrefixConfiguration](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/JavaPrefixConfiguration.java):

```java
public class JavaPrefixConfiguration {

  String prefix;

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }
}
```

The builder class, implemented below as [JavaPrefixBuilder](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/JavaPrefixBuilder.java), must implement [ExternalTransformBuilder](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ExternalTransformBuilder.html) and override `buildExternal`, which uses the config object.

```java
public class JavaPrefixBuilder implements
    ExternalTransformBuilder<JavaPrefixConfiguration, PCollection<String>, PCollection<String>> {

    @Override
    public PTransform<PCollection<String>, PCollection<String>> buildExternal(
        JavaPrefixConfiguration configuration) {
      return new JavaPrefix(configuration.prefix);
    }
}
```

You also need to add a registrar class to register your transform with the expansion service.

```java
@AutoService(ExternalTransformRegistrar.class)
public class JavaPrefixRegistrar implements ExternalTransformRegistrar {

  final String URN = "beam:transform:my.beam.test:javaprefix:v1";

  @Override
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    return ImmutableMap.of(URN,new JavaPrefixBuilder());
  }
}
```

As shown here in [JavaPrefixRegistrar](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/JavaPrefixRegistrar.java), the registrar must implement [ExternalTransformRegistrar](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/expansion/ExternalTransformRegistrar.html), which has one method, `knownBuilderInstances`. This returns a map that maps a unique URN to an instance of your builder. You can use the [AutoService](https://github.com/google/auto/tree/master/service) annotation to register this class with the expansion service.

## Choose an expansion service

When building a job for a multi-language pipeline, Beam uses an [expansion service](/documentation/glossary/#expansion-service) to expand [composite transforms](/documentation/glossary/#composite-transform). You must have at least one expansion service per remote SDK.

In most cases, you can use the default Java [ExpansionService](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/expansion/service/ExpansionService.html). The service takes a single parameter, which specifies the port of the expansion service. The address is then provided by the Python pipeline.

Before running your multi-language pipeline, you need to build the Java cross-language transform and start the expansion service. When you start the expansion service, you need to add dependencies to the classpath. You can use more than one JAR, but it’s often easier to create a single shaded JAR. Both Python and Java dependencies will be staged for the runner by the Python SDK.

The steps for running the expansion service will vary depending on your build tooling. Assuming you've built a JAR named **java-prefix-bundled-0.1.jar**, you can start the service with a command like the following, where `12345` is the port on which the expansion service will run:

```
java -jar java-prefix-bundled-0.1.jar 12345
```

For instructions on running an example expansion service, see [this README](https://github.com/apache/beam/blob/master/examples/multi-language/README.md#instructions-for-running-the-pipelines).

## Create a Python pipeline

Your Python pipeline can now use the [ExternalTransform](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external.html#apache_beam.transforms.external.ExternalTransform) API to configure your cross-language transform. Here’s an example from [addprefix.py](https://github.com/apache/beam/blob/master/examples/multi-language/python/addprefix.py):

```py
with beam.Pipeline(options=pipeline_options) as p:
  input = p | 'Read' >> ReadFromText(input_path).with_output_types(str)

  java_output = (
      input
      | 'JavaPrefix' >> beam.ExternalTransform(
            'beam:transform:my.beam.test:javaprefix:v1',
            ImplicitSchemaPayloadBuilder({'prefix': 'java:'}),
            "localhost:12345"))

  def python_prefix(record):
    return 'python:%s' % record

  output = java_output | 'PythonPrefix' >> beam.Map(python_prefix)
  output | 'Write' >> WriteToText(output_path)
```

`ExternalTransform` takes three parameters:

* The URN for the cross-language transform
* The payload, either as a byte string or a [PayloadBuilder](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external.html#apache_beam.transforms.external.PayloadBuilder)
* An expansion service

The URN is simply a unique Beam identifier for the transform, and the expansion service has already been discussed. The PayloadBuilder is a new concept, discussed next.

> **NOTE**: To ensure that your URN doesn't run into confilcts with URNs from other transforms, follow the URN conventions described at [Selecting a URN for Cross-language Transforms](/documentation/programming-guide/#1314-selecting-a-urn-for-cross-language-transforms).

## Provide a payload builder

The Python pipeline example above provides an [ImplicitSchemaPayloadBuilder](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external.html#apache_beam.transforms.external.ImplicitSchemaPayloadBuilder) as the second argument to `ExternalTransform`. The `ImplicitSchemaPayloadBuilder` builds a payload that generates a schema from the provided values. In this case, the provided values are contained in the following key-value pair: `{'prefix': 'java:'}`. The `JavaPrefix` transform expects a `prefix` argument, and the payload builder passes in the string `java:`, which will be prepended to each input element.

Payload builders help build the payload for the transform in the expansion request. Instead of the `ImplicitSchemaPayloadBuilder`, you could use a [NamedTupleBasedPayloadBuilder](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external.html#apache_beam.transforms.external.NamedTupleBasedPayloadBuilder), which builds a payload based on a named tuple schema, or an [AnnotationBasedPayloadBuilder](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external.html#apache_beam.transforms.external.AnnotationBasedPayloadBuilder), which builds a schema based on type annotations. For a complete list of available payload builders, see the [transforms.external API reference](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external.html).

## Use standard element types

At a multi-language boundary, you have to use element types that all the Beam SDKs understand. These are types represented by the [Beam standard coders](https://github.com/apache/beam/blob/42e1ae8f8d07fb9c6fde14bd688c5a4b763d9d6e/model/pipeline/src/main/proto/beam_runner_api.proto#L784):

* `BYTES`
* `STRING_UTF8`
* `KV`
* `BOOL`
* `VARINT`
* `DOUBLE`
* `ITERABLE`
* `TIMER`
* `WINDOWED_VALUE`
* `ROW`

For arbitrary structured types (for example, an arbitrary Java object), use `ROW` (`PCollection<Row>`). You may have to develop a new Java composite transform that produces a `PCollection<Row>`. You can use SDK-specific coders within a composite cross-language transform, as long as these coders aren't used by PCollections that are consumed by the other SDKs.

## Run the pipeline

The exact commands for running the Python pipeline will vary based on your environment. Assuming that your pipeline is coded in a file named **addprefix.py**, the steps should be similar to those below. For more information, see [the comments in addprefix.py](https://github.com/apache/beam/blob/41d585f82b10195f758d14e3a54076ea1f05aa75/examples/multi-language/python/addprefix.py#L18-L40).

### Run with direct runner

In the following command, `input1` is a file containing lines of text:

```
python addprefix.py --runner DirectRunner --environment_type=DOCKER --input input1 --output output
```

### Run with Dataflow runner

The following script runs the multi-language pipeline on Dataflow, using example text from a Cloud Storage bucket. You'll need to adapt the script to your environment.

```
#!/bin/bash
export GCP_PROJECT=<project>
export GCS_BUCKET=<bucket>
export TEMP_LOCATION=gs://$GCS_BUCKET/tmp
export GCP_REGION=<region>
export JOB_NAME="javaprefix-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="1"

# other commands, e.g. changing into the appropriate directory

gsutil rm gs://$GCS_BUCKET/javaprefix/*

python addprefix.py \
    --runner DataflowRunner \
    --temp_location $TEMP_LOCATION \
    --project $GCP_PROJECT \
    --region $GCP_REGION \
    --job_name $JOB_NAME \
    --num_workers $NUM_WORKERS \
    --input "gs://dataflow-samples/shakespeare/kinglear.txt" \
    --output "gs://$GCS_BUCKET/javaprefix/output"
```
