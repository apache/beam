---
type: languages
title: "Java multi-language pipelines quickstart"
aliases:
  - /documentation/patterns/cross-language/
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

# Java multi-language pipelines quickstart

This page provides a high-level overview of creating multi-language pipelines
with the Apache Beam SDK for Java. For a more complete discussion of the topic,
see
[Multi-language pipelines](/documentation/programming-guide/#multi-language-pipelines).

A *multi-language pipeline* is a pipeline that’s built in one Beam SDK language
and uses one or more transforms from another Beam SDK language. These transforms
from another SDK are called *cross-language transforms*. Multi-language support
makes pipeline components easier to share across the Beam SDKs and grows the
pool of available transforms for all the SDKs.

In the examples below, the multi-language pipeline is built with the Beam Java
SDK, and the cross-language transform is built with the Beam Python SDK.

## Prerequisites

This quickstart is based on a Java example pipeline,
[PythonDataframeWordCount](https://github.com/apache/beam/blob/master/examples/multi-language/src/main/java/org/apache/beam/examples/multilanguage/PythonDataframeWordCount.java),
that counts words in a Shakespeare text. If you’d like to run the pipeline, you
can clone or download the Beam repository and build the example from the source
code.

To build and run the example, you need a Java environment with the Beam Java SDK
version 2.41.0 or later installed, and a Python environment. If you don’t
already have these environments set up, first complete the
[Apache Beam Java SDK Quickstart](/get-started/quickstart-java/) and the
[Apache Beam Python SDK Quickstart](/get-started/quickstart-py/).

For running with portable DirectRunner, you need to have Docker installed
locally and the Docker daemon should be running. This is not needed for Dataflow.

For running on Dataflow, you need a Google Cloud project with billing enabled and a
[Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets).

This example relies on Python pandas package 1.4.0 or later which is unavailable
for Python versions earlier than 3.8. Hence please make sure that the default Python
version installed in your system is 3.8 or later.

## Specify a cross-language transform

The Java example pipeline uses the Python
[DataframeTransform](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/dataframe/transforms.py)
as a cross-language transform. The transform is part of the
[Beam Dataframe API](/documentation/dsls/dataframes/overview/) for working with
pandas-like
[DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)
objects.

To apply a cross-language transform, your pipeline must specify it. Python
transforms are identified by their fully qualified name. For example,
`DataframeTransform` can be found in the `apache_beam.dataframe.transforms`
package, so its fully qualified name is
`apache_beam.dataframe.transforms.DataframeTransform`.
The example pipeline,
[PythonDataframeWordCount](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/multilanguage/PythonDataframeWordCount.java),
passes this fully qualified name to
[PythonExternalTransform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/python/PythonExternalTransform.html).

> **Note:** The example pipeline is intended to demonstrate the development of
> Java multi-language pipelines that use arbitrary Python cross-language
> transforms. For production use cases of the Dataframe API in Java, you should
> use the higher-level
> [DataframeTransform](https://github.com/apache/beam/blob/master/sdks/java/extensions/python/src/main/java/org/apache/beam/sdk/extensions/python/transforms/DataframeTransform.java)
> instead.

Here's the complete pipeline definition from the example:

```java
static void runWordCount(WordCountOptions options) {
  Pipeline p = Pipeline.create(options);

  p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
      .apply(ParDo.of(new ExtractWordsFn()))
      .setRowSchema(ExtractWordsFn.SCHEMA)
      .apply(
          PythonExternalTransform.<PCollection<Row>, PCollection<Row>>from(
                  "apache_beam.dataframe.transforms.DataframeTransform",
                  options.getExpansionService())
              .withKwarg("func", PythonCallableSource.of("lambda df: df.groupby('word').sum()"))
              .withKwarg("include_indexes", true))
      .apply(MapElements.via(new FormatAsTextFn()))
      .apply("WriteCounts", TextIO.write().to(options.getOutput()));

  p.run().waitUntilFinish();
}
```

`PythonExternalTransform` is a wrapper for invoking external Python transforms.
The
[`from`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/python/PythonExternalTransform.html#from-java.lang.String-java.lang.String-)
method accepts two strings: 1) the fully qualified transform name; 2) an
optional address and port number for the expansion service. The method returns
a stub for the Python cross-language transform that can be used directly in a
Java pipeline.
[`withKwarg`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/python/PythonExternalTransform.html#withKwarg-java.lang.String-java.lang.Object-)
specifies a keyword argument for instantiating the Python cross-language
transform. In this case, `withKwarg` is invoked twice, to specify a `func`
argument and an `include_indexes` argument, and these arguments are passed to
`DataframeTransform`. `PythonExternalTransform` also provides other ways to
specify args and kwargs for Python cross-language transforms.

To understand how this pipeline works, it’s helpful to look more closely at the
first `withKwarg` invocation:

```java
.withKwarg("func", PythonCallableSource.of("lambda df: df.groupby('word').sum()"))
```

The argument to `PythonCallableSource.of` is a string representation of a Python
lambda function. `DataframeTransform` takes as an argument a Python callable to
apply to a `PCollection` as if it were a Dataframe. The `withKwarg` method lets
you specify a Python callable in your Java pipeline. To learn more about passing
a function to `DataframeTransform`, see
[Embedding DataFrames in a pipeline](/documentation/dsls/dataframes/overview/#embedding-dataframes-in-a-pipeline).

## Run the Java pipeline

If you want to customize the environment or use transforms not available in the
default Beam SDK, you might need to run your own expansion service. In such
cases, [start the expansion service](#advanced-start-an-expansion-service)
before running your pipeline.

Before running the pipeline, make sure to perform the
[runner specific setup](/get-started/quickstart-java/#run-a-pipeline) for your selected Beam runner.

### Run with Dataflow runner using a Maven Archetype (Beam 2.43.0 and later)

* Check out the Beam examples Maven archetype for the relevant Beam version.

```
export BEAM_VERSION=<Beam version>

mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion=$BEAM_VERSION \
    -DgroupId=org.example \
    -DartifactId=multi-language-beam \
    -Dversion="0.1" \
    -Dpackage=org.apache.beam.examples \
    -DinteractiveMode=false
```

* Run the pipeline.

```
export GCP_PROJECT=<GCP project>
export GCP_BUCKET=<GCP bucket>
export GCP_REGION=<GCP region>

mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.multilanguage.PythonDataframeWordCount \
    -Dexec.args="--runner=DataflowRunner --project=$GCP_PROJECT \
                 --region=$GCP_REGION \
                 --gcpTempLocation=gs://$GCP_BUCKET/multi-language-beam/tmp \
                 --output=gs://$GCP_BUCKET/multi-language-beam/output" \
    -Pdataflow-runner
```

### Run with Dataflow runner at HEAD

The following script runs the example multi-language pipeline on Dataflow, using
example text from a Cloud Storage bucket. You’ll need to adapt the script to
your environment.

```
export GCP_PROJECT=<project>
export OUTPUT_BUCKET=<bucket>
export GCP_REGION=<region>
export TEMP_LOCATION=gs://$OUTPUT_BUCKET/tmp

./gradlew :examples:multi-language:pythonDataframeWordCount --args=" \
--runner=DataflowRunner \
--project=$GCP_PROJECT \
--output=gs://${OUTPUT_BUCKET}/count \
--region=${GCP_REGION}"
```

The pipeline outputs a file with the results to
**gs://$OUTPUT_BUCKET/count-00000-of-00001**.

### Run with DirectRunner

> **Note:** Multi-language Pipelines need to use [portable](/roadmap/portability/)
> runners. Portable DirectRunner is still experimental and does not support all
> Beam features.

1. Create a Python virtual environment with the latest version of Beam Python SDK installed.
   Please see [here](/get-started/quickstart-py/) for instructions.
2. Run the job server for portable DirectRunner (implemented in Python).

```
export JOB_SERVER_PORT=<port>

python -m apache_beam.runners.portability.local_job_service_main -p $JOB_SERVER_PORT
```

3. In a different shell, go to a [Beam HEAD Git clone](https://github.com/apache/beam).

4. Build the Beam Java SDK container for a local pipeline execution
   (this guide requires that your JAVA_HOME is set to Java 11).

```
./gradlew :sdks:java:container:java11:docker -Pjava11Home=$JAVA_HOME
```

5. Run the pipeline.

```
export JOB_SERVER_PORT=<port>  # Same port as before
export OUTPUT_FILE=<local relative path>

./gradlew :examples:multi-language:pythonDataframeWordCount --args=" \
--runner=PortableRunner \
--jobEndpoint=localhost:$JOB_SERVER_PORT \
--output=$OUTPUT_FILE"
```

> **Note** This output gets written to the local file system of a Python Docker
> container. To verify the output by writing to GCS, you need to specify a
> publicly accessible
> GCS path for the `output` option since portable DirectRunner is currently
> unable to correctly forward local credentials for accessing GCS.

## Advanced: Start an expansion service

When building a job for a multi-language pipeline, Beam uses an
[expansion service](/documentation/glossary/#expansion-service) to expand
composite transforms. You must have at least one expansion service per remote
SDK.

In the general case, if you have a supported version of Python installed on your
system, you can let `PythonExternalTransform` handle the details of creating and
starting up the expansion service. But if you want to customize the environment
or use transforms not available in the default Beam SDK, you might need to run
your own expansion service.

For example, to start the standard expansion service for a Python transform,
[ExpansionServiceServicer](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/portability/expansion_service.py),
follow these steps:

1. Activate a new virtual environment following
[these instructions](/get-started/quickstart-py/#create-and-activate-a-virtual-environment).

2. Install Apache Beam with `gcp` and `dataframe` packages.

```
pip install 'apache-beam[gcp,dataframe]'
```

4. Run the following command

```
python -m apache_beam.runners.portability.expansion_service_main -p <PORT> --fully_qualified_name_glob "*"
```

The command runs
[expansion_service_main.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/portability/expansion_service_main.py), which starts the standard expansion service. When you use
Gradle to run your Java pipeline, you can specify the expansion service with the
`expansionService` option. For example: `--expansionService=localhost:<PORT>`.

## Next steps

To learn more about Beam support for cross-language pipelines, see
[Multi-language pipelines](/documentation/programming-guide/#multi-language-pipelines).
To learn more about the Beam DataFrame API, see
[Beam DataFrames overview](/documentation/dsls/dataframes/overview/).
