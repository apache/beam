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

### Multi pipeline

Apache Beam is a popular open-source platform for building batch and streaming data processing pipelines. One of the key features of Apache Beam is its ability to support multi-language pipelines. With Apache Beam, you can write different parts of your pipeline in different programming languages, and they can all work together seamlessly.

Apache Beam supports multiple programming languages, including Java, Python, and Go. This makes it possible to use the language that best suits your needs for each part of your pipeline.

To build a multi-language pipeline with Apache Beam, you can use the following approach:

Define your pipeline using Apache Beam's SDK in your preferred programming language. This defines the data processing steps that need to be executed.

Use Apache Beam's language-specific SDKs to implement the data processing steps in the appropriate programming languages. For example, you could use Java to process some data, Python to process some other data, and Go to perform a specific computation.

Use Apache Beam's cross-language support to connect the different parts of your pipeline together. Apache Beam provides a common data model and serialization format, so data can be passed seamlessly between different languages.

By using Apache Beam's multi-language support, you can take advantage of the strengths of different programming languages, while still building a unified data processing pipeline. This can be especially useful when working with large datasets, as different languages may have different performance characteristics for different tasks.

To create a multi-language pipeline in Apache Beam, follow these steps:

Choose your SDKs: First, decide which programming languages and corresponding SDKs you'd like to use. Apache Beam currently supports Python, Java, and Go SDKs.

Set up the dependencies: Make sure you have installed the necessary dependencies for each language. For instance, you'll need the Beam Python SDK for Python or the Beam Java SDK for Java.

Create a pipeline: Using the primary language of your choice, create a pipeline object using the respective SDK. This pipeline will serve as the main entry point for your multi-language pipeline.

Use cross-language transforms: To execute transforms written in other languages, use the ExternalTransform class (in Python) or the External class (in Java). This allows you to use a transform written in another language as if it were a native transform in your main pipeline. You'll need to provide the appropriate expansion service address for the language of the transform.

{{if (eq .Sdk "java")}}

#### Start an expansion service

When building a job for a multi-language pipeline, Beam uses an expansion service to expand composite transforms. You must have at least one expansion service per remote SDK.

In the general case, if you have a supported version of Python installed on your system, you can let `PythonExternalTransform` handle the details of creating and starting up the expansion service. But if you want to customize the environment or use transforms not available in the default Beam SDK, you might need to run your own expansion service.

For example, to start the standard expansion service for a Python transform, ExpansionServiceServicer, follow these steps:

* Activate a new virtual environment following these instructions.

* Install Apache Beam with gcp and dataframe packages.
```
pip install 'apache-beam[gcp,dataframe]'
```
* Run the following command
```
python -m apache_beam.runners.portability.expansion_service_main -p <PORT> --fully_qualified_name_glob "*"
```

The command runs `expansion_service_main.py`, which starts the standard expansion service. When you use Gradle to run your Java pipeline, you can specify the expansion service with the `expansionService` option. For example: `--expansionService=localhost:<PORT>`.

#### Run Java program

```
private String getModelLoaderScript() {
        String s = "from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy\n";
        s = s + "from apache_beam.ml.inference.base import KeyedModelHandler\n";
        s = s + "def get_model_handler(model_uri):\n";
        s = s + "  return KeyedModelHandler(SklearnModelHandlerNumpy(model_uri))\n";

        return s;
}

void runExample(SklearnMnistClassificationOptions options, String expansionService) {
        // Schema of the output PCollection Row type to be provided to the RunInference transform.
        Schema schema =
                Schema.of(
                        Schema.Field.of("example", Schema.FieldType.array(Schema.FieldType.INT64)),
                        Schema.Field.of("inference", FieldType.STRING));

        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<Long, Iterable<Long>>> col =
                pipeline
                        .apply(TextIO.read().from(options.getInput()))
                        .apply(Filter.by(new FilterNonRecordsFn()))
                        .apply(MapElements.via(new RecordsToLabeledPixelsFn()));

        col.apply(RunInference.ofKVs(getModelLoaderScript(), schema, VarLongCoder.of())
                                .withKwarg("model_uri", options.getModelPath())
                                .withExpansionService(expansionService))
                .apply(MapElements.via(new FormatOutput()))
                .apply(TextIO.write().to("out.txt"));

        pipeline.run().waitUntilFinish();
}
```

> **Note**. Python extension service may not work on Windows. The **extension service** and the **Java pipeline** must run in the same environment. If the **extension service** is running in a **docker** container, you should run **Java pipeline** inside it.

#### Run in docker

Run **extension service** and **java program**:
```
# Use an official Java base image
FROM openjdk:11

# Install Python
RUN apt-get update && \
    apt-get install -y python python-pip

# Set the working directory
WORKDIR /app

# Copy Java and Python source files to the working directory
COPY src /app/src
COPY scripts /app/scripts

# Install Python dependencies
RUN pip install --upgrade pip \
RUN pip install apache-beam==2.46.0 \
RUN pip install --default-timeout=100 torch \
RUN pip install torchvision \
RUN pip install pandas \
RUN pip install scikit-learn

RUN wget https://repo1.maven.org/maven2/org/springframework/spring-expression/$SPRING_VERSION/spring-expression-$SPRING_VERSION.jar &&\
    mv spring-expression-$SPRING_VERSION.jar /opt/apache/beam/jars/spring-expression.jar

# Compile the Java program
RUN javac -d /app/bin /app/src/MyJavaProgram.java

# Set the entrypoint to run both the Java program and Python script
ENTRYPOINT ["bash", "-c", "java -cp /app/bin MyJavaProgram && python -m apache_beam.runners.portability.expansion_service_main -p 9090 --fully_qualified_name_glob=*"]
```
{{end}}


{{if (eq .Sdk "python")}}


Write own java service
```
import autovalue.shaded.com.google.auto.service.AutoService;
import autovalue.shaded.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

public class Task {
    static class JavaCount extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

        public JavaCount() {
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {
            return input
                    .apply(
                            "JavaCount", Count.perElement());
        }
    }

    static class JavaCountBuilder implements
            ExternalTransformBuilder<JavaCountConfiguration, PCollection<String>, PCollection<KV<String, Long>>> {

        @Override
        public PTransform<PCollection<String>, PCollection<KV<String, Long>>> buildExternal(
                JavaCountConfiguration configuration) {
            return new JavaCount();
        }
    }

    static class JavaCountConfiguration {

    }

    @AutoService(ExternalTransformRegistrar.class)
    public class JavaCountRegistrar implements ExternalTransformRegistrar {

        final String URN = "my.beam.transform.javacount";

        @Override
        public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
            return ImmutableMap.of(URN, new JavaCountBuilder());
        }
    }
}
```

Build jar:
```
mvn clean
mvn package -DskipTests
cd target
java -jar java-count-bundled-0.1.jar 12345
```

Python pipeline:
```
import logging
import re
import typing

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.options.pipeline_options import PipelineOptions


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the element being processed
    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run(input_path, output_path, pipeline_args):
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:
    lines = p | 'Read' >> ReadFromText(input_path).with_output_types(str)
    words = lines | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))

    java_output = (
        words
        | 'JavaCount' >> beam.ExternalTransform(
              'my.beam.transform.javacount',
              None,
              "localhost:12345"))

    def format(kv):
      key, value = kv
      return '%s:%s' % (key, value)

    output = java_output | 'Format' >> beam.Map(format)
    output | 'Write' >> WriteToText(output_path)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file')
  known_args, pipeline_args = parser.parse_known_args()

  run(
      known_args.input,
      known_args.output,
      pipeline_args)
```

Run program
```
python javacount.py --runner DirectRunner --environment_type=DOCKER --input input1 --output output --sdk_harness_container_image_overrides ".*java.*,chamikaramj/beam_java11_sdk:latest"
```
{{end}}

{{if (eq .Sdk "go")}}

When building a job for a multi-language pipeline, Beam uses an expansion service to expand composite transforms. You must have at least one expansion service per remote SDK.

In the general case, if you have a supported version of Python installed on your system, you can let `PythonExternalTransform` handle the details of creating and starting up the expansion service. But if you want to customize the environment or use transforms not available in the default Beam SDK, you might need to run your own expansion service.

For example, to start the standard expansion service for a Python transform, ExpansionServiceServicer, follow these steps:

* Activate a new virtual environment following these instructions.

* Install Apache Beam with gcp and dataframe packages.
```
pip install 'apache-beam[gcp,dataframe]'
```
* Run the following command
```
python -m apache_beam.runners.portability.expansion_service_main -p <PORT> --fully_qualified_name_glob "*"
```

The command runs `expansion_service_main.py`, which starts the standard expansion service. When you use Gradle to run your Java pipeline, you can specify the expansion service with the `expansionService` option. For example: `--expansionService=localhost:<PORT>`.

```
import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/external"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"strings"
)

func main() {
	// Parse command line arguments.
	flag.Parse()
	beam.Init()

	// Define the pipeline.
	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	// Read from input text file.
	input := textio.Read(scope, "input.txt")

	// Apply an external Python transform.
	pythonTransformURN := "beam:transforms:python_transform:v1"
	expansionServiceURL := "localhost:12345"
	pythonTransformOutput := external.Expand(scope, pythonTransformURN, input, expansionServiceURL)

	// Continue with Go transforms.
	processData := beam.ParDo(scope, func(element string) string {
		return "Go: " + strings.ToUpper(element)
	}, pythonTransformOutput)

	// Write the results to an output text file.
	textio.Write(scope, "output.txt", processData)

	// Run the pipeline.
	ctx := context.Background()
	err := beamx.Run(ctx, pipeline)
	if err != nil {
		panic(err)
	}
}
```
{{end}}