---
title: "Cross-language transforms"
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

# Cross-language transforms

With the samples on this page we will demonstrate how to create and leverage cross-language pipelines.

The goal of a cross-language pipeline is to incorporate transforms from one SDK (e.g. the Python SDK) into a pipeline written using another SDK (e.g. the Java SDK). This enables having already developed transforms (e.g. ML transforms in Python) and libraries (e.g. the vast library of IOs in Java), and strengths of certain languages at your disposal in whichever language you are more comfortable authoring pipelines while vastly expanding your toolkit in given language.

In this section we will cover a specific use-case: incorporating a Python transform that does inference on a model but is part of a larger Java pipeline. The section is broken down into 2 parts:

1. How to author the cross-language pipeline?
1. How to run the cross-language pipeline?

{{< language-switcher java py >}}

## How to author the cross-language pipeline?

This section digs into what changes when authoring a cross-language pipeline:

1. "Classic" pipeline in Java
1. External transform in Python
1. Expansion server

### "Classic" pipeline

We start by developing an Apache Beam pipeline like we would normally do if you were using only one SDK (e.g. the Java SDK):

{{< highlight java >}}
public class CrossLanguageTransform extends PTransform<PCollection<String>, PCollection<String>> {
    private static final String URN = "beam:transforms:xlang:pythontransform";

    private static String expansionAddress;

    public CrossLanguageTransform(String expansionAddress) {
        this.expansionAddress = expansionAddress;
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        PCollection<String> output =
            input.apply(
                "ExternalPythonTransform",
                External.of(URN, new byte [] {}, this.expansionAddress)
            );
    }
}

public class CrossLanguagePipeline {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        String expansionAddress = "localhost:9097"

        PCollection<String> inputs = p.apply(Create.of("features { feature { key: 'country' value { bytes_list { value: 'Belgium' }}}}"));
        input.apply(new CrossLanguageTransform(expansionAddress));

        p.run().waitUntilFinish();
    }
}
{{< /highlight >}}

The main differences with authoring a classic pipeline and transform are

- The PTransform uses the [External](https://github.com/apache/beam/blob/master/runners/core-construction-java/src/main/java/org/apache/beam/runners/core/construction/External.java) transform.
- This has a Uniform Resource Name (URN) which will identify the transform in your expansion service (more below).
- The address on which the expansion service is running.

Check the [documentation](https://beam.apache.org/documentation/programming-guide/#use-x-lang-transforms) for a deeper understanding of using external transforms.

### External transform

The transform we are trying to call from Java is defined in Python as follows:

{{< highlight java >}}
Implemented in Python.
{{< /highlight >}}

{{< highlight py >}}
URN = "beam:transforms:xlang:pythontransform"

@ptransform.PTransform.register_urn(URN, None)
class PythonTransform(ptransform.PTransform):
    def __init__(self):
        super().__init__()

    def expand(self, pcoll):
        return (pcoll
                | "Input preparation"
                    >> beam.Map(
                        lambda input: google.protobuf.text_format.Parse(input, tf.train.Example())
                    )
                | "Get predictions" >> RunInference(
                        model_spec_pb2.InferenceSpecType(
                            saved_model_spec=model_spec_pb2.SavedModelSpec(
                                model_path=model_path,
                                signature_name=['serving_default']))))

    def to_runner_api_parameter(self, unused_context):
        return URN, None

    def from_runner_api_parameter(
        unused_ptransform, unused_paramter, unused_context):
        return PythonTransform()
{{< /highlight >}}

Check the [documentation](https://beam.apache.org/documentation/programming-guide/#create-x-lang-transforms) for a deeper understanding of creating an external transform.

### Expansion service

The expansion service is written in the same language as the external transform. It takes care of injecting the transforms in your pipeline before submitting them to the Runner.

{{< highlight java >}}
Implemented in Python.
{{< /highlight >}}

{{< highlight py >}}
def main(unused_argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-p', '--port', type=int, help='port on which to serve the job api')
  options = parser.parse_args()
  global server
  server = grpc.server(thread_pool_executor.shared_unbounded_instance())
  beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
      expansion_service.ExpansionServiceServicer(
          PipelineOptions(
              ["--experiments", "beam_fn_api", "--sdk_location", "container"])), server)
  server.add_insecure_port('localhost:{}'.format(options.port))
  server.start()
  _LOGGER.info('Listening for expansion requests at %d', options.port)

  signal.signal(signal.SIGTERM, cleanup)
  signal.signal(signal.SIGINT, cleanup)
  signal.pause()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv)
{{< /highlight >}}

## How to run the cross-language pipeline?

In this section, the steps to run a cross-language pipeline are set out:

1. Start the **expansion service** with your Python transforms: `python expansion_service.py -p 9097`
1. Start the **Job Server** which will translated into the stage that will run on your back-end or runner (e.g. Spark):

   - From Apache Beam source code:
     `./gradlew :runners:spark:job-server:runShadow`
   - Using the pre-build Docker container:
     `docker run -net=host apache/beam_spark_job_server`

1. **Run pipeline**: ```mvn exec:java -Dexec.mainClass=CrossLanguagePipeline \
    -Pportable-runner \
    -Dexec.args=" \
        --runner=PortableRunner \
        --jobEndpoint=localhost:8099 \
        --useExternal=true \
        --expansionServiceURL=localhost:9097 \
        --experiments=beam_fn_api"```
