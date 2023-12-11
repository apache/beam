Prompt:
Is Avro format supported in Apache Beam?
Response:
[Apache Avro](https://avro.apache.org/) is a popular data format for storing and exchanging data. Apache Beam supports reading and writing Avro files using the `ReadFromAvro` and `WriteToAvro` transforms from a `AvroIO` module:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/avro/io/AvroIO.html),
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.avroio.html).
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio),
* [Typescript via X-language](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/avroio.ts)

[Dataflow-cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) will help you to get started with Avro and Apache Beam.

Apache Beam pipeline code for reading data from Avro file might look like the following example:
```python

class ReadAvroOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--path",
          default="gs://cloud-samples-data/bigquery/us-states/*.avro",
          help="GCS path to read from")

  options = ReadAvroOptions()

  with beam.Pipeline(options=options) as p:

    (p | "Read from Avro" >> ReadFromAvro(options.path)
       | Map(logging.info))
```
