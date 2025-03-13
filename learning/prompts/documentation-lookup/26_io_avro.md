Prompt:
Is Apache Avro format supported in Apache Beam?

Response:
[Apache Avro](https://avro.apache.org/) is a data format for storing and exchanging data. Apache Beam supports reading from and writing to Avro files using the `ReadFromAvro` and `WriteToAvro` transforms in the `AvroIO` module. For more information, see the AvroIO connector documentation for the following languages:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/avro/io/AvroIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.avroio.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio)
* [Typescript (using cross-language transforms)](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/avroio.ts)

To get started with Avro and Apache Beam, refer to the [Dataflow Cookbook GitHub repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook).

Here is an example of Apache Beam pipeline code for reading data from an Avro file:

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
