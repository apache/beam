Prompt:
Is Apache Parquet format supported in Apache Beam?

Response:
[Apache Parquet](https://parquet.apache.org/) is a data format for storing and exchanging data. Apache Beam supports reading from and writing to Parquet files using the `ReadFromParquet` and `WriteToParquet` transforms in the `ParquetIO` module. For more information, see the ParquetIO connector documentation for the following languages:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/parquet/ParquetIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.parquetio.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/parquetio)
* [Typescript (using cross-language transforms)](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/parquetio.ts)

To get started with Avro and Apache Beam, refer to the [Dataflow Cookbook GitHub repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook).

Here is an example of Apache Beam pipeline code for reading data from a Parquet file:

```python
class ReadParquetOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--path",
          help="GCS path to parquet file")

  options = ReadParquetOptions()

  with beam.Pipeline(options=options) as p:

    (p | "ReadParquet" >> ReadFromParquet(options.path)
       | Map(logging.info))

```
