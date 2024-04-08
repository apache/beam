Prompt:
Is Apache Parquet format supported in Apache Beam?

Response:
Apache Parquet is a data format for storing and exchanging data. Apache Beam supports reading from and writing to Parquet files using the `ReadFromParquet` and `WriteToParquet` transforms in the `ParquetIO` module. For more information, see the ParquetIO connector documentation for your programming language of choice.

To get started with Avro and Apache Beam, refer to the Dataflow Cookbook GitHub repository.

Here is an example of Apache Beam pipeline code for reading data from a Parquet file:

```python
class ReadParquetOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--path", help="GCS path to parquet file")

    options = ReadParquetOptions()

    with beam.Pipeline(options=options) as p:
        (p | "ReadParquet" >> ReadFromParquet(options.path) | Map(logging.info))
```
