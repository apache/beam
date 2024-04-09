Prompt:
Is CSV format supported in Apache Beam?

Response:
[CSV](https://en.wikipedia.org/wiki/Comma-separated_values) is a data format for storing and exchanging data. Apache Beam supports reading data from and writing data to CSV files using the `ReadFromText` and `WriteToText` transforms in the `TextIO` module. For more information, see the TextIO connector documentation:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio)
* [Typescript](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/textio.ts)

To get started with CSV and Apache Beam, refer to the [Dataflow Cookbook GitHub repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook).

Here's an example of Apache Beam pipeline code for reading data from a CSV file:

```python
class CsvOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--file_path',
            default="gs://your-bucket/your-file.csv",
            help='Csv file path'
        )

options = CsvOptions()

with beam.Pipeline(options=options) as p:
    output = (p | "Read from Csv file" >> ReadFromCsv(
        path=options.file_path
        )
        | "Log Data" >> Map(logging.info))

```
