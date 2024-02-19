Prompt:
Is JSON supported in Apache Beam?

Response:
[JSON](https://www.json.org/) is a data format for storing and exchanging data. Apache Beam supports reading and writing JSON files using the `ReadFromJson` and `WriteToJson` transforms in the `TextIO` module. For more information, see the TextIO connector documentation:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio)
* [Typescript](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/textio.ts)

The [Dataflow cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) can help you to get started with JSON and Apache Beam.

Apache Beam pipeline code for reading data from a JSON file might look like the following example:

```python
class JsonOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--file_path',
            default="gs://your-bucket/your-file.json",
            help='Json file path'
        )

options = JsonOptions()
with beam.Pipeline(options=options) as p:
    output = (p | "Read from Json file" >> ReadFromJson(
        path=options.file_path,
        lines=False
        )
        | "Log Data" >> Map(logging.info))
```