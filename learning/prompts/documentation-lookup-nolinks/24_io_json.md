Prompt:
Is JSON supported in Apache Beam?

Response:
JSON is a data format for storing and exchanging data. Apache Beam supports reading data from and writing data to JSON files using the `ReadFromJson` and `WriteToJson` transforms within the `TextIO` module. For more information, see the TextIO connector documentation for your programming language of choice.

To get started with JSON and Apache Beam, refer to the Dataflow Cookbook GitHub repository.

Here is an example of Apache Beam pipeline code for reading data from a JSON file:

```python
class JsonOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--file_path",
            default="gs://your-bucket/your-file.json",
            help="Json file path",
        )


options = JsonOptions()
with beam.Pipeline(options=options) as p:
    output = (
        p
        | "Read from Json file" >> ReadFromJson(path=options.file_path, lines=False)
        | "Log Data" >> Map(logging.info)
    )
```
