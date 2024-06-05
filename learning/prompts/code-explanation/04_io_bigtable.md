Prompt:
What does this code do?
```python
class BigtableOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            required=True,
            help='Project ID'
        )
        parser.add_argument(
            '--instance_id',
            default="beam-test",
            help='Cloud Bigtable instance ID'
        )
        parser.add_argument(
            '--table_id',
            default="your-test-table",
            help='Cloud Bigtable table ID'
        )

    options = BigtableOptions()

with beam.Pipeline(options=options) as p:
    output = (p | "Read from Bigtable" >> ReadFromBigtable(
        project_id=options.project_id,
        instance_id=options.instance_id,
        table_id=options.table_id
        )
        | "Extract cells" >> beam.Map(lambda row: row._cells)
        | "Log Data" >> Map(logging.info))
```

Response:
This code uses the Apache Beam [BigtableIO connector](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html) to read data from a [Bigtable](https://cloud.google.com/bigtable/docs) table `table_id` in a Bigtable instance `instance_id`.

The `BigtableOptions` class defines the command-line arguments `project_id`, ‘instance_id’, and `table_id`, which are used to configure the `ReadFromBigtable` transform. These arguments are parsed from the command line using [pipeline options](https://beam.apache.org/documentation/patterns/pipeline-options/).

The `ReadFromBigtable` transform returns a `PCollection` of `PartialRowData` objects, each representing a Bigtable row. For more information about this row object, see [PartialRowData (row_key)](https://cloud.google.com/python/docs/reference/bigtable/latest/row#class-googlecloudbigtablerowpartialrowdatarowkey).

The Apache Beam pipeline extracts the data from a Bigtable row and logs it to the console.

For more information about how to use the BigtableIO connector with the Python SDK for Apache Beam, see [BigtableIO module](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html).
