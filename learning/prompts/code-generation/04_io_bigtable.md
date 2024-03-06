Prompt:
Write the Python code to read data from Bigtable using Apache Beam.

Response:
Your Apache Beam pipeline can read data from [Cloud Bigtable](https://cloud.google.com/bigtable) using the `ReadFromBigtable` transform. The following Python code reads a table `table_id` from a Bigtable instance `instance_id` and logs the data to the console. The `table_id` and `instance_id` are provided as command-line arguments.

```python
import logging
import apache_beam as beam

from apache_beam import Map
from apache_beam.io.gcp.bigtableio import ReadFromBigtable
from apache_beam.options.pipeline_options import PipelineOptions

class BigtableOptions(PipelineOptions):
"""
Configure pipeline options for Bigtable read transform.
"""
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


def run():
    """
    This pipeline shows how to read from Cloud Bigtable.
    """

    #parse pipeline options
    options = BigtableOptions()

    with beam.Pipeline(options=options) as p:
        #this pipeline reads from a Bigtable table and logs the data to the console
        output = (p | "Read from Bigtable" >> ReadFromBigtable(
                    project_id=options.project_id,
                    instance_id=options.instance_id,
                    table_id=options.table_id
                 )
                    | "Extract cells" >> beam.Map(lambda row: row._cells)
                    | "Log Data" >> Map(logging.info))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
```
The `ReadFromBigtable` transform returns a `PCollection` of `PartialRowData` objects, each representing a Bigtable row. For more information about this row object, see [PartialRowData (row_key)](https://cloud.google.com/python/docs/reference/bigtable/latest/row#class-googlecloudbigtablerowpartialrowdatarowkey).

For more information, see the [BigTable I/O connector documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html).

For samples that show common pipeline configurations, see [Pipeline option patterns](https://beam.apache.org/documentation/patterns/pipeline-options/).

