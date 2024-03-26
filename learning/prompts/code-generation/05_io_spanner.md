Prompt:
Write the Python code to read data from Cloud Spanner using Apache Beam.

Response:
Your Apache Beam pipeline can read data from [Cloud Spanner](https://cloud.google.com/spanner) using the Apache Beam [SpannerIO connector](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.spanner.html). The SpannerIO connector lets you read data from and write data to Cloud Spanner. Cloud Spanner is supported in both batch pipelines and streaming pipelines.

```python
import logging
import apache_beam as beam

from apache_beam import coders
from apache_beam import Map
from apache_beam.io.gcp.spanner import ReadFromSpanner
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple

class ExampleRow(NamedTuple):
    id: int
    name: str

class SpannerOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            default='your-project-id',
            help='Google Cloud project ID'
        )
        parser.add_argument(
            '--instance_id',
            default='your-instance-id',
            help='Google Cloud Spanner instance ID'
        )
        parser.add_argument(
            '--database_id',
            default='your-database-id',
            help='Google Cloud Spanner database ID'
        )

options = SpannerOptions()
coders.registry.register_coder(ExampleRow, coders.RowCoder)

with beam.Pipeline(options=options) as p:

    output = (p | "Read from table" >> ReadFromSpanner(
        project_id=options.project_id,
        instance_id=options.instance_id,
        database_id=options.database_id,
        row_type=ExampleRow,
        sql="SELECT * FROM example_row"
        )
        | "Log Data" >> Map(logging.info))
```

The `ReadFromSpanner` transform is a built-in Apache Beam transform that reads data from a Cloud Spanner table. The `ReadFromSpanner` transform returns a `PCollection` of `NamedTuple` objects. The `NamedTuple` object is a Python class that represents a row in a Cloud Spanner table.

Registering a coder for `NamedTuple` is required to use `NamedTuple` as a row type:

```python
 coders.registry.register_coder(ExampleRow, coders.RowCoder)
```
For more information about how to register a coder for a custom type, see [Data encoding and type safety](https://beam.apache.org/documentation/programming-guide/#data-encoding-and-type-safety).

The `SpannerOptions` class defines the command-line arguments `project_id`, `instance_id`, and `database_id`, which are used to configure the `ReadFromSpanner` transform. These arguments are parsed from the command line using [pipeline options](https://beam.apache.org/documentation/patterns/pipeline-options/).
