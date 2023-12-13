Prompt:
Write the python code to read data from Cloud Spanner using Apache Beam.
Response:
You can read data from [Cloud Spanner](https://cloud.google.com/spanner) using the Apache Beam [SpannerIO](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.spanner.html) connector which allows you to read and write data from and to Spanner. Cloud Spanner is supported both in batch and streaming pipelines:

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

ReadFromSpanner is a built-in Apache Beam transform that reads data from a Cloud Spanner table. The `ReadFromSpanner` transform returns a `PCollection` of `NamedTuple` objects. The `NamedTuple` object is a Python class that represents a row in a Cloud Spanner table. 

Registering a coder for `NamedTuple` is required to use `NamedTuple` as a row type:
```python
 coders.registry.register_coder(ExampleRow, coders.RowCoder)
```
For more information on how to register a coder for a custom type, visit [here](https://beam.apache.org/documentation/programming-guide/#data-encoding-and-type-safety).

SpannerOptions class defines three command line arguments `project_id`, `instance_id` and `database_id` that are used to configure `ReadFromSpanner` transform. Those arguments are parsed from a command line using [Pipeline option pattern](https://beam.apache.org/documentation/patterns/pipeline-options/). 