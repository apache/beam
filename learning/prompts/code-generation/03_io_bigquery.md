Prompt:
Write the Python code to read data from BigQuery using Apache Beam.

Response:
You can read data from BigQuery using the `ReadFromBigQuery` transform. The following Python code reads data from a BigQuery table. The table name is provided as the command-line argument `table`. The data is logged to the console.

```python
import logging

import apache_beam as beam
from apache_beam.io import ReadFromBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

class BigQueryReadOptions(PipelineOptions):
"""
Configure pipeline options for BigQuery read transform.
"""
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--table",
          required=True,
          help="BigQuery table to read from.")

def read_table():
    """Read from BigQuery table function."""

    #parse pipeline options
    #streaming=True is required for a streaming pipeline
    options = BigQueryReadOptions(streaming=True)

    with beam.Pipeline(options=options) as p:
        #this pipeline reads from a BigQuery table and logs the data to the console
        (p | "Read BigQuery table" >> ReadFromBigQuery(table=options.table)
         | "Format data" >> Map(lambda row: f"Received row:\n{row}\n")
         | Map(logging.info))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    read_table()
```
The `ReadFromBigQuery` transform also supports reading from a BigQuery query. The following Python code reads data from a BigQuery table using a SQL query. The query is provided as the command-line argument `query`. The data is logged to the console.

```python
with beam.Pipeline(options=options) as p:
    p | "Read BigQuery table" >> ReadFromBigQuery(query='SELECT * FROM table')
      | "Format data" >> Map(lambda row: f"Received row:\n{row}\n")
      | Map(logging.info)
 ```

For more information, see the [BigQueryIO connector documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html).

For samples that show common pipeline configurations, see [Pipeline option patterns](https://beam.apache.org/documentation/patterns/pipeline-options/).

