Prompt:
What does this code do?
```python
class BigQueryReadTableOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
      "--table",
      default="bigquery-public-data:samples.github_timeline",
      help="BigQuery table to read data from")

options = BigQueryReadTableOptions()
with beam.Pipeline(options=options) as p:
  output = (p | "Read table" >> ReadFromBigQuery(table=options.table)
  | "Log data" >> Map(logging.info)
  )
```

Response:
This code uses the [Google BigQuery I/O connector](https://beam.apache.org/documentation/io/built-in/google-bigquery/) to read data from a [BigQuery](https://cloud.google.com/bigquery/docs) table `table`.

The BigQueryIO connector supports reading from BigQuery tables, executing queries, and reading from the BigQuery Storage API (beta). It supports writing to BigQuery tables in batch and streaming modes.

```python
output = (p | "Read table" >> ReadFromBigQuery(table=options.table))
```

The `ReadFromBigQuery` transform uses a BigQuery export job to save a table snapshot to Google Cloud Storage and then reads from each produced file. The default file format is Avro.

You can also read data from BigQuery by using an SQL query.

```python
output = (p | "Read table" >> ReadFromBigQuery(query="SELECT * FROM table"))
```

For more information about how to use the BigQueryIO connector with the Python SDK for Apache Beam, see [BigQueryIO module](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html).

This code uses [pipeline options](https://beam.apache.org/documentation/patterns/pipeline-options/) for the required `table` argument. The `table` argument is used to specify the BigQuery table to read data from.

For performance metrics of the BigQueryIO connector, see [BigQueryIO Performance](https://beam.apache.org/performance/bigquery/).
