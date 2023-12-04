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
This code uses Apache Beam [BigQueryIO](https://beam.apache.org/documentation/io/built-in/google-bigquery/) connector to read data from a [BigQuery](https://cloud.google.com/bigquery/docs) table `table`.

The BigQueryIO connector supports reading from BigQuery tables, executing queries, and reading from BigQuery Storage API (beta). The BigQueryIO connector supports writing to BigQuery tables in batch and streaming modes.

```python
output = (p | "Read table" >> ReadFromBigQuery(table=options.table))
```
The `ReadFromBigQuery` transform uses a BigQuery export job to take a snapshot of the table on GCS, and then reads from each produced file. File format is Avro by default.

Another option would be to use a query to read data from BigQuery.

```python
output = (p | "Read table" >> ReadFromBigQuery(query="SELECT * FROM table"))
```
For more information on how to use BigQueryIO connector with Apache Beam Python SDK see [here](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html).

This code uses [Pipeline option pattern](https://beam.apache.org/documentation/patterns/pipeline-options/) for a requred `table` argument. The `table` argument is used to specify the BigQuery table to read data from.

For performance metrics of BigQueryIO connector, see [BigQueryIO Performance](https://beam.apache.org/performance/bigquery/).


