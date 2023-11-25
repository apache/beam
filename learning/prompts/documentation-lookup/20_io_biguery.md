Prompt:
Is BigQuery supported in Apache Beam?
Response:
[BigQuery](https://cloud.google.com/bigquery) is a[ Google Cloud](https://cloud.google.com/) serverless and cost-effective enterprise data warehouse. Apache Beam provides a BigQueryIO connector to read and write data from and to BigQuery. BigQueryIO supports both batch and streaming pipelines.

BigQueryIO is supported in the following Beam SDKs:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio) native and via [X-language](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigqueryio)
* [Typescript](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/bigqueryio.ts) via X-language

`ReadFromBigQuery` is used to read data from BigQuery. Data can be read from a BigQuery table or using a SQL query. The default mode is to return table rows read from a BigQuery source as dictionaries. Native `TableRow` objects can also be returned if desired.

Reading from BigQuery in its simplest form could be something like:

```python
from apache_beam.io.gcp.bigquery import ReadFromBigQuery

with beam.Pipeline(options=options) as p:
  # read from a table
    lines_table = p | 'Read' >> ReadFromBigQuery(table=table)
  # read from a query
    lines_query = p | 'Read' >> ReadFromBigQuery(query="SELECT * FROM table")

```
Writing to BigQuery in its simplest form could be something like:

```python
from apache_beam.io.gcp.bigquery import WriteToBigQuery

with beam.Pipeline(options=options) as p:
  # write to a table
    p | 'Write' >> beam.io.WriteToBigQuery(
        table,
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
```
[Dataflow-cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) will help you to get started with BigQuery and Apache Beam. See here for [read](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/bigquery/read_table_bigquery.py) and [write](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/bigquery/write_bigquery.py) examples in Python.
