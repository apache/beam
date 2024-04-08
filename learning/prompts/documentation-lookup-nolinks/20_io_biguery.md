Prompt:
Is BigQuery supported in Apache Beam?

Response:
Yes, Apache Beam supports BigQuery. BigQuery is a serverless and cost-effective enterprise data warehouse offered by Google Cloud. Apache Beam provides a BigQueryIO connector to read and write data from and to BigQuery. The BigQueryIO connector supports both batch and streaming pipelines.

The following Apache Beam SDKs support the BigQueryIO connector:
* Java (natively)
* Python (natively)
* Go (natively and through X Language)
* Typescript (through X Language)

To read data from BigQuery, use the `ReadFromBigQuery` function. Apache Beam can read data directly from a BigQuery table or using an SQL query. The default mode is to return table rows read from a BigQuery source as dictionaries. Built-in `TableRow` objects can also be returned.

Here is an example of Apache Beam pipeline code for reading from BigQuery:

```python
from apache_beam.io.gcp.bigquery import ReadFromBigQuery

with beam.Pipeline(options=options) as p:
    # read from a table
    lines_table = p | "Read" >> ReadFromBigQuery(table=table)
    # read from a query
    lines_query = p | "Read" >> ReadFromBigQuery(query="SELECT * FROM table")
```

Here is an example of Apache Beam pipeline code for writing to BigQuery:

```python
from apache_beam.io.gcp.bigquery import WriteToBigQuery

with beam.Pipeline(options=options) as p:
    # write to a table
    p | "Write" >> beam.io.WriteToBigQuery(
        table,
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )
```

To get started with BigQuery and Apache Beam, see the examples in the Dataflow Cookbook GitHub repository.
