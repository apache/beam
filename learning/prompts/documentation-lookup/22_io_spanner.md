Prompt:
Is Cloud Spanner supported in Apache Beam?

Response:
[Cloud Spanner](https://cloud.google.com/spanner) is a [Google Cloud](https://cloud.google.com/) service that provides a fully managed, mission-critical, relational database service that offers transactional consistency at global scale, schemas, SQL (ANSI 2011 with extensions), and automatic, synchronous replication for high availability.

Apache Beam has a built-in SpannerIO connector for Cloud Spanner that lets you read data from and write data to Cloud Spanner. Cloud Spanner is supported in both batch pipelines and streaming pipelines, and as a source and a sink. For more information, see the SpannerIO connector documentation:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.html)
* [Python (using cross-language transforms)](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.spanner.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/spannerio)

The [Dataflow cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) can help you to get started with Cloud Spanner and Apache Beam.

Apache Beam pipeline code for reading data from a Cloud Spanner table using a SQL query might look like the following example:

```python
class ExampleRow(NamedTuple):
    id: int
    name: str

with beam.Pipeline(options=options) as p:
    output = (p | "Read from table" >> ReadFromSpanner(
        project_id=options.project_id,
        instance_id=options.instance_id,
        database_id=options.database_id,
        row_type=ExampleRow,
        sql="SELECT * FROM example_row"
        )
        | "Map Data" >> Map(lambda row: f"Id = {row.id}, Name = {row.name}")
        | "Log Data" >> Map(logging.info))
```
