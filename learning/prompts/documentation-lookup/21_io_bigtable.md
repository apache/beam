Prompt:
Is Cloud BigTable supported in Apache Beam?
Response:
[Cloud Bigtable](https://cloud.google.com/bigtable) is a [Google Cloud](https://cloud.google.com/) service that provides  enterprise-grade NoSQL database service with single-digit millisecond latency, limitless scale, and 99.999% availability for large analytical and operational workloads.

Apache Beam has a built-in connector for Cloud Bigtable that allows you to read and write data from and to Bigtable:  
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigtable/BigtableIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html)
* [Python via X lang](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigtableio)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigtableio)
* [Go via X lang](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigtableio)

[Dataflow-cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) will help you to get started with Cloud Bigtable and Apache Beam.

Apache Beam pipeline code for reading data from Bigtable table might look like the following example:

```python
with beam.Pipeline(options=options) as p:
output = (p | "Read from Bigtable" >> ReadFromBigtable(
                    project_id=project_id,
                    instance_id=instance_id,
                    table_id=table_id,
                 )
                    | "Extract cells" >> beam.Map(lambda row: row._cells)
                    | "Log Data" >> Map(logging.info))
```
For performance benchmarks of Bigtable IO connectors, see [here](https://beam.apache.org/performance/bigtable/).