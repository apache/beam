Prompt:
Is Cloud Bigtable supported in Apache Beam?

Response:
Cloud Bigtable is a Google Cloud service that provides enterprise-grade NoSQL database service with single-digit millisecond latency, limitless scale, and 99.999% availability for large analytical and operational workloads.

Apache Beam has a built-in connector for Cloud Bigtable that lets you read data from and write data to Bigtable. For more information, see the BigtableIO connector documentation for your preferred programming language.

To get started with Cloud Bigtable and Apache Beam, refer to the Dataflow Cookbook GitHub repository.

Here is an example of Apache Beam pipeline code for reading data from a Bigtable table:

```python
with beam.Pipeline(options=options) as p:
    output = (
        p
        | "Read from Bigtable"
        >> ReadFromBigtable(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
        )
        | "Extract cells" >> beam.Map(lambda row: row._cells)
        | "Log Data" >> Map(logging.info)
    )
```

For performance benchmarks of the Bigtable IO connector, see the 'Bigtable performance' section on the Apache Beam website.
