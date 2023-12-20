Prompt:
Is GCS file system supported in Apache Beam?
Response:
[Google Cloud Storage](https://cloud.google.com/storage) is a popular object storage service for storing and accessing data on Google Cloud. Apache Beam supports both working with the GCS file system and reading and writing data from and to GCS buckets.

Apache Beam has a built-in `TextIO` connector for reading and writing data from and to GCS buckets:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio)
* [Typescript via X-language](https://beam.apache.org/documentation/sdks/typescript/io/textio/)

Reading and writing data in different file formats from and to GCS buckets is also supported in the AvroIO, XMLIO, TFRecordIO, and ParquetIO connectors.

GCS path is specified using the following format: `gs://<bucket>/<path>`. For example, `gs://my-bucket/my-file.txt`.
Read and write transforms support reading and writing data from and to multiple files using wildcards. For example, `gs://my-bucket/my-files-*.txt`.

Apache Beam also has support for the GCS file system that allows you to work with the GCS file system directly. For example, you can use GCS file system to check if a file exists, list files in a directory, or delete a file. GCS file system is supported in the following languages:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/gcp/storage/GcsFileSystemRegistrar.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.gcsfilesystem.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs)


