Prompt:
Is Google Cloud Storage file system supported in Apache Beam?

Response:
[Google Cloud Storage](https://cloud.google.com/storage) is an object storage service for storing and accessing data on Google Cloud. Apache Beam supports both working with the Google Cloud Storage file system and reading data from and writing data to Google Cloud Storage buckets.

Apache Beam has a built-in TextIO connector for reading data from and writing data to Google Cloud Storage buckets. For more information, see the TextIO connector documentation:
* [Java connector](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.html)
* [Python connector](https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html)
* [Go connector](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio)
* [Typescript connector (using cross-language transforms)](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/textio.ts)

Reading and writing data in different file formats in Google Cloud Storage buckets is also supported in the AvroIO, XMLIO, TFRecordIO, and ParquetIO connectors.

Specify the Google Cloud Storage path using the following format: `gs://<bucket>/<path>`. For example, `gs://my-bucket/my-file.txt`.
You can use wildcards in read transforms and write transforms to read data from and write data to multiple files. For example, `gs://my-bucket/my-files-*.txt`.

Apache Beam also has support for the Google Cloud Storage file system that lets you work with the Google Cloud Storage file system directly. For example, you can use the Google Cloud Storage file system to check whether a file exists, list files in a directory, or delete a file. The Google Cloud Storage file system is supported in the following languages:
* [Java file system](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/gcp/storage/GcsFileSystemRegistrar.html)
* [Python file system](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.gcsfilesystem.html)
* [Go file system](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs)


