Prompt:
Is the Google Cloud Storage file system supported in Apache Beam?

Response:
[Google Cloud Storage](https://cloud.google.com/storage) is an object storage service for storing and accessing data on Google Cloud. Apache Beam supports both working with the Google Cloud Storage file system and reading data from and writing data to Google Cloud Storage buckets.

Apache Beam has a built-in TextIO connector for reading data from and writing data to Google Cloud Storage buckets. For more information, see the TextIO connector documentation for your programming language of choice:
* [Java connector](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.html)
* [Python connector](https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html)
* [Go connector](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio)
* [Typescript connector (using cross-language transforms)](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/textio.ts)

The Apache Beam AvroIO, XMLIO, TFRecordIO, and ParquetIO connectors also support reading data from and writing data to different file formats within Google Cloud Storage buckets.

You can specify the Google Cloud Storage file path using the following format: `gs://<bucket>/<path>`. For example, `gs://my-bucket/my-file.txt`.

You can use wildcards in read and write transforms to process data from multiple files or write data to multiple files simultaneously. For instance, you can specify `gs://my-bucket/my-files-*.txt` to read or write to files with names matching the specified pattern.

Apache Beam also allows pipelines to interact directly with the Google Cloud Storage file system. For instance, you can use this file system to verify the existence of a file, retrieve a list of files in a directory, or delete a file. The Google Cloud Storage file system is supported in the following languages:
* [Java file system](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/gcp/storage/GcsFileSystemRegistrar.html)
* [Python file system](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.gcsfilesystem.html)
* [Go file system](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs)
