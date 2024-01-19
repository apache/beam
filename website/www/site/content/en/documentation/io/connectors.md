---
title: "I/O Connectors"
aliases: [built-in]
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# I/O Connectors

Apache Beam I/O connectors provide read and write transforms for the most popular data storage systems so that Beam users can benefit from native optimised connectivity.  With the available I/Os, Apache Beam pipelines can read and write data from and to an external storage type in a unified and distributed way.

I/O connectors denoted _via X-language_ have been made available using the Apache Beam [multi-language pipelines framework](/documentation/programming-guide/#multi-language-pipelines).

## Built-in I/O Connectors

This table provides a consolidated, at-a-glance overview of the available built-in I/O connectors.
<div class="table-container-wrapper">
  <table class="table table-bordered table-connectors">
  <tr>
    <th>Connector Name</th>
    <th>Source Supported</th>
    <th>Sink Supported</th>
    <th>Java</th>
    <th>Python</th>
    <th>Go</th>
    <th>Typescript</th>
    <th>Batch Supported</th>
    <th>Streaming Supported</th>
  </tr>
  <tr>
    <td>FileIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/FileIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.fileio.html">native</a>
    </td>
     <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/fileio">native</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>AvroIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/avro/io/AvroIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.avroio.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/avroio">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/avroio.ts">via X-language</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>TextIO (<a href="/performance/textio">metrics</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/textio.ts">via X-language</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>TFRecordIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TFRecordIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.tfrecordio.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>XmlIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/xml/XmlIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>TikaIO</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/tika/TikaIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>ParquetIO (<a href="/documentation/io/built-in/parquet/">guide</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/parquet/ParquetIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.parquetio.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/parquetio">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/parquetio.ts">via X-language</a>
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>ThriftIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/thrift/ThriftIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>HadoopFileSystem</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/hdfs/HadoopFileSystemRegistrar.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.hadoopfilesystem.html">native</a>
    </td>
    <td>Not available</td>
    <td class="present">
      ✔via X-language
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>GcsFileSystem (<a href="/performance/textio">metrics</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/gcp/storage/GcsFileSystemRegistrar.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.gcsfilesystem.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs">native</a>
    </td>
    <td class="present">
      ✔via X-language
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>LocalFileSystem</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/LocalFileSystemRegistrar.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.localfilesystem.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local">native</a>
    </td>
    <td class="present">
      ✔via X-language
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>S3FileSystem</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/aws2/s3/S3FileSystemRegistrar.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.aws.s3filesystem.html">native</a>
    </td>
    <td>Not available</td>
    <td class="present">
      ✔via X-language
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>In-memory</td>
    <td class="absent">✘</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/memfs">native</a>
    </td>
    <td class="absent">✘</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>KinesisIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/aws2/kinesis/KinesisIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.kinesis.html#apache_beam.io.kinesis.ReadDataFromKinesis">via X-language</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>AmqpIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/amqp/AmqpIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>KafkaIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.kafka.html">via X-language</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/kafkaio">via X-language</a>
    </td>
    <td class="present">
      ✔
      <a href="https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/kafka.ts">via X-language</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>PubSubIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/pubsub.ts">via X-language</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>JmsIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jms/JmsIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>MqttIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/mqtt/MqttIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>RabbitMqIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/rabbitmq/RabbitMqIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>SqsIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/aws2/sqs/SqsIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>SnsIO</td>
    <td class="absent">✘</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/aws2/sns/SnsIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>CassandraIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/cassandra/CassandraIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>HadoopFormatIO (<a href="/documentation/io/built-in/hadoop/">guide</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/hadoop/format/HadoopFormatIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>HBaseIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/hbase/HBaseIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>HCatalogIO (<a href="/documentation/io/built-in/hcatalog/">guide</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/hcatalog/HCatalogIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>KuduIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kudu/KuduIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>SolrIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/solr/SolrIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>ElasticsearchIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/elasticsearch/ElasticsearchIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>BigQueryIO (<a href="/documentation/io/built-in/google-bigquery/">guide</a>) (<a href="/performance/bigquery">metrics</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigquery.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio">native</a>
      <br>
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigqueryio">via X-language</a>
    </td>
    <td class="present">
      ✔
      <a href="https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/bigqueryio.ts">via X-language</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>BigTableIO (<a href="/performance/bigtable">metrics</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigtable/BigtableIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html">native</a> (sink)
      <br>
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html">via X-language</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigtableio">native</a> (sink)
      <br>
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigtableio">via X-language</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>DatastoreIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/datastore/DatastoreIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.datastore.v1new.datastoreio.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/datastoreio">native</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>SnowflakeIO (<a href="/documentation/io/built-in/snowflake">guide</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/snowflake/SnowflakeIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.snowflake.html">via X-language</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>SpannerIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.spanner.html">via X-language</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/spannerio">native</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>JdbcIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.jdbc.html">via X-language</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/jdbcio">via X-language</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>DebeziumIO</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/debezium/DebeziumIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.debezium.html">via X-language</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/debeziumio">via X-language</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>MongoDbIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/mongodb/MongoDbIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.mongodbio.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/mongodbio">native</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>MongoDbGridFSIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/mongodb/MongoDbGridFSIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>RedisIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/redis/RedisIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>DynamoDBIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/aws2/dynamodb/DynamoDBIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
     <td>Not available</td>
   <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>ClickHouseIO</td>
    <td class="absent">✘</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/clickhouse/ClickHouseIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>DatabaseIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/databaseio">native</a>
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>GenerateSequence</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/GenerateSequence.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>SplunkIO</td>
    <td class="absent">✘</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/splunk/SplunkIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>FhirIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/healthcare/FhirIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/fhirio">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>HL7v2IO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/healthcare/HL7v2IO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>DicomIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/healthcare/DicomIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.healthcare.dicomio.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>
      FlinkStreaming<br>ImpulseSource
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td>Not available</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.flink.flink_streaming_impulse_source.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>Firestore IO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/firestore/FirestoreIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>Neo4j</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>Pub/Sub Lite</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsublite/PubsubLiteIO.html">native</a>
    </td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsublite.html">via X-language</a>
    </td>
    <td>Not available</td>
    <td class="present">
      ✔
      <a href="https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/pubsublite.ts">via X-language</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>InfluxDB</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/influxdb/InfluxDbIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>SparkReceiverIO (<a href="/documentation/io/built-in/sparkreceiver/">guide</a>)</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/sparkreceiver/SparkReceiverIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="absent">✘</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>CdapIO (<a href="/documentation/io/built-in/cdap/">guide</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/cdap/CdapIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>SingleStoreDB (<a href="/documentation/io/built-in/singlestore/">guide</a>)</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/singlestore/SingleStoreIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>GoogleAdsIO</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/googleads/GoogleAdsIO.html">native</a>
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
</table>
</div>

## Other I/O Connectors for Apache Beam

<div class="table-container-wrapper">
  <table class="table table-bordered table-connectors">
  <tr>
    <th>Connector Name</th>
    <th>Source Supported</th>
    <th>Sink Supported</th>
    <th>Java</th>
    <th>Python</th>
    <th>Go</th>
    <th>Typescript</th>
    <th>Batch Supported</th>
    <th>Streaming Supported</th>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/SolaceProducts/solace-apache-beam">Solace</a>
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/google/hana-bq-beam-connector">SAP Hana to Google BigQuery</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/esakik/beam-mysql-connector">MySQL</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td>Not available</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/Refinitiv/trep-websockets-beam-io">TrepWsIO</a>
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/kineticadb/kinetica-connector-beam">KineticaDB</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/cognitedata/cdf-beam-connector-java">Cognite Data Fusion</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/314e/beam-pyodbc-connector">Pyodbc</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td>Not available</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/amient/goconnect">Go Connect</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td class="absent">✘</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/tinybirdco/tinybird-beam">Tinybird</a>
    </td>
    <td class="absent">✘</td>
    <td class="present">✔</td>
    <td>Not available</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="present">✔</td>
  </tr>
  <tr>
    <td>
      <a href="https://github.com/jccatrinck/dataflow-cloud-sql-python">Cloud SQL</a>
    </td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
    <td>Not available</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
  <tr>
    <td>
      <a href="https://cloud.google.com/bigtable/docs/hbase-dataflow-java">Cloud Bigtable (HBase based)</a>
    </td>
    <td class="present">✔</td>
    <td class="present">✔</td>
    <td class="present">
      ✔
      native
    </td>
    <td>Not available</td>
    <td>Not available</td>
    <td>Not available</td>
    <td class="present">✔</td>
    <td class="absent">✘</td>
  </tr>
</table>
</div>
