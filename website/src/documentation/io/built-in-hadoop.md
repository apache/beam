---
layout: section
title: "Apache Hadoop InputFormat IO"
section_menu: section-menu/documentation.html
permalink: /documentation/io/built-in/hadoop/
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

# Hadoop InputFormat IO

A `HadoopInputFormatIO` is a transform for reading data from any source that implements Hadoop's `InputFormat`. For example, Cassandra, Elasticsearch, HBase, Redis, Postgres, etc.

`HadoopInputFormatIO` allows you to connect to many data sources that do not yet have a Beam IO transform. However, `HadoopInputFormatIO` has to make several performance trade-offs in connecting to `InputFormat`. So, if there is another Beam IO transform for connecting specifically to your data source of choice, we recommend you use that one.

You will need to pass a Hadoop `Configuration` with parameters specifying how the read will occur. Many properties of the `Configuration` are optional and some are required for certain `InputFormat` classes, but the following properties must be set for all `InputFormat` classes:

- `mapreduce.job.inputformat.class` - The `InputFormat` class used to connect to your data source of choice.
- `key.class` - The `Key` class returned by the `InputFormat` in `mapreduce.job.inputformat.class`.
- `value.class` - The `Value` class returned by the `InputFormat` in `mapreduce.job.inputformat.class`.

For example:
```java
Configuration myHadoopConfiguration = new Configuration(false);
// Set Hadoop InputFormat, key and value class in configuration
myHadoopConfiguration.setClass("mapreduce.job.inputformat.class", InputFormatClass,
  InputFormat.class);
myHadoopConfiguration.setClass("key.class", InputFormatKeyClass, Object.class);
myHadoopConfiguration.setClass("value.class", InputFormatValueClass, Object.class);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

You will need to check if the `Key` and `Value` classes output by the `InputFormat` have a Beam `Coder` available. If not, you can use `withKeyTranslation` or `withValueTranslation` to specify a method transforming instances of those classes into another class that is supported by a Beam `Coder`. These settings are optional and you don't need to specify translation for both key and value.

For example:
```java
SimpleFunction<InputFormatKeyClass, MyKeyClass> myOutputKeyType =
new SimpleFunction<InputFormatKeyClass, MyKeyClass>() {
  public MyKeyClass apply(InputFormatKeyClass input) {
  // ...logic to transform InputFormatKeyClass to MyKeyClass
  }
};
SimpleFunction<InputFormatValueClass, MyValueClass> myOutputValueType =
new SimpleFunction<InputFormatValueClass, MyValueClass>() {
  public MyValueClass apply(InputFormatValueClass input) {
  // ...logic to transform InputFormatValueClass to MyValueClass
  }
};
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

### Reading using Hadoop InputFormat IO

#### Read data only with Hadoop configuration.

```java
p.apply("read",
  HadoopInputFormatIO.<InputFormatKeyClass, InputFormatKeyClass>read()
  .withConfiguration(myHadoopConfiguration);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

#### Read data with configuration and key translation

For example, a Beam `Coder` is not available for `Key` class, so key translation is required.

```java
p.apply("read",
  HadoopInputFormatIO.<MyKeyClass, InputFormatKeyClass>read()
  .withConfiguration(myHadoopConfiguration)
  .withKeyTranslation(myOutputKeyType);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

#### Read data with configuration and value translation

For example, a Beam `Coder` is not available for `Value` class, so value translation is required.

```java
p.apply("read",
  HadoopInputFormatIO.<InputFormatKeyClass, MyValueClass>read()
  .withConfiguration(myHadoopConfiguration)
  .withValueTranslation(myOutputValueType);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

#### Read data with configuration, value translation and key translation

For example, Beam Coders are not available for both `Key` class and `Value` classes of `InputFormat`, so key and value translation are required.

```java
p.apply("read",
  HadoopInputFormatIO.<MyKeyClass, MyValueClass>read()
  .withConfiguration(myHadoopConfiguration)
  .withKeyTranslation(myOutputKeyType)
  .withValueTranslation(myOutputValueType);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

# Examples for specific InputFormats

### Cassandra - CqlInputFormat

To read data from Cassandra, use `org.apache.cassandra.hadoop.cql3.CqlInputFormat`, which needs the following properties to be set:

```java
Configuration cassandraConf = new Configuration();
cassandraConf.set("cassandra.input.thrift.port", "9160");
cassandraConf.set("cassandra.input.thrift.address", CassandraHostIp);
cassandraConf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
cassandraConf.set("cassandra.input.keyspace", "myKeySpace");
cassandraConf.set("cassandra.input.columnfamily", "myColumnFamily");
cassandraConf.setClass("key.class", java.lang.Long Long.class, Object.class);
cassandraConf.setClass("value.class", com.datastax.driver.core.Row Row.class, Object.class);
cassandraConf.setClass("mapreduce.job.inputformat.class", org.apache.cassandra.hadoop.cql3.CqlInputFormat CqlInputFormat.class, InputFormat.class);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

Call Read transform as follows:

```java
PCollection<KV<Long, String>> cassandraData =
  p.apply("read",
  HadoopInputFormatIO.<Long, String>read()
  .withConfiguration(cassandraConf)
  .withValueTranslation(cassandraOutputValueType);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

The `CqlInputFormat` key class is `java.lang.Long` `Long`, which has a Beam `Coder`. The `CqlInputFormat` value class is `com.datastax.driver.core.Row` `Row`, which does not have a Beam `Coder`. Rather than write a new coder, you can provide your own translation method, as follows:

```java
SimpleFunction<Row, String> cassandraOutputValueType = SimpleFunction<Row, String>()
{
  public String apply(Row row) {
    return row.getString('myColName');
  }
};
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

### Elasticsearch - EsInputFormat

To read data from Elasticsearch, use `EsInputFormat`, which needs following properties to be set:

```java
Configuration elasticSearchConf = new Configuration();
elasticSearchConf.set("es.nodes", ElasticsearchHostIp);
elasticSearchConf.set("es.port", "9200");
elasticSearchConf.set("es.resource", "ElasticIndexName/ElasticTypeName");
elasticSearchConf.setClass("key.class", org.apache.hadoop.io.Text Text.class, Object.class);
elasticSearchConf.setClass("value.class", org.elasticsearch.hadoop.mr.LinkedMapWritable LinkedMapWritable.class, Object.class);
elasticSearchConf.setClass("mapreduce.job.inputformat.class", org.elasticsearch.hadoop.mr.EsInputFormat EsInputFormat.class, InputFormat.class);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

Call Read transform as follows:

```java
PCollection<KV<Text, LinkedMapWritable>> elasticData = p.apply("read",
  HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(elasticSearchConf));
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

The `org.elasticsearch.hadoop.mr.EsInputFormat`'s `EsInputFormat` key class is `org.apache.hadoop.io.Text` `Text`, and its value class is `org.elasticsearch.hadoop.mr.LinkedMapWritable` `LinkedMapWritable`. Both key and value classes have Beam Coders.

### HCatalog - HCatInputFormat

To read data using HCatalog, use `org.apache.hive.hcatalog.mapreduce.HCatInputFormat`, which needs the following properties to be set:

```java
Configuration hcatConf = new Configuration();
hcatConf.setClass("mapreduce.job.inputformat.class", HCatInputFormat.class, InputFormat.class);
hcatConf.setClass("key.class", LongWritable.class, Object.class);
hcatConf.setClass("value.class", HCatRecord.class, Object.class);
hcatConf.set("hive.metastore.uris", "thrift://metastore-host:port");

org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(hcatConf, "my_database", "my_table", "my_filter");
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

Call Read transform as follows:

```java
PCollection<KV<Long, HCatRecord>> hcatData =
  p.apply("read",
  HadoopInputFormatIO.<Long, HCatRecord>read()
  .withConfiguration(hcatConf);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

### Amazon DynamoDB - DynamoDBInputFormat

To read data from Amazon DynamoDB, use `org.apache.hadoop.dynamodb.read.DynamoDBInputFormat`.
DynamoDBInputFormat implements the older `org.apache.hadoop.mapred.InputFormat` interface and to make it compatible with HadoopInputFormatIO which uses the newer abstract class `org.apache.hadoop.mapreduce.InputFormat`,
a wrapper API is required which acts as an adapter between HadoopInputFormatIO and DynamoDBInputFormat (or in general any InputFormat implementing `org.apache.hadoop.mapred.InputFormat`)
The below example uses one such available wrapper API - <https://github.com/twitter/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/mapreduce/input/MapReduceInputFormatWrapper.java>


```java
Configuration dynamoDBConf = new Configuration();
Job job = Job.getInstance(dynamoDBConf);
com.twitter.elephantbird.mapreduce.input.MapReduceInputFormatWrapper.setInputFormat(org.apache.hadoop.dynamodb.read.DynamoDBInputFormat.class, job);
dynamoDBConf = job.getConfiguration();
dynamoDBConf.setClass("key.class", Text.class, WritableComparable.class);
dynamoDBConf.setClass("value.class", org.apache.hadoop.dynamodb.DynamoDBItemWritable.class, Writable.class);
dynamoDBConf.set("dynamodb.servicename", "dynamodb");
dynamoDBConf.set("dynamodb.input.tableName", "table_name");
dynamoDBConf.set("dynamodb.endpoint", "dynamodb.us-west-1.amazonaws.com");
dynamoDBConf.set("dynamodb.regionid", "us-west-1");
dynamoDBConf.set("dynamodb.throughput.read", "1");
dynamoDBConf.set("dynamodb.throughput.read.percent", "1");
dynamoDBConf.set("dynamodb.version", "2011-12-05");
dynamoDBConf.set(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, "aws_access_key");
dynamoDBConf.set(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, "aws_secret_key");
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

Call Read transform as follows:

```java
PCollection<Text, DynamoDBItemWritable> dynamoDBData =
  p.apply("read",
  HadoopInputFormatIO.<Text, DynamoDBItemWritable>read()
  .withConfiguration(dynamoDBConf);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

### Apache HBase - TableSnapshotInputFormat

To read data from an HBase table snapshot, use `org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat`.
Reading from a table snapshot bypasses the HBase region servers, instead reading HBase data files directly from the filesystem.
This is useful for cases such as reading historical data or offloading of work from the HBase cluster. 
There are scenarios when this may prove faster than accessing content through the region servers using the `HBaseIO`.

A table snapshot can be taken using the HBase shell or programmatically:
```java
try (
    Connection connection = ConnectionFactory.createConnection(hbaseConf);
    Admin admin = connection.getAdmin()
  ) {
  admin.snapshot(
    "my_snaphshot",
    TableName.valueOf("my_table"),
    HBaseProtos.SnapshotDescription.Type.FLUSH);
}  
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

A `TableSnapshotInputFormat` is configured as follows:

```java
// Construct a typical HBase scan
Scan scan = new Scan();
scan.setCaching(1000);
scan.setBatch(1000);
scan.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("col_1"));
scan.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("col_2"));

Configuration hbaseConf = HBaseConfiguration.create();
hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "zk1:2181");
hbaseConf.set("hbase.rootdir", "/hbase");
hbaseConf.setClass(
    "mapreduce.job.inputformat.class", TableSnapshotInputFormat.class, InputFormat.class);
hbaseConf.setClass("key.class", ImmutableBytesWritable.class, Writable.class);
hbaseConf.setClass("value.class", Result.class, Writable.class);
ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));

// Make use of existing utility methods
Job job = Job.getInstance(hbaseConf); // creates internal clone of hbaseConf
TableSnapshotInputFormat.setInput(job, "my_snapshot", new Path("/tmp/snapshot_restore"));
hbaseConf = job.getConfiguration(); // extract the modified clone
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```

Call Read transform as follows:

```java
PCollection<ImmutableBytesWritable, Result> hbaseSnapshotData =
  p.apply("read",
  HadoopInputFormatIO.<ImmutableBytesWritable, Result>read()
  .withConfiguration(hbaseConf);
```

```py
  # The Beam SDK for Python does not support Hadoop InputFormat IO.
```
