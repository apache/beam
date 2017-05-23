---
layout: default
title: "Apache Hadoop InputFormat IO"
permalink: /documentation/io/built-in/hadoop/
---

[Pipeline I/O Table of Contents]({{site.baseurl}}/documentation/io/io-toc/)

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