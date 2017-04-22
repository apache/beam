<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Hadoop InputFormat IO

A HadoopInputFormatIO is a Transform for reading data from any source which
implements Hadoop InputFormat. For example- Cassandra, Elasticsearch, HBase, Redis, Postgres, etc.

HadoopInputFormatIO has to make several performance trade-offs in connecting to InputFormat, so if there is another Beam IO Transform specifically for connecting to your data source of choice, we would recommend using that one, but this IO Transform allows you to connect to many data sources that do not yet have a Beam IO Transform.

You will need to pass a Hadoop Configuration with parameters specifying how the read will occur. Many properties of the Configuration are optional, and some are required for certain InputFormat classes, but the following properties must be set for all InputFormats:

mapreduce.job.inputformat.class: The InputFormat class used to connect to your data source of choice.
key.class: The key class returned by the InputFormat in 'mapreduce.job.inputformat.class'.
value.class: The value class returned by the InputFormat in 'mapreduce.job.inputformat.class'.

For example:
```java
Configuration myHadoopConfiguration = new Configuration(false);
// Set Hadoop InputFormat, key and value class in configuration
myHadoopConfiguration.setClass("mapreduce.job.inputformat.class", InputFormatClass,
  InputFormat.class);
myHadoopConfiguration.setClass("key.class", InputFormatKeyClass, Object.class);
myHadoopConfiguration.setClass("value.class", InputFormatValueClass, Object.class);
```

You will need to check to see if the key and value classes output by the InputFormat have a Beam Coder available. If not, You can use withKeyTranslation/withValueTranslation to specify a method transforming instances of those classes into another class that is supported by a Beam Coder. These settings are optional and you don't need to specify translation for both key and value.

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

### Reading using Hadoop InputFormat IO
Pipeline p = ...; // Create pipeline.
// Read data only with Hadoop configuration.

```java
p.apply("read",
  HadoopInputFormatIO.<InputFormatKeyClass, InputFormatKeyClass>read()
  .withConfiguration(myHadoopConfiguration);
```

// Read data with configuration and key translation (Example scenario: Beam Coder is not
available for key class hence key translation is required.).

```java
p.apply("read",
  HadoopInputFormatIO.<MyKeyClass, InputFormatKeyClass>read()
  .withConfiguration(myHadoopConfiguration)
  .withKeyTranslation(myOutputKeyType);
```

// Read data with configuration and value translation (Example scenario: Beam Coder is not
available for value class hence value translation is required.).

```java
p.apply("read",
  HadoopInputFormatIO.<InputFormatKeyClass, MyValueClass>read()
  .withConfiguration(myHadoopConfiguration)
  .withValueTranslation(myOutputValueType);
```

// Read data with configuration, value translation and key translation (Example scenario: Beam Coders are not available for both key class and value class of InputFormat hence key and value translation is required.).

```java
p.apply("read",
  HadoopInputFormatIO.<MyKeyClass, MyValueClass>read()
  .withConfiguration(myHadoopConfiguration)
  .withKeyTranslation(myOutputKeyType)
  .withValueTranslation(myOutputValueType);
```

# Examples for specific InputFormats

### Cassandra - CqlInputFormat

To read data from Cassandra, org.apache.cassandra.hadoop.cql3.CqlInputFormat
CqlInputFormat can be used which needs following properties to be set.

Create Cassandra Hadoop configuration as follows:

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

Call Read transform as follows:

```java
PCollection<KV<Long, String>> cassandraData =
  p.apply("read",
  HadoopInputFormatIO.<Long, String>read()
  .withConfiguration(cassandraConf)
  .withValueTranslation(cassandraOutputValueType);
```

The CqlInputFormat key class is java.lang.Long Long, which has a Beam Coder. The CqlInputFormat value class is com.datastax.driver.core.Row Row, which does not have a Beam Coder. Rather than write a new coder, you can provide your own translation method as follows:

```java
SimpleFunction<Row, String> cassandraOutputValueType = SimpleFunction<Row, String>()
{
  public String apply(Row row) {
    return row.getString('myColName');
  }
};
```
 
### Elasticsearch - EsInputFormat
 
To read data from Elasticsearch, EsInputFormat can be used which needs following properties to be set.
 
Create ElasticSearch Hadoop configuration as follows:

```java
Configuration elasticSearchConf = new Configuration();
elasticSearchConf.set("es.nodes", ElasticsearchHostIp);
elasticSearchConf.set("es.port", "9200");
elasticSearchConf.set("es.resource", "ElasticIndexName/ElasticTypeName");
elasticSearchConf.setClass("key.class", org.apache.hadoop.io.Text Text.class, Object.class);
elasticSearchConf.setClass("value.class", org.elasticsearch.hadoop.mr.LinkedMapWritable LinkedMapWritable.class, Object.class);
elasticSearchConf.setClass("mapreduce.job.inputformat.class", org.elasticsearch.hadoop.mr.EsInputFormat EsInputFormat.class, InputFormat.class);
```

Call Read transform as follows:

```java
PCollection<KV<Text, LinkedMapWritable>> elasticData = p.apply("read",
  HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(elasticSearchConf));
```

The org.elasticsearch.hadoop.mr.EsInputFormat EsInputFormat key class is
org.apache.hadoop.io.Text Text and value class is org.elasticsearch.hadoop.mr.LinkedMapWritable LinkedMapWritable. Both key and value classes have Beam Coders.