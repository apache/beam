# HDFS IO

This library provides HDFS sources and sinks to make it possible to read and
write Apache Hadoop file formats from Apache Beam pipelines.

Currently, only the read path is implemented. A `HDFSFileSource` allows any
Hadoop `FileInputFormat` to be read as a `PCollection`.

A `HDFSFileSource` can be read from using the
`org.apache.beam.sdk.io.Read` transform. For example:

```java
HDFSFileSource<K, V> source = HDFSFileSource.from(path, MyInputFormat.class,
  MyKey.class, MyValue.class);
PCollection<KV<MyKey, MyValue>> records = Read.from(mySource);
```

Alternatively, the `readFrom` method is a convenience method that returns a read
transform. For example:

```java
PCollection<KV<MyKey, MyValue>> records = HDFSFileSource.readFrom(path,
  MyInputFormat.class, MyKey.class, MyValue.class);
```
