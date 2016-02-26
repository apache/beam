# Hadoop module

This library provides Dataflow sources and sinks to make it possible to read and
write Apache Hadoop file formats from Dataflow pipelines.

Currently, only the read path is implemented. A `HadoopFileSource` allows any
Hadoop `FileInputFormat` to be read as a `PCollection`.

A `HadoopFileSource` can be read from using the
`com.google.cloud.dataflow.sdk.io.Read` transform. For example:

```java
HadoopFileSource<K, V> source = HadoopFileSource.from(path, MyInputFormat.class,
  MyKey.class, MyValue.class);
PCollection<KV<MyKey, MyValue>> records = Read.from(mySource);
```

Alternatively, the `readFrom` method is a convenience method that returns a read
transform. For example:

```java
PCollection<KV<MyKey, MyValue>> records = HadoopFileSource.readFrom(path,
  MyInputFormat.class, MyKey.class, MyValue.class);
```
