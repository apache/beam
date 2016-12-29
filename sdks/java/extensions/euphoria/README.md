# Euphoria

Euphoria is an open source API for creating unified big-data processing flows. It provides an engine independent programming model that can express both batch and stream transformations.

## Key features

 * Unified API that supports both batch and stream processing using the same code
 * Avoids vendor lock-in - migrating between different engines is matter of configuration
 * Fluent Java API using Java 8 Lamda expressions
 * Support for different notions of time (_event time, ingestion time_)
 * Flexible windowing (_Time, Session, TimeSliding, Count_)

## Supported Engines

Euphoria translates the flow you define into the specific API of supported big-data processing engines. Currently the following engines are supported:

 * [Apache Flink](https://flink.apache.org/)
 * [Apache Spark](http://spark.apache.org/)

## Basic concepts

 * __Dataset__: Represents a logical view on distributed data. A data set can be _bounded_  or _unbounded_
 * __DataSource__: Creates a data set from a given input (local/remote file, cloud storage, network stream)
 * __DataSink__: Writes the content of a data set to a given output.
 * __Flow__: Chain of transformations applied on a data set. Typically forms a _direct acyclic graph (DAG)_ starting with one or more data sources while ending in one or more data sinks.

## Getting started

### Maven dependencies

TODO

### Running example

```java
// Define data source and data sinks
// Euphoria is fully compatible with Hadoop input formats
Configuration conf = new Configuration();
conf.set(FileInputFormat.INPUT_DIR, inputPath);
DataSource<Pair<LongWritable, Text>> dataSource = new HadoopDataSource<>(
        LongWritable.class, // key class
        Text.class, // value class
        TextInputFormat.class, // input format class
        conf);// configuration

conf = new Configuration();
conf.set(FileOutputFormat.OUTDIR, outputPath);
DataSink<Pair<Text, NullWritable>> dataSink =
        new HadoopDataSink<>((Class) TextOutputFormat.class, conf);

// Define chain of transformations (flow)
Flow flow = Flow.create("WordCount");

Dataset<Pair<LongWritable, Text>> lines = flow.createInput(dataSource);

Dataset<String> words = FlatMap.named("TOKENIZER")
        .of(lines)
        .using((Pair<LongWritable, Text> line, Context<String> context) -> {
          for (String word : line.getSecond().toString().split("\\s+")) {
            context.collect(word);
          }
        })
        .output();

Dataset<Pair<String, Long>> counted = ReduceByKey.named("COUNT")
        .of(words)
        .keyBy(w -> w)
        .valueBy(w -> 1L)
        .combineBy(Sums.ofLongs())
        .output();

MapElements.named("FORMAT-OUTPUT")
           .of(counted)
           .using(p -> Pair.of(new Text(p.getFirst() + "\n" + p.getSecond()), NullWritable.get()))
           .output()
           .persist(dataSink);

// initialize executor and run existing flow
Executor executor = new FlinkExecutor();
executor.submit(flow).get();
```

### Building from source

To build the latest devel version of Euphoria use following commands

```
git clone XXX

cd euphoria
mvn clean install -DskipTests
```

