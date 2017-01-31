# Euphoria

Euphoria is an open source Java API for creating unified big-data
processing flows.  It provides an engine independent programming model
that can express both batch and stream transformations.

The main goal of the API is to ease the creation of programs with
business logic independent of a specific runtime framework/engine and
independent of the source or destination of the processed data.  Such
programs are then transferable with little effort to new environments
and new data sources or destinations - idealy just by configuration.


## Key features

 * Unified API that supports both batch and stream processing using
   the same code
 * Avoids vendor lock-in - migrating between different engines is
   matter of configuration
 * Declarative Java API using Java 8 Lamda expressions
 * Support for different notions of time (_event time, ingestion
   time_)
 * Flexible windowing (_Time, TimeSliding, Session, Count_)


## WordCount example

```java
// Define data source and data sinks
DataSource<String> dataSource = new SimpleHadoopTextFileSource(inputPath);
DataSink<String> dataSink = new SimpleHadoopTextFileSink<>(outputPath);

// Define a flow, i.e. a chain of transformations
Flow flow = Flow.create("WordCount");

Dataset<String> lines = flow.createInput(dataSource);

Dataset<String> words = FlatMap.named("TOKENIZER")
    .of(lines)
    .using((String line, Context<String> context) -> {
      for (String word : line.split("\\s+")) {
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

MapElements.named("FORMAT")
    .of(counted)
    .using(p -> p.getFirst() + "\n" + p.getSecond())
    .output()
    .persist(dataSink);

// Initialize an executor and run the flow (using Apache Flink)
Executor executor = new FlinkExecutor();
executor.submit(flow).get();
```

## Supported Engines

Euphoria translates flows, also known as data transformation
pipelines, into the specific API of a chosen, supported big-data
processing engine.  Currently, the following are supported:

 * [Apache Flink](https://flink.apache.org/)
 * [Apache Spark](http://spark.apache.org/)
 * An independent, standalone, in-memory engine which is part of the
   Euphoria project suitable for running flows in unit tests.

In the WordCount example from above, to switch the execution engine
from Apache Flink to Apache Spark, we'd merely need to replace
`FlinkExecutor` with `SparkExecutor`.


## Building

To build the Euphoria artifacts, the following is required:

* Git
* Maven 3
* Java 8

Building the project itself is a matter of:

```
git clone https://github.com/seznam/euphoria
cd euphoria
mvn clean install -DskipTests
```


## Documentation

* Documentation is currently maintained in the form of a
  [Wiki on Github](https://github.com/seznam/euphoria/wiki). A lot of work
  in this area is still to be done.

* Another source of documentation are deliberately simple examples
  maintained in the [euphoria-examples module](https://github.com/seznam/euphoria/tree/master/euphoria-examples).


## License

Euphoria is licensed under the terms of the Apache License 2.0.
