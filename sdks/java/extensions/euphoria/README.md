# Euphoria

[![Build Status](https://travis-ci.org/seznam/euphoria.svg?branch=master)](https://travis-ci.org/seznam/euphoria)
[![Join the chat at https://gitter.im/euphoria-api/Lobby](https://badges.gitter.im/euphoira-api/Lobby.svg)](https://gitter.im/euphoria-api/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

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

## Download

The best way to use Euphoria is by adding the following Maven dependency to your _pom.xml_:

```xml
<dependency>
  <groupId>cz.seznam.euphoria</groupId>
  <artifactId>euphoria-core</artifactId>
  <version>0.5.0</version>
</dependency>
```
You may want to add additional modules, such as support of various engines or I/O data sources/sinks. For more details read the [Maven Dependencies](https://github.com/seznam/euphoria/wiki/Maven-dependencies) wiki page.


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

## Bugs / Features / Contributing

There's still a lot of room for improvements and extensions.  Have a
look into the [issue tracker](https://github.com/seznam/euphoria/issues)
and feel free to contribute by reporting new problems, contributing to
existing ones, or even open issues in case of questions.  Any constructive
feedback is warmly welcome!

As usually with open source, don't hesitate to fork the repo and
submit a pull requests if you see something to be changed.  We'll be
happy see euphoria improving over time.

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

For faster local development cycles, building javadocs, preparing
source code packages, running project and source code validations can
be avoided by de-activating the "regular-build" profile:

```
mvn clean install -DskipTests -P-regular-build
```

This is just a convenient, quick and dirty way to install the binaries
locally without going through the full build cycle.  However, pull
requests which do not pass the full build lifecycle as defined by the
"regular-build" profile cannot be accepted.


## Documentation

* Documentation is currently maintained in the form of a
  [Wiki on Github](https://github.com/seznam/euphoria/wiki). A lot of work
  in this area is still to be done.

* Another source of documentation are deliberately simple examples
  maintained in the [euphoria-examples module](https://github.com/seznam/euphoria/tree/master/euphoria-examples).


## License

Euphoria is licensed under the terms of the Apache License 2.0.
