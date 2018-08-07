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


# Euphoria Java 8 DSL

Easy to use Java 8 DSL for the Beam Java SDK. Provides a high-level abstraction of Beam transformations, which is both easy to read and write. Can be used as a complement to existing Beam pipelines (convertible back and forth).

Integration of Euphoria API to Beam is in **progress** ([BEAM-3900](https://issues.apache.org/jira/browse/BEAM-3900)).

## How to build

Euphoria is located in `dsl-euphoria` branch. To build `euphoria` subprojects use command:

```
./gradlew :beam-sdks-java-extensions-euphoria-beam:build 
```

## WordCount example

```java
Pipeline pipeline = Pipeline.create(options);

// Transform to euphoria's flow.
BeamFlow flow = BeamFlow.create(pipeline);

// Source of data loaded from Beam IO.
PCollection<String> input =
    pipeline.apply(Create.of(inputs)).setTypeDescriptor(TypeDescriptor.of(String.class));
// Transform PCollection to euphoria's Dataset.
Dataset<String> lines = flow.wrapped(input);

// FlatMap processes one input element at a time and allows user code to emit
// zero, one, or more output elements. From input lines we will get data set of words.
Dataset<String> words = FlatMap.named("TOKENIZER")
    .of(lines)
    .using((String line, Collector<String> context) -> {
      for (String word : line.split("\\s+")) {
        context.collect(word);
      }
    })
    .output();

// From each input element we extract a key (word) and value, which is the constant `1`.
// Then, we reduce by the key - the operator ensures that all values for the same
// key end up being processed together. It applies user defined function (summing word counts for each
// unique word) and its emitted to output. 
Dataset<KV<String, Long>> counted = ReduceByKey.named("COUNT")
    .of(words)
    .keyBy(w -> w)
    .valueBy(w -> 1L)
    .combineBy(Sums.ofLongs())
    .output();

// Format output.
Dataset<String> output = MapElements.named("FORMAT")
    .of(counted)
    .using(p -> p.getKey() + ": " + p.getValue())
    .output();

// Transform Dataset back to PCollection. It can be done in any step of this flow.
PCollection<String> outputCollection = flow.unwrapped(output);

// Now we can again use Beam transformation. In this case we save words and their count
// into the text file.
outputCollection.apply(TextIO.write().to(options.getOutput()));

pipeline.run();
```



## Key features

 * Unified API that supports both batch and stream processing using
   the same code
 * Avoids vendor lock-in - migrating between different engines is
   matter of configuration
 * Declarative Java API using Java 8 Lambda expressions
 * Support for different notions of time (_event time, ingestion
   time_)
 * Flexible windowing (_Time, TimeSliding, Session, Count_)


## Supported Engines

Euphoria can be translated to Beam so it support same runners as Beam ([more info](https://beam.apache.org/documentation/runners/capability-matrix/))


## Bugs / Features / Contributing

There's still a lot of room for improvements and extensions.  Have a
look into the [issue tracker](https://issues.apache.org/jira/browse/BEAM-3900)
and feel free to contribute by reporting new problems, contributing to
existing ones, or even open issues in case of questions.  Any constructive
feedback is warmly welcome!

As usually with open source, don't hesitate to fork the repo and
submit a pull requests if you see something to be changed.  We'll be
happy see euphoria improving over time.


## Documentation
_In progress_

### Available transformations

- `CountByKey`: Counting elements with same key.
- `Distinct`: Outputting distinct (based on equals method) elements.
- `Join` : Inner join of two datasets by given key producing single new dataset.
- `LeftJoin` : Left outer join of two input datasets producing single new dataset.
- `RightJoin`: Right outer join of two input datasets producing single new dataset.
- `FullJoin`: Full outer join of two input datasets producing single new dataset.
- `MapElements`: Simple one-to-one transformation of input elements.
- `FlatMap` : A transformation of a dataset from one type into another allowing user code to generate zero,
     one, or many output elements for a given input element.
- `Filter` : Output elements that pass given condition.
- `ReduceByKey`:Operator performing state-less aggregation by given reduce function. The reduction is performed
                 on all extracted values on each key-window.
- `ReduceWindow`: Reduces all elements in a window. 
- `SumByKey`: summing of long values extracted from elements.
- `TopPerKey` :  Emits top element for defined keys and windows.
- `Union` : The union of at least two datasets of the same type.
- `AssignEventTime`: A convenient alias for assignment of event time.


Lower level transformations (if possible user should prefer above transformations):
- `ReduceStateByKey`: assigns each input item to a set of windows and turns the item into a key/value pair.
For each of the assigned windows the extracted value is accumulated using a user provided `StateFactory` state
 implementation under the extracted key. I.e. the value is accumulated into a state identified by
 a key/window pair.

## Contact us

* Feel free to open an issue in the [issue tracker](https://issues.apache.org/jira/browse/BEAM-3900)
with filled `dsl-euphoria` component. 

## License

Euphoria is licensed under the terms of the Apache License 2.0.



