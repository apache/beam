---
layout: section
title: "Euphoria Java 8 DSL"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/java/euphoria/
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
# Euphoria Java 8 DSL

## What is Euphoria

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
Dataset<Pair<String, Long>> counted = ReduceByKey.named("COUNT")
    .of(words)
    .keyBy(w -> w)
    .valueBy(w -> 1L)
    .combineBy(Sums.ofLongs())
    .output();

// Format output.
Dataset<String> output = MapElements.named("FORMAT")
    .of(counted)
    .using(p -> p.getFirst() + ": " + p.getSecond())
    .output();

// Transform Dataset back to PCollection. It can be done in any step of this flow.
PCollection<String> outputCollection = flow.unwrapped(output);

// Now we can again use Beam transformation. In this case we save words and their count
// into the text file.
outputCollection.apply(TextIO.write().to(options.getOutput()));

pipeline.run();
```



