---
type: languages
title: "Euphoria Java 8 DSL"
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
<!--
NOTE for future maintainer.
There is [`DocumentationExamplesTest`](/documentation/sdks/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/sdk/extensions/euphoria/core/docs/DocumentationExamplesTest.html) in `beam-sdks-java-extensions-euphoria-core` project where all code examples are validated. Do not change the code examples without reflecting it in the `DocumentationExamplesTest` and vice versa.

Following operator is unsupported. Include it in documentation when supported.

Lower level transformations (if possible user should prefer above transformations):
### `ReduceStateByKey`: assigns each input item to a set of windows and turns the item into a key/value pair.
For each of the assigned windows the extracted value is accumulated using a user provided `StateFactory` state
 implementation under the extracted key. I.e. the value is accumulated into a state identified by
 a key/window pair.
-->

## What is Euphoria
Easy to use Java 8 API build on top of the Beam's Java SDK. API provides a [high-level abstraction](#operator-reference) of data transformations, with focus on the Java 8 language features (e.g. lambdas and streams). It is fully inter-operable with existing Beam SDK and convertible back and forth. It allows fast prototyping through use of (optional) [Kryo](https://github.com/EsotericSoftware/kryo) based coders, lambdas and high level operators and can be seamlessly integrated into existing Beam `Pipelines`.

[Euphoria API](https://github.com/seznam/euphoria) project has been started in 2014, with a clear goal of providing the main building block for [Seznam.cz's](https://www.seznam.cz/) data infrastructure.
In 2015, [DataFlow whitepaper](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf) inspired original authors to go one step further and also provide the unified API for both stream and batch processing.
The API has been open-sourced in 2016 and is still in active development. As the Beam's community goal was very similar, we decided to contribute
the API as a high level DSL over Beam Java SDK and share our effort with the community.

Euphoria DSL integration is still work in progress and is tracked as part of [BEAM-3900](https://issues.apache.org/jira/browse/BEAM-3900).

## WordCount Example
Lets start with the small example.
{{< highlight java >}}
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline pipeline = Pipeline.create(options);

// Use Kryo as coder fallback
KryoCoderProvider.of().registerTo(pipeline);

// Source of data loaded from Beam IO.
PCollection<String> input =
    pipeline
        .apply(Create.of(textLineByLine))
        .setTypeDescriptor(TypeDescriptor.of(String.class));

// zero, one, or more output elements. From input lines we will get data set of words.
PCollection<String> words =
    FlatMap.named("TOKENIZER")
        .of(lines)
        .using(
            (String line, Collector<String> context) -> {
              for (String word : Splitter.onPattern("\\s+").split(line)) {
                context.collect(word);
              }
            })
        .output();

// Now we can count input words - the operator ensures that all values for the same
// key (word in this case) end up being processed together. Then it counts number of appearances
// of the same key in 'words' PCollection and emits it to output.
PCollection<KV<String, Long>> counted =
    CountByKey.named("COUNT")
        .of(words)
        .keyBy(w -> w)
        .output();

// Format output.
PCollection<String> output =
    MapElements.named("FORMAT")
        .of(counted)
        .using(p -> p.getKey() + ": " + p.getValue())
        .output();

// Now we can again use Beam transformation. In this case we save words and their count
// into the text file.
output
    .apply(TextIO.write()
    .to("counted_words"));

pipeline.run();
{{< /highlight >}}

## Euphoria Guide

Euphoria API is composed from a set of operators, which allows you to construct `Pipeline` according to your application needs.

### Inputs and Outputs
Input data can be supplied through Beams IO into `PCollection`, the same way as in Beam.

{{< highlight java >}}
PCollection<String> input =
  pipeline
    .apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"))
    .setTypeDescriptor(TypeDescriptor.of(String.class));
{{< /highlight >}}

### Adding Operators
Real power of Euphoria API is in its [operators suite](#operator-reference). Each Operator consumes one or more input and produces one output
`PCollection`. Lets take a look at simple `MapElements` example.

{{< highlight java >}}
PCollection<Integer> input = ...

PCollection<String> mappedElements =
  MapElements
    .named("Int2Str")
    .of(input)
    .using(String::valueOf)
    .output();
{{< /highlight >}}
The operator consumes `input`, it applies given lambda expression (`String::valueOf`) on each element of `input` and returns mapped `PCollection`. Developer is guided through series of steps when creating operator so the declaration of an operator is straightforward. To start building operator just wrote its name and '.' (dot). Your IDE will give you hints.

First step to build any operator is to give it a name through `named()` method. The name is propagated through system and can latter be used when debugging.

### Coders and Types
Beam's Java SDK requires developers to supply `Coder` for custom element type in order to have a way of materializing elements. Euphoria allows to use [Kryo](https://github.com/EsotericSoftware/kryo) as a way of serialization. The [Kryo](https://github.com/EsotericSoftware/kryo) is located in `:sdks:java:extensions:kryo` module.

```
//gradle
dependencies {
    compile "org.apache.beam:sdks:java:extensions:kryo:${beam.version}"
}
```
```
//maven
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-sdks-java-extensions-kryo</artifactId>
  <version>${beam.version}</version>
</dependency>
```

All you need is to create `KryoCoderProvider` and register it to your
`Pipeline`. There are two ways of doing that.

When prototyping you may decide not to care much about coders, then create `KryoCoderProvider` without any class registrations to [Kryo](https://github.com/EsotericSoftware/kryo).
{{< highlight java >}}
//Register `KryoCoderProvider` which attempt to use `KryoCoder` to every non-primitive type
KryoCoderProvider.of().registerTo(pipeline);
{{< /highlight >}}
Such a `KryoCoderProvider` will return `KryoCoder` for every non-primitive element type. That of course degrades performance, since Kryo is not able to serialize instance of unknown types effectively. But it boost speed of pipeline development. This behavior is enabled by default and can be disabled when creating `Pipeline` through `KryoOptions`.
{{< highlight java >}}
PipelineOptions options = PipelineOptionsFactory.create();
options.as(KryoOptions.class).setKryoRegistrationRequired(true);
{{< /highlight >}}

Second more performance friendly way is to register all the types which will Kryo serialize. Sometimes it is also a good idea to register Kryo serializers of its own too. Euphoria allows you to do that by implementing your own `KryoRegistrar` and using it when creating `KryoCoderProvider`.
{{< highlight java >}}
//Do not allow `KryoCoderProvider` to return `KryoCoder` for unregistered types
options.as(KryoOptions.class).setKryoRegistrationRequired(true);

KryoCoderProvider.of(
        (kryo) -> { //KryoRegistrar of your uwn
          kryo.register(KryoSerializedElementType.class); //other may follow
        })
    .registerTo(pipeline);
{{< /highlight >}}
Beam resolves coders using types of elements. Type information is not available at runtime when element type is described by lambda implementation. It is due to type erasure and dynamic nature of lambda expressions. So there is an optional way of supplying `TypeDescriptor` every time new type is introduced during Operator construction.
{{< highlight java >}}
PCollection<Integer> input = ...

MapElements
  .named("Int2Str")
  .of(input)
  .using(String::valueOf, TypeDescriptors.strings())
  .output();
{{< /highlight >}}
Euphoria operator's will use `TypeDescriptor<Object>`, when `TypeDescriptors` is not supplied by user. So `KryoCoderProvider` may return `KryoCoder<Object>` for every element with unknown type, if allowed by `KryoOptions`. Supplying `TypeDescriptors` becomes mandatory when using `.setKryoRegistrationRequired(true)`.

### Metrics and Accumulators
Statistics about job's internals are very helpful during development of distributed jobs. Euphoria calls them accumulators. They are accessible through environment `Context`, which can be obtained from `Collector`, whenever working with it. It is usually present when zero-to-many output elements are expected from operator. For example in case of `FlatMap`.
{{< highlight java >}}
Pipeline pipeline = ...
PCollection<String> dataset = ..

PCollection<String> mapped =
FlatMap
  .named("FlatMap1")
  .of(dataset)
  .using(
    (String value, Collector<String> context) -> {
      context.getCounter("my-counter").increment();
        context.collect(value);
    })
  .output();
{{< /highlight >}}
`MapElements` also allows for `Context` to be accessed by supplying implementations of `UnaryFunctionEnv` (add second context argument) instead of `UnaryFunctor`.
{{< highlight java >}}
Pipeline pipeline = ...
PCollection<String> dataset = ...

PCollection<String> mapped =
  MapElements
    .named("MapThem")
    .of(dataset)
    .using(
      (input, context) -> {
        // use simple counter
        context.getCounter("my-counter").increment();
        return input.toLowerCase();
        })
      .output();
{{< /highlight >}}
Accumulators are translated into Beam Metrics in background so they can be viewed the same way. Namespace of translated metrics is set to operator's name.

### Windowing
Euphoria follows the same [windowing principles](/documentation/programming-guide/#windowing) as Beam Java SDK. Every shuffle operator (operator which needs to shuffle data over the network) allows you to set it. The same parameters as in Beam are required. `WindowFn`, `Trigger`, `WindowingStrategy` and other. Users are guided to either set all mandatory and several optional parameters  or none when building an operator. Windowing is propagated down through the `Pipeline`.
{{< highlight java >}}
PCollection<KV<Integer, Long>> countedElements =
  CountByKey.of(input)
      .keyBy(e -> e)
      .windowBy(FixedWindows.of(Duration.standardSeconds(1)))
      .triggeredBy(DefaultTrigger.of())
      .discardingFiredPanes()
      .withAllowedLateness(Duration.standardSeconds(5))
      .withOnTimeBehavior(OnTimeBehavior.FIRE_IF_NON_EMPTY)
      .withTimestampCombiner(TimestampCombiner.EARLIEST)
      .output();
{{< /highlight >}}

## How to get Euphoria
Euphoria is located in `dsl-euphoria` branch, `beam-sdks-java-extensions-euphoria` module of The Apache Beam project. To build `euphoria` subproject call:

```
./gradlew beam-sdks-java-extensions-euphoria:build
```

## Operator Reference
Operators are basically higher level data transformations, which allows you to build business logic of your data processing job in a simple way. All the Euphoria operators are documented in this section including examples. There are no examples with [windowing](#windowing) applied for the sake of simplicity. Refer to the [windowing section](#windowing) for more details.

### `CountByKey`
Counting elements with the same key. Requires input dataset to be mapped by given key extractor (`UnaryFunction`) to keys which are then counted. Output is emitted as `KV<K, Long>` (`K` is key type) where each `KV` contains key and number of element in input dataset for the key.
{{< highlight java >}}
// suppose input: [1, 2, 4, 1, 1, 3]
PCollection<KV<Integer, Long>> output =
  CountByKey.of(input)
    .keyBy(e -> e)
    .output();
// Output will contain:  [KV(1, 3), KV(2, 1), KV(3, 1), (4, 1)]
{{< /highlight >}}

### `Distinct`
 Outputting distinct (based on equals method) elements. It takes optional `UnaryFunction` mapper parameter which maps elements to output type.
 {{< highlight java >}}
// suppose input: [1, 2, 3, 3, 2, 1]
Distinct.named("unique-integers-only")
  .of(input)
  .output();
// Output will contain:  1, 2, 3
 {{< /highlight >}}
`Distinct` with mapper.
{{< highlight java >}}
// suppose keyValueInput: [KV(1, 100L), KV(3, 100_000L), KV(42, 10L), KV(1, 0L), KV(3, 0L)]
Distinct.named("unique-keys-only")
  .of(keyValueInput)
  .projected(KV::getKey)
  .output();
// Output will contain kvs with keys:  1, 3, 42 with some arbitrary values associated with given keys
{{< /highlight >}}

### `Join`
Represents inner join of two (left and right) datasets on given key producing a new dataset. Key is extracted from both datasets by separate extractors so elements in left and right can have different types denoted as `LeftT` and `RightT`. The join itself is performed by user-supplied `BinaryFunctor` which consumes elements from both dataset sharing the same key. And outputs result of the join (`OutputT`). The operator emits output dataset of `KV<K, OutputT>` type.
{{< highlight java >}}
// suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
// suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, String>> joined =
  Join.named("join-length-to-words")
    .of(left, right)
    .by(le -> le, String::length) // key extractors
    .using((Integer l, String r, Collector<String> c) -> c.collect(l + "+" + r))
    .output();
// joined will contain: [ KV(1, "1+X"), KV(3, "3+cat"), KV(3, "3+rat"), KV(4, "4+duck"),
// KV(3, "3+cat"), KV(3, "3+rat"), KV(1, "1+X")]
{{< /highlight >}}

### `LeftJoin`
Represents left join of two (left and right) datasets on given key producing single new dataset. Key is extracted from both datasets by separate extractors so elements in left and right can have different types denoted as `LeftT` and `RightT`. The join itself is performed by user-supplied `BinaryFunctor` which consumes one element from both dataset, where right is present optionally, sharing the same key. And outputs result of the join (`OutputT`). The operator emits output dataset of `KV<K, OutputT>` type.
{{< highlight java >}}
// suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
// suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, String>> joined =
  LeftJoin.named("left-join-length-to-words")
      .of(left, right)
      .by(le -> le, String::length) // key extractors
      .using(
          (Integer l, Optional<String> r, Collector<String> c) ->
              c.collect(l + "+" + r.orElse(null)))
      .output();
// joined will contain: [KV(1, "1+X"), KV(2, "2+null"), KV(3, "3+cat"),
// KV(3, "3+rat"), KV(0, "0+null"), KV(4, "4+duck"), KV(3, "3+cat"),
// KV(3, "3+rat"), KV(1, "1+X")]
{{< /highlight >}}
Euphoria support performance optimization called 'BroadcastHashJoin' for the `LeftJoin`. Broadcast join can be very efficient when joining two datasets where one fits in memory (in `LeftJoin` right dataset has to fit in memory). How to use 'Broadcast Hash Join' is described in [Translation](#translation) section.

### `RightJoin`
Represents right join of two (left and right) datasets on given key producing single new dataset. Key is extracted from both datasets by separate extractors so elements in left and right can have different types denoted as `LeftT` and `RightT`. The join itself is performed by user-supplied `BinaryFunctor` which consumes one element from both dataset, where left is present optionally, sharing the same key. And outputs result of the join (`OutputT`). The operator emits output dataset of `KV<K, OutputT>` type.
{{< highlight java >}}
// suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
// suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, String>> joined =
  RightJoin.named("right-join-length-to-words")
    .of(left, right)
    .by(le -> le, String::length) // key extractors
    .using(
      (Optional<Integer> l, String r, Collector<String> c) ->
        c.collect(l.orElse(null) + "+" + r))
    .output();
    // joined will contain: [ KV(1, "1+X"), KV(3, "3+cat"), KV(3, "3+rat"),
    // KV(4, "4+duck"), KV(3, "3+cat"), KV(3, "3+rat"), KV(1, "1+X"),
    // KV(8, "null+elephant"), KV(5, "null+mouse")]
{{< /highlight >}}
Euphoria support performance optimization called 'BroadcastHashJoin' for the `RightJoin`. Broadcast join can be very efficient when joining two datasets where one fits in memory (in `RightJoin` left dataset has to fit in memory). How to use 'Broadcast Hash Join' is described in [Translation](#translation) section.

### `FullJoin`
Represents full outer join of two (left and right) datasets on given key producing single new dataset. Key is extracted from both datasets by separate extractors so elements in left and right can have different types denoted as `LeftT` and `RightT`. The join itself is performed by user-supplied `BinaryFunctor` which consumes one element from both dataset, where both are present only optionally, sharing the same key. And outputs result of the join (`OutputT`). The operator emits output dataset of `KV<K, OutputT>` type.
{{< highlight java >}}
// suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
// suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, String>> joined =
  FullJoin.named("join-length-to-words")
    .of(left, right)
    .by(le -> le, String::length) // key extractors
    .using(
      (Optional<Integer> l, Optional<String> r, Collector<String> c) ->
        c.collect(l.orElse(null) + "+" + r.orElse(null)))
    .output();
// joined will contain: [ KV(1, "1+X"), KV(2, "2+null"), KV(3, "3+cat"), KV(3, "3+rat"),
// KV(0, "0+null"), KV(4, "4+duck"), KV(3, "3+cat"), KV(3, "3+rat"),KV(1, "1+X"),
//  KV(1, "null+elephant"), KV(5, "null+mouse")]
{{< /highlight >}}

### `MapElements`
Transforms one input element of input type `InputT` to one output element of another (potentially the same) `OutputT` type. Transformation is done through user specified `UnaryFunction`.
{{< highlight java >}}
// suppose inputs contains: [ 0, 1, 2, 3, 4, 5]
PCollection<String> strings =
  MapElements.named("int2str")
    .of(input)
    .using(i -> "#" + i)
    .output();
// strings will contain: [ "#0", "#1", "#2", "#3", "#4", "#5"]
{{< /highlight >}}

### `FlatMap`
Transforms one input element of input type `InputT` to zero or more output elements of another (potentially the same) `OutputT` type. Transformation is done through user specified `UnaryFunctor`, where `Collector<OutputT>` is utilized to emit output elements. Notice similarity with `MapElements` which can always emit only one element.
{{< highlight java >}}
// suppose words contain: ["Brown", "fox", ".", ""]
PCollection<String> letters =
  FlatMap.named("str2char")
    .of(words)
    .using(
      (String s, Collector<String> collector) -> {
        for (int i = 0; i < s.length(); i++) {
          char c = s.charAt(i);
          collector.collect(String.valueOf(c));
        }
      })
    .output();
// characters will contain: ["B", "r", "o", "w", "n",  "f", "o", "x", "."]
{{< /highlight >}}
`FlatMap` may be used to determine time-stamp of elements. It is done by supplying implementation of `ExtractEventTime` time extractor when building it. There is specialized `AssignEventTime` operator to assign time-stamp to elements. Consider using it, you code may be more readable.
{{< highlight java >}}
// suppose events contain events of SomeEventObject, its 'getEventTimeInMillis()' methods returns time-stamp
PCollection<SomeEventObject> timeStampedEvents =
  FlatMap.named("extract-event-time")
    .of(events)
    .using( (SomeEventObject e, Collector<SomeEventObject> c) -> c.collect(e))
    .eventTimeBy(SomeEventObject::getEventTimeInMillis)
    .output();
//Euphoria will now know event time for each event
{{< /highlight >}}

### `Filter`
`Filter` throws away all the elements which do not pass given condition. The condition is supplied by the user as implementation of `UnaryPredicate`. Input and output elements are of the same type.
{{< highlight java >}}
// suppose nums contains: [0,  1, 2, 3, 4, 5, 6, 7, 8, 9]
PCollection<Integer> divisibleBythree =
  Filter.named("divisibleByThree").of(nums).by(e -> e % 3 == 0).output();
//divisibleBythree will contain: [ 0, 3, 6, 9]
{{< /highlight >}}

### `ReduceByKey`
Performs aggregation of `InputT` type elements with the same key through user-supplied reduce function. Key is extracted from each element through `UnaryFunction` which takes input element and outputs its key of type `K`. Elements can optionally be mapped to value of type `V`, it happens before elements shuffle, so it can have positive performance influence.

Finally, elements with the same key are aggregated by user-defined `ReduceFunctor`, `ReduceFunction` or `CombinableReduceFunction`. They differs in number of arguments they take and in way output is interpreted. `ReduceFunction` is basically a function which takes `Stream` of elements as input and outputs one aggregation result. `ReduceFunctor` takes second `Collector` which allows for access to `Context`. When `CombinableReduceFunction` is provided, partial reduction is performed before shuffle so less data have to be transported through network.

Following example shows basic usage of `ReduceByKey` operator including value extraction.
{{< highlight java >}}
//suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, Long>> countOfAnimalNamesByLength =
  ReduceByKey.named("to-letters-couts")
    .of(animals)
    .keyBy(String::length) // length of animal name will be used as groupping key
    // we need to count each animal name once, so why not to optimize each string to 1
    .valueBy(e -> 1)
    .reduceBy(Stream::count)
    .output();
// countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]
{{< /highlight >}}

Now suppose that we want to track our `ReduceByKey` internals using counter.
{{< highlight java >}}
//suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, Long>> countOfAnimalNamesByLenght =
  ReduceByKey.named("to-letters-couts")
    .of(animals)
    .keyBy(String::length) // length of animal name will be used as grouping key
    // we need to count each animal name once, so why not to optimize each string to 1
    .valueBy(e -> 1)
    .reduceBy(
      (Stream<Integer> s, Collector<Long> collector) -> {
        collector.collect(s.count());
        collector.asContext().getCounter("num-of-keys").increment();
      })
      .output();
// countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]
{{< /highlight >}}

Again the same example with optimized combinable output.
{{< highlight java >}}
//suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, Long>> countOfAnimalNamesByLenght =
  ReduceByKey.named("to-letters-couts")
    .of(animals)
    .keyBy(String::length) // length of animal name will e used as grouping key
    // we need to count each animal name once, so why not to optimize each string to 1
    .valueBy(e -> 1L)
    .combineBy(s -> s.mapToLong(l -> l).sum()) //Stream::count will not be enough
    .output();
// countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]
{{< /highlight >}}
Note that the provided `CombinableReduceFunction` has to be associative and commutative to be truly combinable. So it can be used to compute partial results before shuffle. And then merge partial result to one. That is why simple `Stream::count` will not work in this example unlike in the previous one.

Euphoria aims to make code easy to write and read. Therefore some support to write combinable reduce functions in form of `Fold` or folding function is already there. It allows user to supply only the reduction logic (`BinaryFunction`) and creates `CombinableReduceFunction` out of it. Supplied `BinaryFunction` still have to be associative.
{{< highlight java >}}
//suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
PCollection<KV<Integer, Long>> countOfAnimalNamesByLenght =
  ReduceByKey.named("to-letters-couts")
    .of(animals)
    .keyBy(String::length) // length of animal name will be used as grouping key
    // we need to count each animal name once, so why not to optimize each string to 1
    .valueBy(e -> 1L)
    .combineBy(Fold.of((l1, l2) -> l1 + l2))
    .output();
// countOfAnimalNamesByLength will contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]
{{< /highlight >}}

### `ReduceWindow`
Reduces all elements in a [window](#windowing). The operator corresponds to `ReduceByKey` with the same key for all elements, so the actual key is defined only by window.
{{< highlight java >}}
//suppose input contains [ 1, 2, 3, 4, 5, 6, 7, 8 ]
//lets assign time-stamp to each input element
PCollection<Integer> withEventTime = AssignEventTime.of(input).using(i -> 1000L * i).output();

PCollection<Integer> output =
  ReduceWindow.of(withEventTime)
    .combineBy(Fold.of((i1, i2) -> i1 + i2))
    .windowBy(FixedWindows.of(Duration.millis(5000)))
    .triggeredBy(DefaultTrigger.of())
    .discardingFiredPanes()
    .output();
//output will contain: [ 10, 26 ]
{{< /highlight >}}

### `SumByKey`
Summing elements with same key. Requires input dataset to be mapped by given key extractor (`UnaryFunction`) to keys. By value extractor, also `UnaryFunction` which outputs to `Long`, to values. Those values are then grouped by key and summed. Output is emitted as `KV<K, Long>` (`K` is key type) where each `KV` contains key and number of element in input dataset for the key.
{{< highlight java >}}
//suppose input contains: [ 1, 2, 3, 4, 5, 6, 7, 8, 9 ]
PCollection<KV<Integer, Long>> output =
  SumByKey.named("sum-odd-and-even")
    .of(input)
    .keyBy(e -> e % 2)
    .valueBy(e -> (long) e)
    .output();
// output will contain: [ KV.of(0, 20L), KV.of(1, 25L)]
{{< /highlight >}}

### `Union`
Merge of at least two datasets of the same type without any guarantee about elements ordering.
{{< highlight java >}}
//suppose cats contains: [ "cheetah", "cat", "lynx", "jaguar" ]
//suppose rodents conains: [ "squirrel", "mouse", "rat", "lemming", "beaver" ]
PCollection<String> animals =
  Union.named("to-animals")
    .of(cats, rodents)
    .output();
// animal will contain: "cheetah", "cat", "lynx", "jaguar", "squirrel", "mouse", "rat", "lemming", "beaver"
{{< /highlight >}}

### `TopPerKey`
Emits one top-rated element per key. Key of type `K` is extracted by given `UnaryFunction`. Another `UnaryFunction` extractor allows for conversion input elements to values of type `V`. Selection of top element is based on _score_, which is obtained from each element by user supplied `UnaryFunction` called score calculator. Score type is denoted as `ScoreT` and it is required to extend `Comparable<ScoreT>` so scores of two elements can be compared directly. Output dataset elements are of type `Triple<K, V, ScoreT>`.
{{< highlight java >}}
// suppose 'animals contain: [ "mouse", "elk", "rat", "mule", "elephant", "dinosaur", "cat", "duck", "caterpillar" ]
PCollection<Triple<Character, String, Integer>> longestNamesByLetter =
  TopPerKey.named("longest-animal-names")
    .of(animals)
    .keyBy(name -> name.charAt(0)) // first character is the key
    .valueBy(UnaryFunction.identity()) // value type is the same as input element type
    .scoreBy(String::length) // length defines score, note that Integer implements Comparable<Integer>
    .output();
//longestNamesByLetter wil contain: [ ('m', "mouse", 5), ('r', "rat", 3), ('e', "elephant", 8), ('d', "dinosaur", 8), ('c', "caterpillar", 11) ]
{{< /highlight >}}
`TopPerKey` is a shuffle operator so it allows for widowing to be defined.

### `AssignEventTime`
Euphoria needs to know how to extract time-stamp from elements when [windowing](#windowing) is applied. `AssignEventTime` tells Euphoria how to do that through given implementation of `ExtractEventTime` function.
{{< highlight java >}}
// suppose events contain events of SomeEventObject, its 'getEventTimeInMillis()' methods returns time-stamp
PCollection<SomeEventObject> timeStampedEvents =
  AssignEventTime.named("extract-event-tyme")
    .of(events)
    .using(SomeEventObject::getEventTimeInMillis)
    .output();
//Euphoria will now know event time for each event
{{< /highlight >}}

## Translation
Euphoria API is built on top of Beam Java SDK. The API is transparently translated into Beam's `PTransforms` in background.

The fact that Euphoria API is translated to Beam Java SDK give us option to fine tune the translation itself. Translation of an `Operator` is realized through implementations of `OperatorTranslator`.
Euphoria uses `TranslationProvider` to decide which translator should be used. User of Euphoria API can supply its own `OperatorTranslator` through `TranslationProvider` by extending `EuphoriaOptions`.
Euphoria already contains some useful implementations.

### TranslationProviders
#### `GenericTranslatorProvider`
General `TranslationProvider`. Allows for registration of `OperatorTranslator` three different ways:
* Registration of operator specific translator by operator class.
* Registration operator specific translator by operator class and additional user defined predicate.
* Registration of general (not specific to one operator type) translator with user defined predicate.
Order of registration is important since `GenericTranslatorProvider` returns first suitable translator.

{{< highlight java >}}
GenericTranslatorProvider.newBuilder()
  .register(FlatMap.class, new FlatMapTranslator<>()) // register by operator class
  .register(
    Join.class,
    (Join op) -> {
      String name = ((Optional<String>) op.getName()).orElse("");
      return name.toLowerCase().startsWith("broadcast");
    },
    new BroadcastHashJoinTranslator<>()) // register by class and predicate
  .register(
    op -> op instanceof CompositeOperator,
    new CompositeOperatorTranslator<>()) // register by predicate only
  .build();
{{< /highlight >}}

`GenericTranslatorProvider` is default provider, see `GenericTranslatorProvider.createWithDefaultTranslators()`.

#### `CompositeProvider`
Implements chaining of `TranslationProvider`s in given order. That in turn allows for composing user defined `TranslationProvider` with already supplied by Euphoria API.

{{< highlight java >}}
CompositeProvider.of(
  CustomTranslatorProvider.of(), // first ask CustomTranslatorProvider for translator
  GenericTranslatorProvider.createWithDefaultTranslators()); // then ask default provider if needed
{{< /highlight >}}

### Operator Translators
Each `Operator` needs to be translated to Java Beam SDK. That is done by implementations of `OperatorTranslator`. Euphoria API contains translator for every `Operator` implementation supplied with it.
Some operators may have an alternative translations suitable in some cases. `Join` typically may have many implementations. We are describing only the most interesting here.

#### `BroadcastHashJoinTranslator`
Is able to translate `LeftJoin` and `RightJoin` when whole dataset of one side fits in memory of target executor. So it can be distributed using Beam's side inputs. Resulting in better performance.

#### `CompositeOperatorTranslator`
Some operators are composite. Meaning that they are in fact wrapped chain of other operators. `CompositeOperatorTranslator` ensures that they are decomposed to elemental operators during translation process.

### Details
Most of the translation happens in `org.apache.beam.sdk.extensions.euphoria.core.translate` package. Where the most interesting classes are:
* `OperatorTranslator` - Interface which defining inner API of Euphoria to Beam translation.
* `TranslatorProvider` - Way of supplying custom translators.
* `OperatorTransform` - Is governing actual translation and/or expansion Euphoria's operators to Beam's `PTransform`
* `EuphoriaOptions` - A `PipelineOptions`, allows for setting custom `TranslatorProvider`.

The package also contains implementation of `OperatorTranslator` for each supported operator type (`JoinTranslator`, `FlatMapTranslator`, `ReduceByKeyTranslator`). Not every operator needs to have translator of its own. Some of them can be composed from other operators. That is why operators may implement `CompositeOperator` which give them option to be expanded to set of other Euphoria operators.

The translation process was designed with flexibility in mind. We wanted to allow different ways of translating higher-level Euphoria operators to Beam's SDK's primitives. It allows for further performance optimizations based on user choices or some knowledge about data obtained automatically.

## Unsupported Features
[Original Euphoria](https://github.com/seznam/euphoria) contained some features and operators not jet supported in Beam port. List of not yet supported features follows:
* `ReduceByKey` in original Euphoria was allowed to sort output values (per key). This is also not yet translatable into Beam, therefore not supported.
