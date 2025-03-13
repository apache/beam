/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.docs;

import static java.util.Arrays.asList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CompositeOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Distinct;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FullJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceWindow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.SumByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.TopPerKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.translate.BroadcastHashJoinTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.CompositeOperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.FlatMapTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.TranslatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.CompositeProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.GenericTranslatorProvider;
import org.apache.beam.sdk.extensions.kryo.KryoCoderProvider;
import org.apache.beam.sdk.extensions.kryo.KryoOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Contains all the examples from documentation page. Not all of them contains asserts, some do, but
 * the rest is often here just to confirm that they compile. Once something break or changes, the
 * documentation needs to change too.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class DocumentationExamplesTest {
  private List<String> textLineByLine =
      Arrays.asList(
          "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ",
          "Vestibulum volutpat pellentesque risus at sodales.",
          "Interdum et malesuada fames ac ante ipsum primis in faucibus.",
          "Donec sit amet arcu nec tellus sodales ultricies.",
          "Quisque ipsum fermentum nisl at libero accumsan consectetur.",
          "Praesent lobortis ex eget ex rhoncus, quis malesuada risus tristique.",
          "Aliquam risus at orci, porttitor eu turpis et, porttitor semper ligula.");

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setup() {
    KryoCoderProvider.of(k -> {}).registerTo(pipeline);
  }

  @Ignore("We do not want to actually write output files from this test.")
  @Test
  public void wordCountExample() {
    final PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline pipeline = Pipeline.create(options);

    // Use Kryo as coder fallback
    KryoCoderProvider.of().registerTo(pipeline);

    // Source of data loaded from Beam IO.
    PCollection<String> lines =
        pipeline
            .apply(Create.of(textLineByLine))
            .setTypeDescriptor(TypeDescriptor.of(String.class));

    // FlatMap processes one input element at a time and allows user code to emit
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
    // of the same key in 'words' dataset and emits it to output.
    PCollection<KV<String, Long>> counted =
        CountByKey.named("COUNT").of(words).keyBy(w -> w).output();

    // Format output.
    PCollection<String> output =
        MapElements.named("FORMAT")
            .of(counted)
            .using(p -> p.getKey() + ": " + p.getValue())
            .output();

    // Now we can again use Beam transformation. In this case we save words and their count
    // into the text file.
    output.apply(TextIO.write().to("counted_words"));

    pipeline.run();
  }

  @Test
  public void inputsAndOutputsSection() {

    pipeline
        .apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"))
        .setTypeDescriptor(TypeDescriptor.of(String.class));

    pipeline.run();
  }

  @Test
  public void addOperatorSection() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 4, 3));

    PCollection<String> mappedElements =
        MapElements.named("Int2Str").of(input).using(String::valueOf).output();

    PAssert.that(mappedElements).containsInAnyOrder("1", "2", "4", "3");

    pipeline.run();
  }

  @Test
  public void metricsAndAccumulatorsSection() {
    final PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> dataset = pipeline.apply(Create.of("a", "x"));

    FlatMap.named("FlatMap1")
        .of(dataset)
        .using(
            (String value, Collector<String> context) -> {
              context.getCounter("my-counter").increment();
              context.collect(value);
            })
        .output();

    MapElements.named("MapThem")
        .of(dataset)
        .using(
            (value, context) -> {
              // use simple counter
              context.getCounter("my-counter").increment();

              return value.toLowerCase();
            })
        .output();
  }

  @Test
  public void codersAndTypesSection() {
    final PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    // Register `KryoCoderProvider` which attempt to use `KryoCoder` to every non-primitive type
    KryoCoderProvider.of().registerTo(pipeline);

    // Do not allow `KryoCoderProvider` to return `KryoCoder` for unregistered types
    options.as(KryoOptions.class).setKryoRegistrationRequired(true);

    KryoCoderProvider.of(
            kryo -> { // KryoRegistrar of your uwn
              kryo.register(KryoSerializedElementType.class); // other may follow
            })
        .registerTo(pipeline);

    PCollection<Integer> input =
        pipeline.apply(Create.of(1, 2, 3, 4)).setTypeDescriptor(TypeDescriptors.integers());

    MapElements.named("Int2Str")
        .of(input)
        .using(String::valueOf, TypeDescriptors.strings())
        .output();
  }

  @Test
  public void windowingSection() {

    PCollection<Integer> input =
        pipeline.apply(Create.of(1, 2, 3, 4)).setTypeDescriptor(TypeDescriptors.integers());

    CountByKey.of(input)
        .keyBy(e -> e)
        .windowBy(FixedWindows.of(Duration.standardSeconds(1)))
        .triggeredBy(DefaultTrigger.of())
        .discardingFiredPanes()
        .withAllowedLateness(Duration.standardSeconds(5))
        .withOnTimeBehavior(OnTimeBehavior.FIRE_IF_NON_EMPTY)
        .withTimestampCombiner(TimestampCombiner.EARLIEST)
        .output();

    pipeline.run();
  }

  private static class KryoSerializedElementType {}

  @Test
  public void countByKeyOperator() {

    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 4, 1, 1, 3));

    // suppose input: [1, 2, 4, 1, 1, 3]
    PCollection<KV<Integer, Long>> output = CountByKey.of(input).keyBy(e -> e).output();
    // Output will contain:  [KV(1, 3), KV(2, 1), KV(3, 1), (4, 1)]

    PAssert.that(output)
        .containsInAnyOrder(asList(KV.of(1, 3L), KV.of(2, 1L), KV.of(3, 1L), KV.of(4, 1L)));

    pipeline.run();
  }

  @Test
  public void distinctOperator() {

    PCollection<Integer> input = pipeline.apply("input", Create.of(1, 2, 3, 3, 2, 1));

    // suppose input: [1, 2, 3, 3, 2, 1]
    Distinct.named("unique-integers-only").of(input).output();
    // Output will contain:  1, 2, 3

    PCollection<KV<Integer, Long>> keyValueInput =
        pipeline.apply(
            "keyValueInput",
            Create.of(
                KV.of(1, 100L), KV.of(3, 100_000L), KV.of(42, 10L), KV.of(1, 0L), KV.of(3, 0L)));

    // suppose input: [KV(1, 100L), KV(3, 100_000L), KV(42, 10L), KV(1, 0L), KV(3, 0L)]
    PCollection<KV<Integer, Long>> distinct =
        Distinct.named("unique-keys-only").of(keyValueInput).projected(KV::getKey).output();

    // Output will contain:  1, 3, 42
    PCollection<Integer> uniqueKeys = MapElements.of(distinct).using(KV::getKey).output();

    PAssert.that(uniqueKeys).containsInAnyOrder(1, 3, 42);

    pipeline.run();
  }

  @Test
  public void batchJoinOperator() {

    PCollection<Integer> left =
        pipeline
            .apply("left", Create.of(1, 2, 3, 0, 4, 3, 1))
            .setTypeDescriptor(TypeDescriptors.integers());
    PCollection<String> right =
        pipeline
            .apply("right", Create.of("mouse", "rat", "elephant", "cat", "X", "duck"))
            .setTypeDescriptor(TypeDescriptors.strings());

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

    PAssert.that(joined)
        .containsInAnyOrder(
            asList(
                KV.of(1, "1+X"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(4, "4+duck"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(1, "1+X")));

    pipeline.run();
  }

  @Test
  public void batchLeftJoinOperator() {

    PCollection<Integer> left =
        pipeline
            .apply("left", Create.of(1, 2, 3, 0, 4, 3, 1))
            .setTypeDescriptor(TypeDescriptors.integers());
    PCollection<String> right =
        pipeline
            .apply("right", Create.of("mouse", "rat", "elephant", "cat", "X", "duck"))
            .setTypeDescriptor(TypeDescriptors.strings());

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

    PAssert.that(joined)
        .containsInAnyOrder(
            asList(
                KV.of(1, "1+X"),
                KV.of(2, "2+null"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(0, "0+null"),
                KV.of(4, "4+duck"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(1, "1+X")));

    pipeline.run();
  }

  @Test
  public void batchRightJoinFullOperator() {

    PCollection<Integer> left =
        pipeline
            .apply("left", Create.of(1, 2, 3, 0, 4, 3, 1))
            .setTypeDescriptor(TypeDescriptors.integers());
    PCollection<String> right =
        pipeline
            .apply("right", Create.of("mouse", "rat", "elephant", "cat", "X", "duck"))
            .setTypeDescriptor(TypeDescriptors.strings());

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

    PAssert.that(joined)
        .containsInAnyOrder(
            asList(
                KV.of(1, "1+X"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(4, "4+duck"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(1, "1+X"),
                KV.of(8, "null+elephant"),
                KV.of(5, "null+mouse")));

    pipeline.run();
  }

  @Test
  public void batchFullJoinOperator() {

    PCollection<Integer> left =
        pipeline
            .apply("left", Create.of(1, 2, 3, 0, 4, 3, 1))
            .setTypeDescriptor(TypeDescriptors.integers());
    PCollection<String> right =
        pipeline
            .apply("right", Create.of("mouse", "rat", "elephant", "cat", "X", "duck"))
            .setTypeDescriptor(TypeDescriptors.strings());

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
    //  KV(1, "null+elephant"), KV(5, "null+mouse")];

    PAssert.that(joined)
        .containsInAnyOrder(
            asList(
                KV.of(1, "1+X"),
                KV.of(2, "2+null"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(0, "0+null"),
                KV.of(4, "4+duck"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(1, "1+X"),
                KV.of(8, "null+elephant"),
                KV.of(5, "null+mouse")));

    pipeline.run();
  }

  @Test
  public void mapElementsOperator() {

    PCollection<Integer> input =
        pipeline.apply(Create.of(0, 1, 2, 3, 4, 5)).setTypeDescriptor(TypeDescriptors.integers());

    // suppose inputs contains: [ 0, 1, 2, 3, 4, 5]
    PCollection<String> strings =
        MapElements.named("int2str").of(input).using(i -> "#" + i).output();
    // strings will contain: [ "#0", "#1", "#2", "#3", "#4", "#5"]

    PAssert.that(strings).containsInAnyOrder("#0", "#1", "#2", "#3", "#4", "#5");

    pipeline.run();
  }

  @Test
  public void flatMapOperator() {

    PCollection<String> words = pipeline.apply(Create.of(asList("Brown", "fox", ".", "")));

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

    PAssert.that(letters).containsInAnyOrder("B", "r", "o", "w", "n", "f", "o", "x", ".");
    pipeline.run();
  }

  @Test
  public void flatMapWithTimeExtractorOperator() {

    PCollection<SomeEventObject> events =
        pipeline.apply(
            Create.of(
                new SomeEventObject(0),
                new SomeEventObject(1),
                new SomeEventObject(2),
                new SomeEventObject(3),
                new SomeEventObject(4)));

    // suppose events contain events of SomeEventObject, its 'getEventTimeInMillis()' methods
    // returns time-stamp
    FlatMap.named("extract-event-time")
        .of(events)
        .using((SomeEventObject e, Collector<SomeEventObject> c) -> c.collect(e))
        .eventTimeBy(SomeEventObject::getEventTimeInMillis)
        .output();
    // Euphoria will now know event time for each event

    pipeline.run();
  }

  @Test
  public void filterOperator() {

    PCollection<Integer> nums = pipeline.apply(Create.of(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));

    // suppose nums contains: [0,  1, 2, 3, 4, 5, 6, 7, 8, 9]
    PCollection<Integer> divisibleBythree =
        Filter.named("divisibleByFive").of(nums).by(e -> e % 3 == 0).output();
    // divisibleBythree will contain: [ 0, 3, 6, 9]

    PAssert.that(divisibleBythree).containsInAnyOrder(0, 3, 6, 9);
    pipeline.run();
  }

  @Test
  public void reduceByKeyTestOperator1() {

    PCollection<String> animals =
        pipeline.apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"));

    // suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    PCollection<KV<Integer, Long>> countOfAnimalNamesByLength =
        ReduceByKey.named("to-letters-counts")
            .of(animals)
            .keyBy(String::length) // length of animal name will be used as grouping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1)
            .reduceBy(Stream::count)
            .output();
    // countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5,
    // 1L), KV.of(8, 1L) ]

    PAssert.that(countOfAnimalNamesByLength)
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  @Test
  public void reduceByKeyTestOperatorCombinable() {

    PCollection<String> animals =
        pipeline.apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"));

    // suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    PCollection<KV<Integer, Long>> countOfAnimalNamesByLength =
        ReduceByKey.named("to-letters-counts")
            .of(animals)
            .keyBy(String::length) // length of animal name will be used as grouping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1L)
            .combineBy(s -> s.mapToLong(l -> l).sum())
            .output();
    // countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5,
    // 1L), KV.of(8, 1L) ]

    PAssert.that(countOfAnimalNamesByLength)
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  @Test
  public void reduceByKeyTestOperatorContext() {

    PCollection<String> animals =
        pipeline.apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"));

    // suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    PCollection<KV<Integer, Long>> countOfAnimalNamesByLength =
        ReduceByKey.named("to-letters-counts")
            .of(animals)
            .keyBy(String::length) // length of animal name will e used as grouping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1)
            .reduceBy(
                (Stream<Integer> s, Collector<Long> collector) -> {
                  collector.collect(s.count());
                  collector.asContext().getCounter("num-of-keys").increment();
                })
            .output();
    // countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5,
    // 1L), KV.of(8, 1L) ]

    PAssert.that(countOfAnimalNamesByLength)
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  /**
   * Note that this one is not mentioned in documentation due to high number of RBK examples and
   * rather lower explanation value. Please consider to include it in future
   */
  @Test
  public void reduceByKeyTestOperatorContextManyOutputs() {

    PCollection<String> animals =
        pipeline.apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"));

    PCollection<KV<Integer, Long>> countOfAnimalNamesByLength =
        ReduceByKey.named("to-letters-counts")
            .of(animals)
            .keyBy(String::length) // length of animal name will e used as grouping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1)
            .reduceBy(
                (Stream<Integer> s, Collector<Long> collector) -> {
                  long count = s.count();
                  collector.collect(count);
                  collector.collect(2L * count);
                })
            .output();

    PAssert.that(countOfAnimalNamesByLength)
        .containsInAnyOrder(
            asList(
                KV.of(1, 1L),
                KV.of(3, 2L),
                KV.of(4, 1L),
                KV.of(5, 1L),
                KV.of(8, 1L),
                KV.of(1, 2L),
                KV.of(3, 4L),
                KV.of(4, 2L),
                KV.of(5, 2L),
                KV.of(8, 2L)));

    pipeline.run();
  }

  @Test
  public void reduceByKeyTestOperatorFold() {

    PCollection<String> animals =
        pipeline.apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"));

    // suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    PCollection<KV<Integer, Long>> countOfAnimalNamesByLength =
        ReduceByKey.named("to-letters-counts")
            .of(animals)
            .keyBy(String::length) // length of animal name will be used as grouping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1L)
            .combineBy(Fold.of((l1, l2) -> l1 + l2))
            .output();
    // countOfAnimalNamesByLength will contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5,
    // 1L), KV.of(8, 1L) ]

    PAssert.that(countOfAnimalNamesByLength)
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  @Test
  public void testSumByKeyOperator() {
    PCollection<Integer> input = pipeline.apply(Create.of(asList(1, 2, 3, 4, 5, 6, 7, 8, 9)));

    // suppose input contains: [ 1, 2, 3, 4, 5, 6, 7, 8, 9 ]
    PCollection<KV<Integer, Long>> output =
        SumByKey.named("sum-odd-and-even")
            .of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> (long) e)
            .output();
    // output will contain: [ KV.of(0, 20L), KV.of(1, 25L)]

    PAssert.that(output).containsInAnyOrder(asList(KV.of(0, 20L), KV.of(1, 25L)));
    pipeline.run();
  }

  @Test
  public void testUnionOperator() {

    PCollection<String> cats =
        pipeline
            .apply("cats", Create.of(asList("cheetah", "cat", "lynx", "jaguar")))
            .setTypeDescriptor(TypeDescriptors.strings());

    PCollection<String> rodents =
        pipeline
            .apply("rodents", Create.of("squirrel", "mouse", "rat", "lemming", "beaver"))
            .setTypeDescriptor(TypeDescriptors.strings());

    // suppose cats contains: [ "cheetah", "cat", "lynx", "jaguar" ]
    // suppose rodents contains: [ "squirrel", "mouse", "rat", "lemming", "beaver" ]
    PCollection<String> animals = Union.named("to-animals").of(cats, rodents).output();

    // animal will contain: "cheetah", "cat", "lynx", "jaguar", "squirrel", "mouse", "rat",
    // "lemming", "beaver"
    PAssert.that(animals)
        .containsInAnyOrder(
            "cheetah", "cat", "lynx", "jaguar", "squirrel", "mouse", "rat", "lemming", "beaver");

    pipeline.run();
  }

  @Test
  public void testAssignEventTimeOperator() {

    PCollection<SomeEventObject> events =
        pipeline.apply(
            Create.of(
                asList(
                    new SomeEventObject(0),
                    new SomeEventObject(1),
                    new SomeEventObject(2),
                    new SomeEventObject(3),
                    new SomeEventObject(4))));

    // suppose events contain events of SomeEventObject, its 'getEventTimeInMillis()' methods
    // returns time-stamp
    AssignEventTime.named("extract-event-time")
        .of(events)
        .using(SomeEventObject::getEventTimeInMillis)
        .output();
    // Euphoria will now know event time for each event

    pipeline.run();
  }

  private static class SomeEventObject implements Serializable {

    private long timestamp;

    SomeEventObject(long timestamp) {
      this.timestamp = timestamp;
    }

    long getEventTimeInMillis() {
      return timestamp;
    }
  }

  @Test
  public void testReduceWithWindowOperator() {

    PCollection<Integer> input = pipeline.apply(Create.of(asList(1, 2, 3, 4, 5, 6, 7, 8)));

    // suppose input contains [ 1, 2, 3, 4, 5, 6, 7, 8 ]
    // lets assign time-stamp to each input element
    PCollection<Integer> withEventTime = AssignEventTime.of(input).using(i -> 1000L * i).output();

    PCollection<Integer> output =
        ReduceWindow.of(withEventTime)
            .combineBy(Fold.of((i1, i2) -> i1 + i2))
            .windowBy(FixedWindows.of(Duration.millis(5000)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .output();
    // output will contain: [ 10, 26 ]

    PAssert.that(output).containsInAnyOrder(10, 26);

    pipeline.run();
  }

  @Test
  public void testTopPerKeyOperator() {

    PCollection<String> animals =
        pipeline.apply(
            Create.of(
                "mouse",
                "elk",
                "rat",
                "mule",
                "elephant",
                "dinosaur",
                "cat",
                "duck",
                "caterpillar"));

    // suppose 'animals contain: [ "mouse", "elk", "rat", "mule", "elephant", "dinosaur", "cat",
    // "duck", "caterpillar" ]
    PCollection<Triple<Character, String, Integer>> longestNamesByLetter =
        TopPerKey.named("longest-animal-names")
            .of(animals)
            .keyBy(name -> name.charAt(0)) // first character is the key
            .valueBy(UnaryFunction.identity()) // value type is the same as input element type
            .scoreBy(String::length) // length defines score, note that Integer implements
            // Comparable<Integer>
            .output();
    // longestNamesByLetter will contain: [ ('m', "mouse", 5), ('r', "rat", 3), ('e', "elephant",
    // 8), ('d', "dinosaur", 8), ('c', "caterpillar", 11) ]

    PAssert.that(longestNamesByLetter)
        .containsInAnyOrder(
            Triple.of('m', "mouse", 5),
            Triple.of('r', "rat", 3),
            Triple.of('e', "elephant", 8),
            Triple.of('d', "dinosaur", 8),
            Triple.of('c', "caterpillar", 11));

    pipeline.run();
  }

  @Test
  public void testGenericTranslatorProvider() {

    GenericTranslatorProvider provider =
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

    Assert.assertNotNull(provider);
  }

  private static class CustomTranslatorProvider implements TranslatorProvider {

    static CustomTranslatorProvider of() {
      return new CustomTranslatorProvider();
    }

    @Override
    public <InputT, OutputT, OperatorT extends Operator<OutputT>>
        Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(
            OperatorT operator) {
      return Optional.empty();
    }
  }

  @Test
  public void testCompositeTranslationProviderExample() {

    CompositeProvider compositeProvider =
        CompositeProvider.of(
            CustomTranslatorProvider.of(), // first ask CustomTranslatorProvider for translator
            GenericTranslatorProvider
                .createWithDefaultTranslators()); // then ask default provider if needed

    Assert.assertNotNull(compositeProvider);
  }
}
