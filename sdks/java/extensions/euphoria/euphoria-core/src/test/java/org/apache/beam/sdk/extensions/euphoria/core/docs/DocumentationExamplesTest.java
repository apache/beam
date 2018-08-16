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

import com.google.common.base.Splitter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
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
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Util;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Fold;
import org.apache.beam.sdk.extensions.euphoria.core.translate.BeamFlow;
import org.apache.beam.sdk.extensions.euphoria.core.translate.EuphoriaPTransform;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.KryoCoder;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.RegisterCoders;
import org.apache.beam.sdk.extensions.euphoria.core.translate.common.PipelineUtils;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * Contains all the examples from documentation page. Not all of them contains asserts, some do, but
 * the rest is often here just to confirm that they compile. Once something break or changes, the
 * documentation needs to change too.
 */
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

  @Ignore("We do not want to actually write output files from this test.")
  @Test
  public void wordCountExample() {
    PipelineOptions options = PipelineUtils.getDirectPipelineOptions();

    Pipeline pipeline = Pipeline.create(options);

    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    // Source of data loaded from Beam IO.
    PCollection<String> input =
        pipeline
            .apply(Create.of(textLineByLine))
            .setTypeDescriptor(TypeDescriptor.of(String.class));
    // Transform PCollection to euphoria's Dataset.
    Dataset<String> lines = flow.wrapped(input);

    // FlatMap processes one input element at a time and allows user code to emit
    // zero, one, or more output elements. From input lines we will get data set of words.
    Dataset<String> words =
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
    Dataset<KV<String, Long>> counted = CountByKey.named("COUNT").of(words).keyBy(w -> w).output();

    // Format output.
    Dataset<String> output =
        MapElements.named("FORMAT")
            .of(counted)
            .using(p -> p.getKey() + ": " + p.getValue())
            .output();

    // Transform Dataset back to PCollection. It can be done in any step of this flow.
    PCollection<String> outputCollection = flow.unwrapped(output);

    // Now we can again use Beam transformation. In this case we save words and their count
    // into the text file.
    outputCollection.apply(TextIO.write().to("counted_words"));

    pipeline.run();
  }

  @Test
  public void inputsAndOutputsSection() {
    BeamFlow flow = BeamFlow.of(pipeline);

    PCollection<String> input =
        pipeline
            .apply(Create.of("mouse", "rat", "elephant", "cat", "X", "duck"))
            .setTypeDescriptor(TypeDescriptor.of(String.class));

    Dataset<String> dataset = flow.wrapped(input);

    pipeline.run();
  }

  @Test
  public void addOperatorSection() {
    BeamFlow flow = BeamFlow.of(pipeline);
    Dataset<Integer> input = flow.createInput(ListDataSource.bounded(asList(1, 2, 4, 3)));

    Dataset<String> mappedElements =
        MapElements.named("Int2Str").of(input).using(String::valueOf).output();

    PAssert.that(flow.unwrapped(mappedElements)).containsInAnyOrder("1", "2", "4", "3");

    pipeline.run();
  }

  @Test
  public void metricsAndAccumulatorsSection() {
    Pipeline pipeline = Pipeline.create(PipelineUtils.getDirectPipelineOptions());

    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> flatMapped =
        FlatMap.named("FlatMap1")
            .of(dataset)
            .using(
                (String value, Collector<String> context) -> {
                  context.getCounter("my-counter").increment();
                  context.collect(value);
                })
            .output();

    Dataset<String> mapped =
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
    Pipeline pipeline = Pipeline.create(PipelineUtils.getDirectPipelineOptions());

    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    KryoCoder<AnotherElementType> beamCoder = KryoCoder.withoutClassRegistration();
    KryoCoder<ParametrizedTestDataType<String>> typeParametrizedCoder =
        KryoCoder.withoutClassRegistration();

    RegisterCoders.to(flow)
        .setKryoClassRegistrar(
            (kryo) -> {
              kryo.register(KryoSerializedElementType.class); //other may follow
            })
        .registerCoder(AnotherElementType.class, beamCoder)
        .registerCoder(
            new TypeDescriptor<ParametrizedTestDataType<String>>() {}, typeParametrizedCoder)
        .done();

    Dataset<Integer> input = Util.createMockDataset(flow, 1);

    MapElements.named("Int2Str")
        .of(input)
        .using(String::valueOf, TypeDescriptors.strings())
        .output();
  }

  @Test
  public void windowingSection() {

    BeamFlow flow = BeamFlow.of(pipeline);

    PCollection<Integer> inputPcoll =
        pipeline.apply(Create.of(1, 2, 3, 4)).setTypeDescriptor(TypeDescriptors.integers());

    Dataset<Integer> input = flow.wrapped(inputPcoll);

    Dataset<KV<Integer, Long>> countedElements =
        CountByKey.of(input)
            .keyBy(e -> e)
            .windowBy(FixedWindows.of(Duration.standardSeconds(1)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .output();

    pipeline.run();
  }

  private static class KryoSerializedElementType {}

  private static class AnotherElementType {}

  private static class ParametrizedTestDataType<T> {}

  @Test
  public void countByKeyOperator() {
    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> input = flow.createInput(ListDataSource.bounded(asList(1, 2, 4, 1, 1, 3)));

    // suppose input: [1, 2, 4, 1, 1, 3]
    Dataset<KV<Integer, Long>> output = CountByKey.of(input).keyBy(e -> e).output();
    // Output will contain:  [KV(1, 3), KV(2, 1), KV(3, 1), (4, 1)]

    PAssert.that(flow.unwrapped(output))
        .containsInAnyOrder(asList(KV.of(1, 3L), KV.of(2, 1L), KV.of(3, 1L), KV.of(4, 1L)));

    pipeline.run();
  }

  @Test
  public void distinctOperator() {

    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> input = flow.createInput(ListDataSource.bounded(asList(1, 2, 3, 3, 2, 1)));

    // suppose input: [1, 2, 3, 3, 2, 1]
    Distinct.named("unique-integers-only").of(input).output();
    // Output will contain:  1, 2, 3

    Dataset<KV<Integer, Long>> keyValueInput =
        flow.createInput(
            ListDataSource.bounded(
                asList(
                    KV.of(1, 100L),
                    KV.of(3, 100_000L),
                    KV.of(42, 10L),
                    KV.of(1, 0L),
                    KV.of(3, 0L))));

    // suppose input: [KV(1, 100L), KV(3, 100_000L), KV(42, 10L), KV(1, 0L), KV(3, 0L)]
    Dataset<Integer> uniqueKeys =
        Distinct.named("unique-keys-only").of(keyValueInput).mapped(KV::getKey).output();
    // Output will contain:  1, 3, 42

    PAssert.that(flow.unwrapped(uniqueKeys)).containsInAnyOrder(1, 3, 42);

    pipeline.run();
  }

  @Test
  public void batchJoinOperator() {

    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> left = flow.createInput(ListDataSource.bounded(asList(1, 2, 3, 0, 4, 3, 1)));
    Dataset<String> right =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    // suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
    // suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, String>> joined =
        Join.named("join-length-to-words")
            .of(left, right)
            .by(le -> le, String::length) // key extractors
            .using((Integer l, String r, Collector<String> c) -> c.collect(l + "+" + r))
            .output();

    // joined will contain: [ KV(1, "1+X"), KV(3, "3+cat"), KV(3, "3+rat"), KV(4, "4+duck"),
    // KV(3, "3+cat"), KV(3, "3+rat"), KV(1, "1+X")]

    PCollection<KV<Integer, String>> outputPCollection = flow.unwrapped(joined);
    PAssert.that(outputPCollection)
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

    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> left = flow.createInput(ListDataSource.bounded(asList(1, 2, 3, 0, 4, 3, 1)));
    Dataset<String> right =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    // suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
    // suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, String>> joined =
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

    PCollection<KV<Integer, String>> outputPCollection = flow.unwrapped(joined);
    PAssert.that(outputPCollection)
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

    // Transform to euphoria's flow.
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> left = flow.createInput(ListDataSource.bounded(asList(1, 2, 3, 0, 4, 3, 1)));
    Dataset<String> right =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    // suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
    // suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, String>> joined =
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

    PCollection<KV<Integer, String>> outputPCollection = flow.unwrapped(joined);
    PAssert.that(outputPCollection)
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

    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> left = flow.createInput(ListDataSource.bounded(asList(1, 2, 3, 0, 4, 3, 1)));
    Dataset<String> right =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    // suppose that left contains: [1, 2, 3, 0, 4, 3, 1]
    // suppose that right contains: ["mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, String>> joined =
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

    PCollection<KV<Integer, String>> outputPCollection = flow.unwrapped(joined);
    PAssert.that(outputPCollection)
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

    BeamFlow flow = BeamFlow.of(pipeline);
    Dataset<Integer> input = flow.createInput(ListDataSource.bounded(asList(0, 1, 2, 3, 4, 5)));

    // suppose inputs contains: [ 0, 1, 2, 3, 4, 5]
    Dataset<String> strings = MapElements.named("int2str").of(input).using(i -> "#" + i).output();
    // strings will contain: [ "#0", "#1", "#2", "#3", "#4", "#5"]

    PAssert.that(flow.unwrapped(strings)).containsInAnyOrder("#0", "#1", "#2", "#3", "#4", "#5");

    pipeline.run();
  }

  @Test
  public void flatMapOperator() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> words =
        flow.createInput(ListDataSource.bounded(asList("Brown", "fox", ".", "")));

    // suppose words contain: ["Brown", "fox", ".", ""]
    Dataset<String> letters =
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

    PAssert.that(flow.unwrapped(letters))
        .containsInAnyOrder("B", "r", "o", "w", "n", "f", "o", "x", ".");
    pipeline.run();
  }

  @Test
  public void flatMapWithTimeExtractorOperator() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<SomeEventObject> events =
        flow.createInput(
            ListDataSource.bounded(
                asList(
                    new SomeEventObject(0),
                    new SomeEventObject(1),
                    new SomeEventObject(2),
                    new SomeEventObject(3),
                    new SomeEventObject(4))));

    // suppose events contain events of SomeEventObject, its 'getEventTimeInMillis()' methods returns time-stamp
    Dataset<SomeEventObject> timeStampedEvents =
        FlatMap.named("extract-event-time")
            .of(events)
            .using((SomeEventObject e, Collector<SomeEventObject> c) -> c.collect(e))
            .eventTimeBy(SomeEventObject::getEventTimeInMillis)
            .output();
    //Euphoria will now know event time for each event

    //        PAssert.that(flow.unwrapped(timeStampedEvents))
    //            .inWindow(new IntervalWindow(new Instant(0), new Instant(5)));

    pipeline.run();
  }

  @Test
  public void filterOperator() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> nums =
        flow.createInput(ListDataSource.bounded(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));

    // suppose nums contains: [0,  1, 2, 3, 4, 5, 6, 7, 8, 9]
    Dataset<Integer> divisibleBythree =
        Filter.named("divisibleByFive").of(nums).by(e -> e % 3 == 0).output();
    //divisibleBythree will contain: [ 0, 3, 6, 9]

    PAssert.that(flow.unwrapped(divisibleBythree)).containsInAnyOrder(0, 3, 6, 9);
    pipeline.run();
  }

  @Test
  public void reduceByKeyTestOperator1() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> animals =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    //suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, Long>> countOfAnimalNamesByLength =
        ReduceByKey.named("to-letters-couts")
            .of(animals)
            .keyBy(String::length) // length of animal name will be used as groupping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1)
            .reduceBy(Stream::count)
            .output();
    // countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]

    PAssert.that(flow.unwrapped(countOfAnimalNamesByLength))
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  @Test
  public void reduceByKeyTestOperatorCombinable() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> animals =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    //suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, Long>> countOfAnimalNamesByLenght =
        ReduceByKey.named("to-letters-couts")
            .of(animals)
            .keyBy(String::length) // length of animal name will be used as grouping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1L)
            .combineBy(s -> s.mapToLong(l -> l).sum())
            .output();
    // countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]

    PAssert.that(flow.unwrapped(countOfAnimalNamesByLenght))
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  @Test
  public void reduceByKeyTestOperatorContext() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> animals =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    //suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, Long>> countOfAnimalNamesByLenght =
        ReduceByKey.named("to-letters-couts")
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
    // countOfAnimalNamesByLength wil contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]

    PAssert.that(flow.unwrapped(countOfAnimalNamesByLenght))
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  /**
   * Note that this one is not mentioned in documentation due to high number of RBK examples
   * and rather lower explanation value. Please consider to include it in future
   */
  @Test
  public void reduceByKeyTestOperatorContextManyOutputs() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> animals =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    Dataset<KV<Integer, Long>> countOfAnimalNamesByLenght =
        ReduceByKey.named("to-letters-couts")
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

    PAssert.that(flow.unwrapped(countOfAnimalNamesByLenght))
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
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> animals =
        flow.createInput(
            ListDataSource.bounded(asList("mouse", "rat", "elephant", "cat", "X", "duck")));

    //suppose animals contains : [ "mouse", "rat", "elephant", "cat", "X", "duck"]
    Dataset<KV<Integer, Long>> countOfAnimalNamesByLenght =
        ReduceByKey.named("to-letters-couts")
            .of(animals)
            .keyBy(String::length) // length of animal name will be used as grouping key
            // we need to count each animal name once, so why not to optimize each string to 1
            .valueBy(e -> 1L)
            .combineBy(Fold.of((l1, l2) -> l1 + l2))
            .output();
    // countOfAnimalNamesByLength will contain [ KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L) ]

    PAssert.that(flow.unwrapped(countOfAnimalNamesByLenght))
        .containsInAnyOrder(
            asList(KV.of(1, 1L), KV.of(3, 2L), KV.of(4, 1L), KV.of(5, 1L), KV.of(8, 1L)));

    pipeline.run();
  }

  @Test
  public void testSumByKeyOperator() {
    BeamFlow flow = BeamFlow.of(pipeline);
    Dataset<Integer> input =
        flow.createInput(ListDataSource.bounded(asList(1, 2, 3, 4, 5, 6, 7, 8, 9)));

    //suppose input contains: [ 1, 2, 3, 4, 5, 6, 7, 8, 9 ]
    Dataset<KV<Integer, Long>> output =
        SumByKey.named("sum-odd-and-even")
            .of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> (long) e)
            .output();
    // output will contain: [ KV.of(0, 20L), KV.of(1, 25L)]

    PAssert.that(flow.unwrapped(output)).containsInAnyOrder(asList(KV.of(0, 20L), KV.of(1, 25L)));
    pipeline.run();
  }

  @Test
  public void testUnionOperator() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<String> cats =
        flow.createInput(ListDataSource.bounded(asList("cheetah", "cat", "lynx", "jaguar")));

    Dataset<String> rodents =
        flow.createInput(
            ListDataSource.bounded(asList("squirrel", "mouse", "rat", "lemming", "beaver")));

    //suppose cats contains: [ "cheetah", "cat", "lynx", "jaguar" ]
    //suppose rodents conains: [ "squirrel", "mouse", "rat", "lemming", "beaver" ]
    Dataset<String> animals = Union.named("to-animals").of(cats, rodents).output();

    // animal will contain: "cheetah", "cat", "lynx", "jaguar", "squirrel", "mouse", "rat", "lemming", "beaver"
    PAssert.that(flow.unwrapped(animals))
        .containsInAnyOrder(
            "cheetah", "cat", "lynx", "jaguar", "squirrel", "mouse", "rat", "lemming", "beaver");

    pipeline.run();
  }

  @Test
  public void testAssignEventTimeOperator() {
    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<SomeEventObject> events =
        flow.createInput(
            ListDataSource.bounded(
                asList(
                    new SomeEventObject(0),
                    new SomeEventObject(1),
                    new SomeEventObject(2),
                    new SomeEventObject(3),
                    new SomeEventObject(4))));

    // suppose events contain events of SomeEventObject, its 'getEventTimeInMillis()' methods returns time-stamp
    Dataset<SomeEventObject> timeStampedEvents =
        AssignEventTime.named("extract-event-time")
            .of(events)
            .using(SomeEventObject::getEventTimeInMillis)
            .output();
    //Euphoria will now know event time for each event

    //    PAssert.that(flow.unwrapped(timeStampedEvents))
    //        .inWindow(new IntervalWindow(new Instant(0), new Instant(5)));

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
  public void testEuphoriaPTransformExample() {

    PCollection<String> inputs =
        pipeline.apply(
            "Create", Create.of("a", "b", "c", "A", "a", "C", "x").withCoder(StringUtf8Coder.of()));

    //suppose inputs PCollection contains: [ "a", "b", "c", "A", "a", "C", "x"]
    PCollection<KV<String, Long>> lettersWithCounts =
        inputs.apply(
            "count-uppercase-letters-in-Euphoria",
            EuphoriaPTransform.of(
                (Dataset<String> input) -> {
                  Dataset<String> upperCase =
                      MapElements.of(input)
                          .using((UnaryFunction<String, String>) String::toUpperCase)
                          .output();

                  return CountByKey.of(upperCase).keyBy(e -> e).output();
                }));
    //now the 'lettersWithCounts' will contain [ KV("A", 3L), KV("B", 1L), KV("C", 2L), KV("X", 1L) ]

    PAssert.that(lettersWithCounts)
        .containsInAnyOrder(asList(KV.of("A", 3L), KV.of("B", 1L), KV.of("C", 2L), KV.of("X", 1L)));

    pipeline.run();
  }

  @Test
  public void testReduceWithWindowOperator() {

    BeamFlow flow = BeamFlow.of(pipeline);

    Dataset<Integer> input =
        flow.createInput(ListDataSource.bounded(asList(1, 2, 3, 4, 5, 6, 7, 8)));

    //suppose input contains [ 1, 2, 3, 4, 5, 6, 7, 8 ]
    //lets assign time-stamp to each input element
    Dataset<Integer> withEventTime = AssignEventTime.of(input).using(i -> 1000L * i).output();

    Dataset<Integer> output =
        ReduceWindow.of(withEventTime)
            .combineBy(Fold.of((i1, i2) -> i1 + i2))
            .windowBy(FixedWindows.of(Duration.millis(5000)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .output();
    //output will contain: [ 10, 26 ]

    PAssert.that(flow.unwrapped(output)).containsInAnyOrder(10, 26);

    pipeline.run();
  }
}
