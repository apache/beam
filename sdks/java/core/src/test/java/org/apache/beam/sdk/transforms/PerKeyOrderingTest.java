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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesPerKeyOrderInBundle;
import org.apache.beam.sdk.testing.UsesPerKeyOrderedDelivery;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@SuppressWarnings(
    "unused") // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
// errorprone is released (2.11.0)
@RunWith(JUnit4.class)
public class PerKeyOrderingTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static class VerifyDoFn<T> extends DoFn<KV<String, T>, KV<String, Boolean>> {

    private final List<T> perKeyElements;

    VerifyDoFn(List<T> perKeyElements) {
      this.perKeyElements = perKeyElements;
    }

    @StateId("matchedElements")
    private final StateSpec<ValueState<Integer>> elementsMatchedState = StateSpecs.value();

    @ProcessElement
    public void verifyOrder(
        @Element KV<String, T> elm,
        OutputReceiver<KV<String, Boolean>> receiver,
        @StateId("matchedElements") ValueState<Integer> matchedElements) {
      Integer matched = matchedElements.read();
      matched = matched == null ? 0 : matched;
      if (matched == -1) {
        // When matched is set to -1, it means that we have met an error, and elements on this
        // key are not matched anymore - thus we ignore all inputs.
        return;
      } else if (matched < this.perKeyElements.size()
          && !this.perKeyElements.get(matched).equals(elm.getValue())) {
        // If we meet this condition, then the order of elements is not what we're expecting.
        // We mark `matched` as -1, and output a failed ordering.
        matchedElements.write(-1);
        receiver.output(KV.of(elm.getKey(), false));
      } else if (matched >= this.perKeyElements.size()) {
        throw new RuntimeException("Got more elements than expected!");
      } else {
        assert this.perKeyElements.get(matched).equals(elm.getValue())
            : String.format("Element %s is not expected %s", elm, this.perKeyElements.get(matched));
        matchedElements.write(matched + 1);
        // If we reached the end of perKeyElements, it means that all elements have been emitted in
        // the expected order, and thus we mark this Key as successful.
        if (matched + 1 == perKeyElements.size()) {
          receiver.output(KV.of(elm.getKey(), true));
        }
      }
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesPerKeyOrderedDelivery.class, UsesStatefulParDo.class})
  public void testSingleCallOrderingWithShuffle() {
    // Here we test that the output of a single process call in a DoFn will be output in order
    List<Integer> perKeyElements =
        Lists.newArrayList(-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 123456789);
    Collections.shuffle(perKeyElements);
    List<String> allKeys =
        Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream()
            .map(elm -> String.format("k%s", elm))
            .collect(Collectors.toList());
    PCollection<KV<String, Integer>> kvSeeds =
        pipeline
            .apply("Generate all keys", Create.of(allKeys))
            .apply(
                "Map into KV pairs",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .via(key -> KV.of(key, 0)))
            .apply("Shuffle by key", Reshuffle.of());

    PCollection<KV<String, Boolean>> result =
        kvSeeds
            .apply(
                "Generate ordered values per key",
                FlatMapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .via(
                        input -> {
                          return perKeyElements.stream()
                              .map(number -> KV.of(input.getKey(), number))
                              .collect(Collectors.toList());
                        }))
            .apply("Reshuffle", Reshuffle.of())
            .apply("Verify", ParDo.of(new VerifyDoFn<>(perKeyElements)));

    PAssert.that(result)
        .containsInAnyOrder(allKeys.stream().map(k -> KV.of(k, true)).collect(Collectors.toList()));
    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesPerKeyOrderInBundle.class, UsesStatefulParDo.class})
  public void testSingleCallOrderingWithoutShuffle() {
    // Here we test that the output of a single process call in a DoFn will be output in order
    List<Long> perKeyElements =
        Lists.newArrayList(
            -8L,
            -7L,
            -6L,
            -5L,
            -4L,
            -3L,
            -2L,
            -1L,
            0L,
            1L,
            2L,
            3L,
            4L,
            5L,
            6L,
            7L,
            8L,
            178907878346L);
    Collections.shuffle(perKeyElements);
    List<String> allKeys =
        Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream()
            .map(elm -> String.format("k%s", elm))
            .collect(Collectors.toList());
    PCollection<KV<String, Long>> kvSeeds =
        pipeline
            .apply("Generate all keys", Create.of(allKeys))
            .apply(
                "Map into KV pairs",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                    .via(key -> KV.of(key, 0L)))
            .apply("Shuffle by key", Reshuffle.of());

    PCollection<KV<String, Boolean>> result =
        kvSeeds
            .apply(
                "Generate ordered values per key",
                FlatMapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                    .via(
                        input -> {
                          return perKeyElements.stream()
                              .map(number -> KV.of(input.getKey(), number))
                              .collect(Collectors.toList());
                        }))
            .apply("Verify", ParDo.of(new VerifyDoFn<>(perKeyElements)));

    PAssert.that(result)
        .containsInAnyOrder(allKeys.stream().map(k -> KV.of(k, true)).collect(Collectors.toList()));
    pipeline.run();
  }

  private static class StatefulOrderedGenerator<T> extends DoFn<KV<String, ?>, KV<String, T>> {
    private final List<T> perKeyElements;

    StatefulOrderedGenerator(List<T> perKeyElements) {
      this.perKeyElements = perKeyElements;
    }

    @StateId("outputElements")
    private final StateSpec<ValueState<Integer>> elementsPointerState = StateSpecs.value();

    @ProcessElement
    public void trigger(
        @Element KV<String, ?> elm,
        @StateId("outputElements") ValueState<Integer> indexState,
        OutputReceiver<KV<String, T>> receiver) {
      Integer index = indexState.read();
      index = index == null ? 0 : index;
      if (index >= perKeyElements.size()) {
        return;
      }
      receiver.output(KV.of(elm.getKey(), perKeyElements.get(index)));
      indexState.write(index + 1);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesPerKeyOrderedDelivery.class, UsesStatefulParDo.class})
  public void testMultipleStatefulOrderingWithShuffle() {
    // Here we test that the output of a single process call in a DoFn will be output in order
    List<Long> perKeyElements =
        Lists.newArrayList(
            -8L,
            -7L,
            -6L,
            -5L,
            -4L,
            -3L,
            -2L,
            -1L,
            0L,
            1L,
            2L,
            3L,
            4L,
            5L,
            6L,
            7L,
            8L,
            -178907878346L);
    Collections.shuffle(perKeyElements);
    List<String> allKeys =
        Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream()
            .map(elm -> String.format("k%s", elm))
            .collect(Collectors.toList());
    PCollection<KV<String, Long>> kvSeeds =
        pipeline
            .apply(
                "Periodic impulse",
                PeriodicImpulse.create()
                    .startAt(Instant.ofEpochMilli(0))
                    .withInterval(Duration.standardSeconds(1))
                    .stopAt(
                        Instant.ofEpochMilli(0)
                            .plus(Duration.standardSeconds(perKeyElements.size()))))
            .apply(
                "Generate all keys",
                FlatMapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                    .via(
                        elm ->
                            allKeys.stream().map(k -> KV.of(k, 0L)).collect(Collectors.toList())))
            .apply("Shuffle by key", Reshuffle.of());

    PCollection<KV<String, Boolean>> result =
        kvSeeds
            .apply(
                "Generate ordered values per key",
                ParDo.of(new StatefulOrderedGenerator<Long>(perKeyElements)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
            .apply("Reshuffle", Reshuffle.of())
            .apply("Verify", ParDo.of(new VerifyDoFn<Long>(perKeyElements)));

    PAssert.that(result)
        .containsInAnyOrder(allKeys.stream().map(k -> KV.of(k, true)).collect(Collectors.toList()));
    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesPerKeyOrderInBundle.class, UsesStatefulParDo.class})
  public void testMultipleStatefulOrderingWithoutShuffle() {
    List<Integer> perKeyElements =
        Lists.newArrayList(-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 123456789);
    Collections.shuffle(perKeyElements);
    List<String> allKeys =
        Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream()
            .map(elm -> String.format("k%s", elm))
            .collect(Collectors.toList());
    PCollection<KV<String, Integer>> kvSeeds =
        pipeline
            .apply(
                "Periodic impulse",
                PeriodicImpulse.create()
                    .startAt(Instant.ofEpochMilli(0))
                    .withInterval(Duration.standardSeconds(1))
                    .stopAt(
                        Instant.ofEpochMilli(0)
                            .plus(Duration.standardSeconds(perKeyElements.size()))))
            .apply(
                "Generate all keys",
                FlatMapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .via(
                        elm ->
                            allKeys.stream().map(k -> KV.of(k, 0)).collect(Collectors.toList())));

    PCollection<KV<String, Boolean>> result =
        kvSeeds
            .apply(
                "Generate ordered values per key",
                ParDo.of(new StatefulOrderedGenerator<Integer>(perKeyElements)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .apply("Verify", ParDo.of(new VerifyDoFn<Integer>(perKeyElements)));

    PAssert.that(result)
        .containsInAnyOrder(allKeys.stream().map(k -> KV.of(k, true)).collect(Collectors.toList()));
    pipeline.run();
  }
}
