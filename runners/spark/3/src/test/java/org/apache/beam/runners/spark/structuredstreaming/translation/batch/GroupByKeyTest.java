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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link ParDo} translation. */
@RunWith(JUnit4.class)
public class GroupByKeyTest implements Serializable {
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.fromOptions(SESSION.createPipelineOptions());

  @Test
  public void testGroupByKeyPreservesWindowing() {
    pipeline
        .apply(
            Create.timestamped(
                shuffleRandomly(
                    TimestampedValue.of(KV.of(1, 1), new Instant(1)),
                    TimestampedValue.of(KV.of(1, 3), new Instant(2)),
                    TimestampedValue.of(KV.of(1, 5), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 2), new Instant(3)),
                    TimestampedValue.of(KV.of(2, 4), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 6), new Instant(12)))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(GroupByKey.create())
        // Passert do not support multiple kv with same key (because multiple windows)
        .apply(
            ParDo.of(
                new AssertContains<>(
                    KV.of(1, containsInAnyOrder(1, 3)), // window [0-10)
                    KV.of(1, containsInAnyOrder(5)), // window [10-20)
                    KV.of(2, containsInAnyOrder(4, 6)), // window [10-20)
                    KV.of(2, containsInAnyOrder(2)) // window [0-10)
                    )));
    pipeline.run();
  }

  @Test
  public void testGroupByKeyExplodesMultipleWindows() {
    pipeline
        .apply(
            Create.timestamped(
                shuffleRandomly(
                    TimestampedValue.of(KV.of(1, 1), new Instant(5)),
                    TimestampedValue.of(KV.of(1, 3), new Instant(7)),
                    TimestampedValue.of(KV.of(1, 5), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 2), new Instant(5)),
                    TimestampedValue.of(KV.of(2, 4), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 6), new Instant(12)))))
        .apply(Window.into(SlidingWindows.of(Duration.millis(10)).every(Duration.millis(5))))
        .apply(GroupByKey.create())
        // Passert do not support multiple kv with same key (because multiple windows)
        .apply(
            ParDo.of(
                new AssertContains<>(
                    KV.of(1, containsInAnyOrder(1, 3)), // window [0-10)
                    KV.of(1, containsInAnyOrder(1, 3, 5)), // window [5-15)
                    KV.of(1, containsInAnyOrder(5)), // window [10-20)
                    KV.of(2, containsInAnyOrder(2)), // window [0-10)
                    KV.of(2, containsInAnyOrder(2, 4, 6)), // window [5-15)
                    KV.of(2, containsInAnyOrder(4, 6)) // window [10-20)
                    )));
    pipeline.run();
  }

  @Test
  public void testGroupByKeyWithMergingWindows() {
    pipeline
        .apply(
            Create.timestamped(
                shuffleRandomly(
                    TimestampedValue.of(KV.of(1, 1), new Instant(5)),
                    TimestampedValue.of(KV.of(1, 3), new Instant(7)),
                    TimestampedValue.of(KV.of(1, 5), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 2), new Instant(5)),
                    TimestampedValue.of(KV.of(2, 4), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 6), new Instant(12)))))
        .apply(Window.into(Sessions.withGapDuration(Duration.millis(5))))
        .apply(GroupByKey.create())
        // Passert do not support multiple kv with same key (because multiple windows)
        .apply(
            ParDo.of(
                new AssertContains<>(
                    KV.of(1, containsInAnyOrder(1, 3, 5)), // window [5-16)
                    KV.of(2, containsInAnyOrder(2)), // window [5-10)
                    KV.of(2, containsInAnyOrder(4, 6)) // window [11-17)
                    )));
    pipeline.run();
  }

  @Test
  public void testGroupByKey() {
    List<KV<Integer, Integer>> elems =
        shuffleRandomly(
            KV.of(1, 1), KV.of(1, 3), KV.of(1, 5), KV.of(2, 2), KV.of(2, 4), KV.of(2, 6));

    PCollection<KV<Integer, Iterable<Integer>>> input =
        pipeline.apply(Create.of(elems)).apply(GroupByKey.create());

    PAssert.thatMap(input)
        .satisfies(
            results -> {
              assertThat(results.get(1), containsInAnyOrder(1, 3, 5));
              assertThat(results.get(2), containsInAnyOrder(2, 4, 6));
              return null;
            });
    pipeline.run();
  }

  static class AssertContains<K, V> extends DoFn<KV<K, Iterable<V>>, Void> {
    private final Map<K, List<SerializableMatcher<Iterable<? extends V>>>> byKey;

    public AssertContains(KV<K, SerializableMatcher<Iterable<? extends V>>>... matchers) {
      byKey = stream(matchers).collect(groupingBy(KV::getKey, mapping(KV::getValue, toList())));
    }

    @ProcessElement
    public void processElement(@Element KV<K, Iterable<V>> elem) {
      assertThat("Unexpected key: " + elem.getKey(), byKey.containsKey(elem.getKey()));
      List<V> values = ImmutableList.copyOf(elem.getValue());
      assertThat(
          "Unexpected values " + values + " for key " + elem.getKey(),
          byKey.get(elem.getKey()).stream().anyMatch(m -> m.matches(values)));
    }
  }

  private <T> List<T> shuffleRandomly(T... elems) {
    ArrayList<T> list = Lists.newArrayList(elems);
    Collections.shuffle(list);
    return list;
  }
}
