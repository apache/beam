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
package org.apache.beam.sdk.transforms.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for BiTemporalJoin. Implements Serializable for anonymous DoFns. */
@RunWith(JUnit4.class)
public class EventTimeEquiJoinTest implements Serializable {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  /**
   * Creates a {@code PCollection<KV<Integer, Integer>>} from a {@code List<KV<Integer, Integer>>}
   * and uses the values as timestamps.
   */
  PCollection<KV<Integer, Integer>> createIntegers(List<KV<Integer, Integer>> kvs) {
    ArrayList<Long> timestamps = new ArrayList<>();
    for (KV<Integer, Integer> kv : kvs) {
      timestamps.add(Long.valueOf(kv.getValue()));
    }
    return p.apply(
        "Create ints",
        Create.timestamped(kvs, timestamps)
            .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), BigEndianIntegerCoder.of())));
  }

  /**
   * Creates a {@code PCollection<KV<Integer, String>>} from a {@code List<KV<Integer, Integer>>} by
   * using the values as timestamps and then converting them to strings.
   */
  PCollection<KV<Integer, String>> createStrings(List<KV<Integer, Integer>> kvs) {
    ArrayList<KV<Integer, String>> results = new ArrayList<>();
    ArrayList<Long> timestamps = new ArrayList<>();
    for (KV<Integer, Integer> kv : kvs) {
      results.add(KV.of(kv.getKey(), kv.getValue().toString()));
      timestamps.add(Long.valueOf(kv.getValue()));
    }
    return p.apply(
        "Create strings",
        Create.timestamped(results, timestamps)
            .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testZeroDelayJoin() {

    List<KV<Integer, Integer>> kvs =
        Arrays.asList(KV.of(1, 0), KV.of(2, 2), KV.of(1, 4), KV.of(1, 6), KV.of(2, 8));

    PCollection<KV<Integer, String>> listOne = createStrings(kvs);
    PCollection<KV<Integer, Integer>> listTwo = createIntegers(kvs);

    PCollection<KV<Integer, Pair<String, Integer>>> result =
        listOne.apply(EventTimeEquiJoin.of(listTwo));

    List<KV<Integer, Pair<String, Integer>>> expected =
        Arrays.asList(
            KV.of(1, Pair.of("0", 0)),
            KV.of(2, Pair.of("2", 2)),
            KV.of(1, Pair.of("4", 4)),
            KV.of(1, Pair.of("6", 6)),
            KV.of(2, Pair.of("8", 8)));

    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testAllowedDelay() {
    List<KV<Integer, Integer>> kvs =
        Arrays.asList(KV.of(1, 0), KV.of(2, 2), KV.of(1, 4), KV.of(1, 6), KV.of(2, 8));

    PCollection<KV<Integer, String>> listOne = createStrings(kvs);
    PCollection<KV<Integer, Integer>> listTwo = createIntegers(kvs);

    PCollection<KV<Integer, Pair<String, Integer>>> result =
        listOne.apply(
            EventTimeEquiJoin.<Integer, String, Integer>of(listTwo).within(new Duration(4L)));

    List<KV<Integer, Pair<String, Integer>>> expected =
        Arrays.asList(
            KV.of(1, Pair.of("0", 0)),
            KV.of(1, Pair.of("0", 4)),
            KV.of(2, Pair.of("2", 2)),
            KV.of(1, Pair.of("4", 0)),
            KV.of(1, Pair.of("4", 4)),
            KV.of(1, Pair.of("4", 6)),
            KV.of(1, Pair.of("6", 4)),
            KV.of(1, Pair.of("6", 6)),
            KV.of(2, Pair.of("8", 8)));

    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testFirstCollectionValidFor() {

    PCollection<KV<Integer, String>> listOne = createStrings(Arrays.asList(KV.of(1, 4)));
    PCollection<KV<Integer, Integer>> listTwo =
        createIntegers(Arrays.asList(KV.of(1, 0), KV.of(1, 4), KV.of(1, 8)));

    // First collection matches with following seconds for up to 4 ms.
    PCollection<KV<Integer, Pair<String, Integer>>> result =
        listOne.apply(
            EventTimeEquiJoin.<Integer, String, Integer>of(listTwo)
                .within(new Duration(4L), Duration.ZERO));

    PAssert.that(result)
        .containsInAnyOrder(Arrays.asList(KV.of(1, Pair.of("4", 4)), KV.of(1, Pair.of("4", 8))));
    p.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testSecondCollectionValidFor() {

    PCollection<KV<Integer, String>> listOne =
        createStrings(Arrays.asList(KV.of(1, 0), KV.of(1, 4), KV.of(1, 8)));
    PCollection<KV<Integer, Integer>> listTwo = createIntegers(Arrays.asList(KV.of(1, 4)));

    // Second collection matches with following seconds for up to 4 ms.
    PCollection<KV<Integer, Pair<String, Integer>>> result =
        listOne.apply(
            EventTimeEquiJoin.<Integer, String, Integer>of(listTwo)
                .within(Duration.ZERO, new Duration(4L)));

    PAssert.that(result)
        .containsInAnyOrder(Arrays.asList(KV.of(1, Pair.of("4", 4)), KV.of(1, Pair.of("8", 4))));
    p.run();
  }

  /**
   * Runs a test with a very late element. If useAllowedLateness then the element should be allowed.
   */
  public void runCleanupTest(boolean useAllowedLateness) {
    Coder<KV<Integer, String>> coder = KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of());
    TestStream<KV<Integer, String>> createEvents =
        TestStream.create(coder)
            .addElements(TimestampedValue.of(KV.of(1, "1"), new Instant(1L)))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(3)))
            .addElements(TimestampedValue.of(KV.of(1, "3"), new Instant(3L))) // Late element
            .advanceWatermarkToInfinity();

    PCollection<KV<Integer, Integer>> listTwo = createIntegers(Arrays.asList(KV.of(1, 3)));

    EventTimeEquiJoin<Integer, String, Integer> join =
        EventTimeEquiJoin.<Integer, String, Integer>of(listTwo).within(new Duration(4L));

    List<KV<Integer, Pair<String, Integer>>> expected = new ArrayList<>();
    expected.add(KV.of(1, Pair.of("1", 3)));

    if (useAllowedLateness) {
      join = join.withAllowedLateness(Duration.standardMinutes(4));
      // 3 is late, but within allowed lateness.
      expected.add(KV.of(1, Pair.of("3", 3)));
    }

    PAssert.that(p.apply(createEvents).apply(join)).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void lateElementsAreCleanedUp() {
    runCleanupTest(false);
  }

  @Test
  @Category({ValidatesRunner.class})
  public void lateElementsAllowedLateness() {
    runCleanupTest(true);
  }
}
