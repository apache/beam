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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTestStreamWithProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeduplicateTest {

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testInDifferentWindows() {
    Instant base = new Instant(0);
    TestStream<String> values =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of("k1", base),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(10))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(20))),
                TimestampedValue.of("k1", base.plus(Duration.standardSeconds(30))),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(40))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(50))),
                TimestampedValue.of("k4", base.plus(Duration.standardSeconds(60))),
                TimestampedValue.of("k5", base.plus(Duration.standardSeconds(70))),
                TimestampedValue.of("k6", base.plus(Duration.standardSeconds(80))))
            .advanceWatermarkToInfinity();

    PCollection<String> distinctValues =
        p.apply(values)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
            .apply(Deduplicate.values());
    PAssert.that(distinctValues)
        .inWindow(new IntervalWindow(base, base.plus(Duration.standardSeconds(30))))
        .containsInAnyOrder("k1", "k2", "k3");
    PAssert.that(distinctValues)
        .inWindow(
            new IntervalWindow(
                base.plus(Duration.standardSeconds(30)), base.plus(Duration.standardSeconds(60))))
        .containsInAnyOrder("k1", "k2", "k3");
    PAssert.that(distinctValues)
        .inWindow(
            new IntervalWindow(
                base.plus(Duration.standardSeconds(60)), base.plus(Duration.standardSeconds(90))))
        .containsInAnyOrder("k4", "k5", "k6");
    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testEventTime() {
    Instant base = new Instant(0);
    TestStream<String> values =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of("k1", base),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(10))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(20))),
                TimestampedValue.of("maybedup", base.plus(Duration.standardSeconds(59))))
            .advanceWatermarkTo(base.plus(Duration.standardMinutes(1)))
            .addElements(
                TimestampedValue.of("k1", base.plus(Duration.standardSeconds(30))),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(40))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(50))))
            .advanceWatermarkTo(
                base.plus(Duration.standardMinutes(1)).plus(Deduplicate.DEFAULT_DURATION))
            .addElements(TimestampedValue.of("maybedup", base.plus(Duration.standardSeconds(59))))
            .advanceWatermarkToInfinity();

    PCollection<String> distinctValues =
        p.apply(values).apply(Deduplicate.<String>values().withTimeDomain(TimeDomain.EVENT_TIME));
    PAssert.that(distinctValues)
        .satisfies(
            (Iterable<String> input) -> {
              assertEquals(1, Iterables.frequency(input, "k1"));
              assertEquals(1, Iterables.frequency(input, "k2"));
              assertEquals(1, Iterables.frequency(input, "k3"));
              assertTrue(
                  Iterables.frequency(input, "maybedup") == 1
                      || Iterables.frequency(input, "maybedup") == 2);
              return null;
            });
    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStreamWithProcessingTime.class})
  public void testProcessingTime() {
    Instant base = new Instant(0);
    TestStream<String> values =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of("k1", base),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(10))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(20))),
                TimestampedValue.of("maybedup", base.plus(Duration.standardSeconds(59))))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                TimestampedValue.of("k1", base.plus(Duration.standardSeconds(30))),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(40))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(50))))
            .advanceProcessingTime(Deduplicate.DEFAULT_DURATION)
            .addElements(TimestampedValue.of("maybedup", base.plus(Duration.standardSeconds(59))))
            .advanceWatermarkToInfinity();

    PCollection<String> distinctValues = p.apply(values).apply(Deduplicate.values());
    PAssert.that(distinctValues)
        .satisfies(
            (Iterable<String> input) -> {
              assertEquals(1, Iterables.frequency(input, "k1"));
              assertEquals(1, Iterables.frequency(input, "k2"));
              assertEquals(1, Iterables.frequency(input, "k3"));
              assertTrue(
                  Iterables.frequency(input, "maybedup") == 1
                      || Iterables.frequency(input, "maybedup") == 2);
              return null;
            });
    p.run();
  }

  private static class Keys<T> implements SerializableFunction<KV<T, String>, T> {
    @Override
    public T apply(KV<T, String> input) {
      return input.getKey();
    }
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStreamWithProcessingTime.class})
  public void testRepresentativeValuesWithCoder() {
    Instant base = new Instant(0);
    TestStream<KV<Integer, String>> values =
        TestStream.create(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(KV.of(1, "k1"), base),
                TimestampedValue.of(KV.of(2, "k2"), base.plus(Duration.standardSeconds(10))),
                TimestampedValue.of(KV.of(3, "k3"), base.plus(Duration.standardSeconds(20))))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                TimestampedValue.of(KV.of(1, "k1"), base.plus(Duration.standardSeconds(30))),
                TimestampedValue.of(KV.of(2, "k2"), base.plus(Duration.standardSeconds(40))),
                TimestampedValue.of(KV.of(3, "k3"), base.plus(Duration.standardSeconds(50))))
            .advanceWatermarkToInfinity();

    PCollection<KV<Integer, String>> distinctValues =
        p.apply(values)
            .apply(
                Deduplicate.withRepresentativeValueFn(new Keys<Integer>())
                    .withRepresentativeCoder(VarIntCoder.of()));

    PAssert.that(distinctValues).containsInAnyOrder(KV.of(1, "k1"), KV.of(2, "k2"), KV.of(3, "k3"));
    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStreamWithProcessingTime.class})
  public void testTriggeredRepresentativeValuesWithType() {
    Instant base = new Instant(0);
    TestStream<KV<Integer, String>> values =
        TestStream.create(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(KV.of(1, "k1"), base),
                TimestampedValue.of(KV.of(2, "k2"), base.plus(Duration.standardSeconds(10))),
                TimestampedValue.of(KV.of(3, "k3"), base.plus(Duration.standardSeconds(20))))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                TimestampedValue.of(KV.of(1, "k1"), base.plus(Duration.standardSeconds(30))),
                TimestampedValue.of(KV.of(2, "k2"), base.plus(Duration.standardSeconds(40))),
                TimestampedValue.of(KV.of(3, "k3"), base.plus(Duration.standardSeconds(50))))
            .advanceWatermarkToInfinity();

    PCollection<KV<Integer, String>> distinctValues =
        p.apply(values)
            .apply(
                Deduplicate.withRepresentativeValueFn(new Keys<Integer>())
                    .withRepresentativeCoder(VarIntCoder.of()));

    PAssert.that(distinctValues).containsInAnyOrder(KV.of(1, "k1"), KV.of(2, "k2"), KV.of(3, "k3"));
    p.run();
  }

  @Test
  public void withLambdaRepresentativeValuesFnNoTypeDescriptorShouldThrow() {

    Multimap<Integer, String> predupedContents = HashMultimap.create();
    predupedContents.put(3, "foo");
    predupedContents.put(4, "foos");
    predupedContents.put(6, "barbaz");
    predupedContents.put(6, "bazbar");
    PCollection<String> dupes =
        p.apply(Create.of("foo", "foos", "barbaz", "barbaz", "bazbar", "foo"));

    assertThrows(
        "Unable to return a default Coder for RemoveRepresentativeDupes",
        IllegalStateException.class,
        () ->
            dupes.apply(
                "RemoveRepresentativeDupes",
                Deduplicate.withRepresentativeValueFn(String::length)));
  }
}
