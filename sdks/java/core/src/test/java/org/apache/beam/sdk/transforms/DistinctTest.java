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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Distinct}. */
@RunWith(JUnit4.class)
public class DistinctTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();


  @Test
  @Category(ValidatesRunner.class)
  public void testDistinct() {
    List<String> strings = Arrays.asList("k1", "k5", "k5", "k2", "k1", "k2", "k3");

    PCollection<String> input = p.apply(Create.of(strings).withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(Distinct.create());

    PAssert.that(output).containsInAnyOrder("k1", "k5", "k2", "k3");
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testDistinctEmpty() {
    List<String> strings = Arrays.asList();

    PCollection<String> input = p.apply(Create.of(strings).withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(Distinct.create());

    PAssert.that(output).empty();
    p.run();
  }

  private static class Keys<T> implements SerializableFunction<KV<T, String>, T> {
    @Override
    public T apply(KV<T, String> input) {
      return input.getKey();
    }
  }

  private static class Checker implements SerializableFunction<Iterable<KV<String, String>>, Void> {
    @Override
    public Void apply(Iterable<KV<String, String>> input) {
      Map<String, String> values = new HashMap<>();
      for (KV<String, String> kv : input) {
        values.put(kv.getKey(), kv.getValue());
      }
      assertEquals(2, values.size());
      assertTrue("v1".equals(values.get("k1")) || "v2".equals(values.get("k1")));
      assertEquals("v1", values.get("k2"));
      return null;
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testDistinctWithRepresentativeValue() {
    List<KV<String, String>> strings =
        Arrays.asList(KV.of("k1", "v1"), KV.of("k1", "v2"), KV.of("k2", "v1"));

    PCollection<KV<String, String>> input = p.apply(Create.of(strings));

    PCollection<KV<String, String>> output =
        input.apply(
            Distinct.withRepresentativeValueFn(new Keys<String>())
                .withRepresentativeType(TypeDescriptor.of(String.class)));

    PAssert.that(output).satisfies(new Checker());

    p.run();
  }

  @Rule public TestPipeline windowedDistinctPipeline = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesTestStream.class})
  public void testWindowedDistinct() {
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
        windowedDistinctPipeline
            .apply(values)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
            .apply(Distinct.create());
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
    windowedDistinctPipeline.run();
  }

  @Rule public TestPipeline triggeredDistinctPipeline = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesTestStream.class})
  public void testTriggeredDistinct() {
    Instant base = new Instant(0);
    TestStream<String> values =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of("k1", base),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(10))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(20))))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(
                TimestampedValue.of("k1", base.plus(Duration.standardSeconds(30))),
                TimestampedValue.of("k2", base.plus(Duration.standardSeconds(40))),
                TimestampedValue.of("k3", base.plus(Duration.standardSeconds(50))))
            .advanceWatermarkToInfinity();

    PCollection<String> distinctValues =
        triggeredDistinctPipeline
            .apply(values)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(30))))
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes())
            .apply(Distinct.create());
    PAssert.that(distinctValues).containsInAnyOrder("k1", "k2", "k3");
    triggeredDistinctPipeline.run();
  }

  @Rule public TestPipeline triggeredDistinctRepresentativePipeline = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesTestStream.class})
  public void testTriggeredDistinctRepresentativeValues() {
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
        triggeredDistinctRepresentativePipeline
            .apply(values)
            .apply(
                Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(30))))
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes())
            .apply(
                Distinct.withRepresentativeValueFn(new Keys<Integer>())
                    .withRepresentativeType(TypeDescriptor.of(Integer.class)));

    PAssert.that(distinctValues).containsInAnyOrder(KV.of(1, "k1"), KV.of(2, "k2"), KV.of(3, "k3"));
    triggeredDistinctRepresentativePipeline.run();
  }

  /**
   * Regression test: when all values are emitted by a speculative trigger, caused a null KV when
   * the on-time firing occurred.
   */
  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testTriggeredDistinctRepresentativeValuesEmpty() {
    Instant base = new Instant(0);
    TestStream<KV<Integer, String>> values =
        TestStream.create(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(KV.of(1, "k1"), base))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .advanceWatermarkToInfinity();

    PCollection<KV<Integer, String>> distinctValues =
        triggeredDistinctRepresentativePipeline
            .apply(values)
            .apply(
                Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(Duration.standardSeconds(30))))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(
                Distinct.withRepresentativeValueFn(new Keys<Integer>())
                    .withRepresentativeType(TypeDescriptor.of(Integer.class)));

    PAssert.that(distinctValues).containsInAnyOrder(KV.of(1, "k1"));
    triggeredDistinctRepresentativePipeline.run();
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

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to return a default Coder for RemoveRepresentativeDupes");

    // Thrown when applying a transform to the internal WithKeys that withRepresentativeValueFn is
    // implemented with
    dupes.apply("RemoveRepresentativeDupes", Distinct.withRepresentativeValueFn(String::length));
  }

  @Test
  @Category(NeedsRunner.class)
  public void withLambdaRepresentativeValuesFnAndTypeDescriptorShouldApplyFn() {

    PCollection<String> dupes =
        p.apply(Create.of("foo", "foos", "barbaz", "barbaz", "bazbar", "foo"));
    PCollection<String> deduped =
        dupes.apply(
            Distinct.withRepresentativeValueFn(String::length)
                .withRepresentativeType(TypeDescriptor.of(Integer.class)));

    PAssert.that(deduped).satisfies((Iterable<String> strs) -> {
      Multimap<Integer, String> predupedContents = HashMultimap.create();
      predupedContents.put(3, "foo");
      predupedContents.put(4, "foos");
      predupedContents.put(6, "barbaz");
      predupedContents.put(6, "bazbar");

      Set<Integer> seenLengths = new HashSet<>();
      for (String s : strs) {
        assertThat(predupedContents.values(), hasItem(s));
        assertThat(seenLengths, not(contains(s.length())));
        seenLengths.add(s.length());
      }
      return null;
    });

    p.run();
  }
}
