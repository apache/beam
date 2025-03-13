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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesImpulse;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PeriodicSequence. */
@RunWith(JUnit4.class)
public class PeriodicSequenceTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  public static class ExtractTsDoFn<InputT>
      extends DoFn<InputT, TimestampedValue<KV<InputT, Instant>>> {

    @ProcessElement
    public void processElement(
        @Element InputT element,
        @Timestamp Instant ts,
        OutputReceiver<TimestampedValue<KV<InputT, Instant>>> output)
        throws Exception {

      output.output(TimestampedValue.of(KV.of(element, ts), Instant.now()));
    }
  }

  @Test
  @Category({
    NeedsRunner.class,
    UsesImpulse.class,
    UsesStatefulParDo.class,
    UsesUnboundedPCollections.class,
    UsesUnboundedSplittableParDo.class
  })
  public void testOutputsProperElements() {
    Instant startTime = Instant.now().plus(Duration.standardSeconds(2));
    Duration interval = Duration.millis(250);
    long intervalMillis = interval.getMillis();
    long duration = 3 * intervalMillis;
    Instant stopTime = startTime.plus(Duration.millis(duration));

    PCollection<TimestampedValue<KV<Instant, Instant>>> result =
        p.apply(Create.of(new PeriodicSequence.SequenceDefinition(startTime, stopTime, interval)))
            .apply(PeriodicSequence.create())
            .apply(
                Window.<Instant>into(FixedWindows.of(interval))
                    .withTimestampCombiner(TimestampCombiner.EARLIEST))
            .apply(WithKeys.of("dummy"))
            .apply(GroupByKey.create())
            .apply(
                MapElements.into(TypeDescriptor.of(Instant.class))
                    .via(e -> Iterables.getOnlyElement(e.getValue())))
            .apply(ParDo.of(new ExtractTsDoFn<>())); // validate timestamps

    ArrayList<Instant> expectedResults = new ArrayList<>();
    for (long i = 0; i <= duration; i += intervalMillis) {
      Instant el = startTime.plus(Duration.millis(i));
      expectedResults.add(el);
    }

    PAssert.that(result)
        .satisfies(
            values -> {
              List<TimestampedValue<KV<Instant, Instant>>> sortedValues =
                  Streams.stream(values)
                      .sorted(Comparator.comparing(e -> e.getValue().getValue()))
                      .collect(Collectors.toList());

              assertEquals(
                  expectedResults,
                  sortedValues.stream()
                      .map(e -> e.getValue().getValue())
                      .collect(Collectors.toList()));

              Instant minTs =
                  sortedValues.stream()
                      .min(Comparator.comparing(TimestampedValue::getTimestamp))
                      .get()
                      .getTimestamp();
              Instant maxTs =
                  sortedValues.stream()
                      .max(Comparator.comparing(TimestampedValue::getTimestamp))
                      .get()
                      .getTimestamp();
              final long expectedDiff = intervalMillis / 2;
              assertTrue(
                  String.format(
                      "Expected processing-time diff at least %d, got %d",
                      expectedDiff, maxTs.getMillis() - minTs.getMillis()),
                  maxTs.getMillis() - minTs.getMillis() > expectedDiff);
              return null;
            });

    p.run().waitUntilFinish();
  }

  @Test
  public void testBacklogBytes() {
    assertEquals(
        0, PeriodicSequence.sequenceBacklogBytes(10, 100, new OffsetRange(100, Long.MAX_VALUE)));
    assertEquals(
        8, PeriodicSequence.sequenceBacklogBytes(10, 100, new OffsetRange(90, Long.MAX_VALUE)));
    assertEquals(
        0, PeriodicSequence.sequenceBacklogBytes(10, 100, new OffsetRange(91, Long.MAX_VALUE)));
    assertEquals(
        8, PeriodicSequence.sequenceBacklogBytes(10, 100, new OffsetRange(89, Long.MAX_VALUE)));
    assertEquals(
        16, PeriodicSequence.sequenceBacklogBytes(10, 101, new OffsetRange(81, Long.MAX_VALUE)));
    assertEquals(
        8 * 10000 / 100,
        PeriodicSequence.sequenceBacklogBytes(100, 10000, new OffsetRange(0, Long.MAX_VALUE)));
    assertEquals(
        0, PeriodicSequence.sequenceBacklogBytes(10, 10000, new OffsetRange(10011, 10025)));
    assertEquals(
        8, PeriodicSequence.sequenceBacklogBytes(10, 10100, new OffsetRange(10011, 10025)));
  }
}
