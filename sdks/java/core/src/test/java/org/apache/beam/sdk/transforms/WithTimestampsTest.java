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

import static org.hamcrest.Matchers.isA;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WithTimestamps}. */
@RunWith(JUnit4.class)
public class WithTimestampsTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(ValidatesRunner.class)
  public void withTimestampsShouldApplyTimestamps() {

    SerializableFunction<String, Instant> timestampFn = input -> new Instant(Long.valueOf(input));

    String yearTwoThousand = "946684800000";
    PCollection<String> timestamped =
        p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
            .apply(WithTimestamps.of(timestampFn));

    PCollection<KV<String, Instant>> timestampedVals =
        timestamped.apply(
            ParDo.of(
                new DoFn<String, KV<String, Instant>>() {
                  @ProcessElement
                  public void processElement(DoFn<String, KV<String, Instant>>.ProcessContext c)
                      throws Exception {
                    c.output(KV.of(c.element(), c.timestamp()));
                  }
                }));

    PAssert.that(timestamped)
        .containsInAnyOrder(yearTwoThousand, "0", "1234", Integer.toString(Integer.MAX_VALUE));
    PAssert.that(timestampedVals)
        .containsInAnyOrder(
            KV.of("0", new Instant(0)),
            KV.of("1234", new Instant(1234L)),
            KV.of(Integer.toString(Integer.MAX_VALUE), new Instant(Integer.MAX_VALUE)),
            KV.of(yearTwoThousand, new Instant(Long.valueOf(yearTwoThousand))));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void withTimestampsBackwardsInTimeShouldThrow() {

    SerializableFunction<String, Instant> timestampFn = input -> new Instant(Long.valueOf(input));
    SerializableFunction<String, Instant> backInTimeFn =
        input -> new Instant(Long.valueOf(input)).minus(Duration.millis(1000L));

    String yearTwoThousand = "946684800000";

    p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
        .apply("WithTimestamps", WithTimestamps.of(timestampFn))
        .apply("AddSkew", WithTimestamps.of(backInTimeFn));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("no earlier than the timestamp of the current input");

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void withTimestampsBackwardsInTimeAndWithAllowedTimestampSkewShouldSucceed() {

    SerializableFunction<String, Instant> timestampFn = input -> new Instant(Long.valueOf(input));

    final Duration skew = Duration.millis(1000L);
    SerializableFunction<String, Instant> backInTimeFn =
        input -> new Instant(Long.valueOf(input)).minus(skew);

    String yearTwoThousand = "946684800000";
    PCollection<String> timestampedWithSkew =
        p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
            .apply("FirstTimestamp", WithTimestamps.of(timestampFn))
            .apply(
                "WithSkew",
                WithTimestamps.of(backInTimeFn)
                    .withAllowedTimestampSkew(skew.plus(Duration.millis(100L))));

    PCollection<KV<String, Instant>> timestampedVals =
        timestampedWithSkew.apply(
            ParDo.of(
                new DoFn<String, KV<String, Instant>>() {
                  @ProcessElement
                  public void processElement(DoFn<String, KV<String, Instant>>.ProcessContext c)
                      throws Exception {
                    c.output(KV.of(c.element(), c.timestamp()));
                  }
                }));

    PAssert.that(timestampedWithSkew)
        .containsInAnyOrder(yearTwoThousand, "0", "1234", Integer.toString(Integer.MAX_VALUE));
    PAssert.that(timestampedVals)
        .containsInAnyOrder(
            KV.of("0", new Instant(0L).minus(skew)),
            KV.of("1234", new Instant(1234L).minus(skew)),
            KV.of(
                Integer.toString(Integer.MAX_VALUE),
                new Instant(Long.valueOf(Integer.MAX_VALUE)).minus(skew)),
            KV.of(yearTwoThousand, new Instant(Long.valueOf(yearTwoThousand)).minus(skew)));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void withTimestampsWithNullTimestampShouldThrow() {
    SerializableFunction<String, Instant> timestampFn = input -> null;

    String yearTwoThousand = "946684800000";
    p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
        .apply(WithTimestamps.of(timestampFn));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(NullPointerException.class));
    thrown.expectMessage("WithTimestamps");
    thrown.expectMessage("cannot be null");

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void withTimestampsWithNullFnShouldThrowOnConstruction() {

    SerializableFunction<String, Instant> timestampFn = null;

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("WithTimestamps fn cannot be null");

    p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE)))
        .apply(WithTimestamps.of(timestampFn));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void withTimestampsLambdaShouldApplyTimestamps() {

    final String yearTwoThousand = "946684800000";
    PCollection<String> timestamped =
        p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
            .apply(WithTimestamps.of((String input) -> new Instant(Long.valueOf(input))));

    PCollection<KV<String, Instant>> timestampedVals =
        timestamped.apply(
            ParDo.of(
                new DoFn<String, KV<String, Instant>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(KV.of(c.element(), c.timestamp()));
                  }
                }));

    PAssert.that(timestamped)
        .containsInAnyOrder(yearTwoThousand, "0", "1234", Integer.toString(Integer.MAX_VALUE));
    PAssert.that(timestampedVals)
        .containsInAnyOrder(
            KV.of("0", new Instant(0)),
            KV.of("1234", new Instant(Long.valueOf("1234"))),
            KV.of(Integer.toString(Integer.MAX_VALUE), new Instant(Integer.MAX_VALUE)),
            KV.of(yearTwoThousand, new Instant(Long.valueOf(yearTwoThousand))));

    p.run();
  }

  @Test
  public void withWatermarkDelay_delaysWatermark() {
    Duration delay = Duration.standardMinutes(5);
    Instant now = Instant.EPOCH.plus(Duration.standardDays(1000));
    long ts = now.plus(delay).getMillis();
    TestStream<Long> stream =
        TestStream.create(VarLongCoder.of())
            .addElements(TimestampedValue.of(ts, now))
            .advanceWatermarkTo(now)
            .advanceWatermarkToInfinity();
    PCollection<Long> timestamped =
        p.apply(stream).apply(WithTimestamps.of(Instant::ofEpochMilli).withWatermarkDelay(delay));
    PCollection<KV<Long, Instant>> timestampedVals =
        timestamped.apply(
            ParDo.of(
                new DoFn<Long, KV<Long, Instant>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(KV.of(c.element(), c.timestamp()));
                  }
                }));
    PCollection<KV<Long, Instant>> windowed =
        timestampedVals.apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))));
    PAssert.that(windowed)
        .inOnTimePane(
            new IntervalWindow(
                now.plus(delay).minus(Duration.millis(1)),
                now.plus(delay).plus(Duration.millis(1))))
        .containsInAnyOrder(KV.of(ts, Instant.ofEpochMilli(ts)));
  }

  @Test
  public void withWatermarkDelay_lateDataIfWatermarkAdvanced() {
    Duration delay = Duration.standardMinutes(5);
    Instant now = Instant.EPOCH.plus(Duration.standardDays(1000));
    long ts = now.plus(delay).getMillis();
    long lateTs = now.minus(Duration.millis(1)).getMillis();
    TestStream<Long> stream =
        TestStream.create(VarLongCoder.of())
            .addElements(TimestampedValue.of(ts, now))
            .advanceWatermarkTo(now)
            .addElements(TimestampedValue.of(lateTs, now))
            .advanceWatermarkToInfinity();
    PCollection<Long> timestamped =
        p.apply(stream).apply(WithTimestamps.of(Instant::ofEpochMilli).withWatermarkDelay(delay));
    PCollection<KV<Long, Instant>> timestampedVals =
        timestamped.apply(
            ParDo.of(
                new DoFn<Long, KV<Long, Instant>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(KV.of(c.element(), c.timestamp()));
                  }
                }));
    PCollection<KV<Long, Instant>> windowed =
        timestampedVals.apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))));
    PAssert.that(windowed)
        .inOnTimePane(
            new IntervalWindow(
                now.plus(delay).minus(Duration.millis(1)),
                now.plus(delay).plus(Duration.millis(1))))
        .containsInAnyOrder(KV.of(ts, Instant.ofEpochMilli(ts)))
        .inLatePane(new IntervalWindow(now.minus(Duration.millis(2)), now))
        .containsInAnyOrder(KV.of(lateTs, Instant.ofEpochMilli(lateTs)));
  }

  @Test
  public void withWatermarkDelay_watermarkAdvanceConstrainedBySourceTimestamp() {
    Duration delay = Duration.standardMinutes(5);
    Instant now = Instant.EPOCH.plus(Duration.standardDays(1000));
    long ts = now.plus(delay).plus(delay).getMillis();
    TestStream<Long> stream =
        TestStream.create(VarLongCoder.of())
            // Can't advance watermark past source time (now)
            .addElements(TimestampedValue.of(ts, now))
            .advanceWatermarkTo(now)
            .addElements(TimestampedValue.of(now.getMillis(), now))
            .advanceWatermarkToInfinity();
    PCollection<Long> timestamped =
        p.apply(stream).apply(WithTimestamps.of(Instant::ofEpochMilli).withWatermarkDelay(delay));
    PCollection<KV<Long, Instant>> timestampedVals =
        timestamped.apply(
            ParDo.of(
                new DoFn<Long, KV<Long, Instant>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(KV.of(c.element(), c.timestamp()));
                  }
                }));
    PCollection<KV<Long, Instant>> windowed =
        timestampedVals.apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))));
    PAssert.that(windowed)
        .inOnTimePane(
            new IntervalWindow(
                now.plus(delay).minus(Duration.millis(1)),
                now.plus(delay).plus(Duration.millis(1))))
        .containsInAnyOrder(KV.of(ts, Instant.ofEpochMilli(ts)))
        .inOnTimePane(
            new IntervalWindow(now.plus(Duration.millis(1)), now.minus(Duration.millis(1))))
        .containsInAnyOrder(KV.of(now.getMillis(), now));
  }
}
