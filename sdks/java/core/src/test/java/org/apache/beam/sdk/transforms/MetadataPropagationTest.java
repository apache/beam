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

import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetadataPropagationTest {

  /** Tests for metadata propagation. */
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  static class CausedByDrainSettingDoFn extends DoFn<Boolean, String> {
    @ProcessElement
    public void process(@Element Boolean isDrain, OutputReceiver<String> r) {
      if (isDrain) {
        r.builder("value").setCausedByDrain(CausedByDrain.CAUSED_BY_DRAIN).output();
      } else {
        r.builder("value").setCausedByDrain(CausedByDrain.NORMAL).output();
      }
    }
  }

  static class CausedByDrainExtractingDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext pc, OutputReceiver<String> r) {
      r.output(pc.causedByDrain().toString());
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMetadataPropagationAcrossShuffleParameter() {
    WindowedValues.WindowedValueCoder.setMetadataSupported();
    PCollection<String> results =
        pipeline
            .apply(Create.of(true))
            .apply(ParDo.of(new CausedByDrainSettingDoFn()))
            .apply(Redistribute.arbitrarily())
            .apply(ParDo.of(new CausedByDrainExtractingDoFn()));

    PAssert.that(results).containsInAnyOrder("CAUSED_BY_DRAIN");

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, NeedsRunner.class})
  public void testMetadataPropagationParameter() {
    PCollection<String> results =
        pipeline
            .apply(Create.of(true))
            .apply(ParDo.of(new CausedByDrainSettingDoFn()))
            .apply(ParDo.of(new CausedByDrainExtractingDoFn()));

    PAssert.that(results).containsInAnyOrder("CAUSED_BY_DRAIN");

    pipeline.run();
  }

  static class CausedByDrainExtracingFromGBKDoFn
      extends DoFn<KV<String, Iterable<String>>, String> {
    @ProcessElement
    public void process(ProcessContext pc, OutputReceiver<String> r) {
      r.output(pc.causedByDrain().toString());
    }
  }

  /**
   * Tests metadata propagation across GroupByKey. Note: This test works only with DirectRunner and
   * runners that support metadata propagation (e.g. via a flag to enable metadata encoding in
   * coders). It fails on portable runners like Prism because they do not have implementation for
   * metadata propagation, leading to coder mismatches.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testMetadataPropagationAcrossGBK() {
    WindowedValues.WindowedValueCoder.setMetadataSupported();
    Instant baseTime = new Instant(0);
    TestStream<Boolean> stream =
        TestStream.create(BooleanCoder.of())
            .advanceWatermarkTo(baseTime)
            .addElements(TimestampedValue.of(false, baseTime.plus(Duration.standardSeconds(10))))
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(1)))
            .addElements(
                TimestampedValue.of(false, baseTime.plus(Duration.standardSeconds(71))),
                TimestampedValue.of(true, baseTime.plus(Duration.standardSeconds(72))))
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(2)))
            .addElements(
                TimestampedValue.of(false, baseTime.plus(Duration.standardSeconds(130))),
                TimestampedValue.of(true, baseTime.plus(Duration.standardSeconds(131))), // drain
                TimestampedValue.of(false, baseTime.plus(Duration.standardSeconds(132))))
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
            .addElements(
                TimestampedValue.of(false, baseTime.plus(Duration.standardSeconds(181)))) // normal
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(4)))
            .advanceWatermarkToInfinity();

    Duration windowDuration = Duration.standardMinutes(1);
    IntervalWindow window1 = new IntervalWindow(baseTime, windowDuration);
    IntervalWindow window2 = new IntervalWindow(window1.end(), windowDuration);
    IntervalWindow window3 = new IntervalWindow(window2.end(), windowDuration);
    IntervalWindow window4 = new IntervalWindow(window3.end(), windowDuration);

    PCollection<String> results =
        pipeline
            .apply(stream)
            .apply(ParDo.of(new CausedByDrainSettingDoFn()))
            .apply(WithKeys.of("1"))
            .apply(Window.into(FixedWindows.of(windowDuration)))
            .apply(GroupByKey.create())
            .apply(ParDo.of(new CausedByDrainExtracingFromGBKDoFn()));

    PAssert.that(results).inWindow(window1).containsInAnyOrder("NORMAL");
    PAssert.that(results).inWindow(window2).containsInAnyOrder("CAUSED_BY_DRAIN");
    PAssert.that(results).inWindow(window3).containsInAnyOrder("CAUSED_BY_DRAIN");
    PAssert.that(results).inWindow(window4).containsInAnyOrder("NORMAL");

    pipeline.run();
  }

  @After
  public void tearDown() {
    WindowedValues.WindowedValueCoder.setMetadataNotSupported();
  }
}
