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
package org.apache.beam.runners.flink.streaming;

import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.beam.runners.flink.translation.wrappers.streaming.FlinkGroupAlsoByWindowWrapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GroupAlsoByWindowTest extends StreamingMultipleProgramsTestBase {

  private final Combine.CombineFn combiner = new Sum.SumIntegerFn();

  private final WindowingStrategy slidingWindowWithAfterWatermarkTriggerStrategy =
      WindowingStrategy.of(SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(5)))
          .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
          .withTrigger(AfterWatermark.pastEndOfWindow()).withMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES);

  private final WindowingStrategy sessionWindowingStrategy =
      WindowingStrategy.of(Sessions.withGapDuration(Duration.standardSeconds(2)))
          .withTrigger(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
          .withMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES)
          .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
          .withAllowedLateness(Duration.standardSeconds(100));

  private final WindowingStrategy fixedWindowingStrategy =
      WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(10)))
          .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp());

  private final WindowingStrategy fixedWindowWithCountTriggerStrategy =
      fixedWindowingStrategy.withTrigger(AfterPane.elementCountAtLeast(5));

  private final WindowingStrategy fixedWindowWithAfterWatermarkTriggerStrategy =
      fixedWindowingStrategy.withTrigger(AfterWatermark.pastEndOfWindow());

  private final WindowingStrategy fixedWindowWithCompoundTriggerStrategy =
    fixedWindowingStrategy.withTrigger(
      AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(5))
        .withLateFirings(AfterPane.elementCountAtLeast(5)).buildTrigger());

  /**
   * The default accumulation mode is
   * {@link org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode#DISCARDING_FIRED_PANES}.
   * This strategy changes it to
   * {@link org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode#ACCUMULATING_FIRED_PANES}
   */
  private final WindowingStrategy fixedWindowWithCompoundTriggerStrategyAcc =
      fixedWindowWithCompoundTriggerStrategy
          .withMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES);

  @Test
  public void testWithLateness() throws Exception {
    WindowingStrategy strategy = WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(2)))
        .withMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES)
        .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
        .withAllowedLateness(Duration.millis(1000));
    long initialTime = 0L;
    Pipeline pipeline = FlinkTestPipeline.createForStreaming();

    KvCoder<String, Integer> inputCoder = KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of());

    FlinkGroupAlsoByWindowWrapper gbwOperaror =
        FlinkGroupAlsoByWindowWrapper.createForTesting(
            pipeline.getOptions(),
            pipeline.getCoderRegistry(),
            strategy,
            inputCoder,
            combiner.<String>asKeyedFn());

    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        new OneInputStreamOperatorTestHarness<>(gbwOperaror);
    testHarness.open();

    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1000), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1200), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processWatermark(new Watermark(initialTime + 2000));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1200), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1200), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processWatermark(new Watermark(initialTime + 4000));

    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 4),
            new Instant(initialTime + 1),
            new IntervalWindow(new Instant(0), new Instant(2000)),
            PaneInfo.createPane(true, false, PaneInfo.Timing.ON_TIME, 0, 0))
        , initialTime + 1));
    expectedOutput.add(new Watermark(initialTime + 2000));

    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 5),
            new Instant(initialTime + 1999),
            new IntervalWindow(new Instant(0), new Instant(2000)),
            PaneInfo.createPane(false, false, PaneInfo.Timing.LATE, 1, 1))
        , initialTime + 1999));


    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 6),
            new Instant(initialTime + 1999),
            new IntervalWindow(new Instant(0), new Instant(2000)),
            PaneInfo.createPane(false, false, PaneInfo.Timing.LATE, 2, 2))
        , initialTime + 1999));
    expectedOutput.add(new Watermark(initialTime + 4000));

    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());
    testHarness.close();
  }

  @Test
  public void testSessionWindows() throws Exception {
    WindowingStrategy strategy = sessionWindowingStrategy;

    long initialTime = 0L;
    Pipeline pipeline = FlinkTestPipeline.createForStreaming();

    KvCoder<String, Integer> inputCoder = KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of());

    FlinkGroupAlsoByWindowWrapper gbwOperaror =
        FlinkGroupAlsoByWindowWrapper.createForTesting(
            pipeline.getOptions(),
            pipeline.getCoderRegistry(),
            strategy,
            inputCoder,
            combiner.<String>asKeyedFn());

    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        new OneInputStreamOperatorTestHarness<>(gbwOperaror);
    testHarness.open();

    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1000), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 3500), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 3700), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 2700), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processWatermark(new Watermark(initialTime + 6000));

    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 6700), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 6800), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 8900), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 7600), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 5600), null, PaneInfo.NO_FIRING), initialTime + 20));

    testHarness.processWatermark(new Watermark(initialTime + 12000));

    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 6),
            new Instant(initialTime + 1),
            new IntervalWindow(new Instant(1), new Instant(5700)),
            PaneInfo.createPane(true, false, PaneInfo.Timing.ON_TIME, 0, 0))
        , initialTime + 1));
    expectedOutput.add(new Watermark(initialTime + 6000));

    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 11),
            new Instant(initialTime + 6700),
            new IntervalWindow(new Instant(1), new Instant(10900)),
            PaneInfo.createPane(true, false, PaneInfo.Timing.ON_TIME, 0, 0))
        , initialTime + 6700));
    expectedOutput.add(new Watermark(initialTime + 12000));

    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());
    testHarness.close();
  }

  @Test
  public void testSlidingWindows() throws Exception {
    WindowingStrategy strategy = slidingWindowWithAfterWatermarkTriggerStrategy;
    long initialTime = 0L;
    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        createTestingOperatorAndState(strategy, initialTime);
    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
    testHarness.processWatermark(new Watermark(initialTime + 25000));

    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 6),
            new Instant(initialTime + 5000),
            new IntervalWindow(new Instant(0), new Instant(10000)),
            PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))
        , initialTime + 5000));
    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 6),
            new Instant(initialTime + 1),
            new IntervalWindow(new Instant(-5000), new Instant(5000)),
            PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))
        , initialTime + 1));
    expectedOutput.add(new Watermark(initialTime + 10000));

    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 11),
            new Instant(initialTime + 15000),
            new IntervalWindow(new Instant(10000), new Instant(20000)),
            PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))
        , initialTime + 15000));
    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 3),
            new Instant(initialTime + 10000),
            new IntervalWindow(new Instant(5000), new Instant(15000)),
            PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))
        , initialTime + 10000));
    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key2", 1),
            new Instant(initialTime + 19500),
            new IntervalWindow(new Instant(10000), new Instant(20000)),
            PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))
        , initialTime + 19500));
    expectedOutput.add(new Watermark(initialTime + 20000));

    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key2", 1),
            new Instant(initialTime + 20000),
            /**
             * this is 20000 and not 19500 because of a convention in dataflow where
             * timestamps of windowed values in a window cannot be smaller than the
             * end of a previous window. Checkout the documentation of the
             * {@link WindowFn#getOutputTime(Instant, BoundedWindow)}
             */
            new IntervalWindow(new Instant(15000), new Instant(25000)),
            PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))
        , initialTime + 20000));
    expectedOutput.add(new StreamRecord<>(
        WindowedValue.of(KV.of("key1", 8),
            new Instant(initialTime + 20000),
            new IntervalWindow(new Instant(15000), new Instant(25000)),
            PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))
        , initialTime + 20000));
    expectedOutput.add(new Watermark(initialTime + 25000));

    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());
    testHarness.close();
  }

  @Test
  public void testAfterWatermarkProgram() throws Exception {
    WindowingStrategy strategy = fixedWindowWithAfterWatermarkTriggerStrategy;
    long initialTime = 0L;
    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        createTestingOperatorAndState(strategy, initialTime);
    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 6),
        new Instant(initialTime + 1), null, PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME)), initialTime + 1));
    expectedOutput.add(new Watermark(initialTime + 10000));

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 11),
        new Instant(initialTime + 10000), null, PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME)), initialTime + 10000));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key2", 1),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME)), initialTime + 19500));
    expectedOutput.add(new Watermark(initialTime + 20000));

    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());
    testHarness.close();
  }

  @Test
  public void testAfterCountProgram() throws Exception {
    WindowingStrategy strategy = fixedWindowWithCountTriggerStrategy;

    long initialTime = 0L;
    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        createTestingOperatorAndState(strategy, initialTime);
    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 5),
        new Instant(initialTime + 1), null, PaneInfo.createPane(true, true, PaneInfo.Timing.EARLY)), initialTime + 1));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 5),
        new Instant(initialTime + 10000), null, PaneInfo.createPane(true, true, PaneInfo.Timing.EARLY)), initialTime + 10000));
    expectedOutput.add(new Watermark(initialTime + 10000));

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key2", 1),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME, 0, 0)), initialTime + 19500));
    expectedOutput.add(new Watermark(initialTime + 20000));
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

    testHarness.close();
  }

  @Test
  public void testCompoundProgram() throws Exception {
    WindowingStrategy strategy = fixedWindowWithCompoundTriggerStrategy;

    long initialTime = 0L;
    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        createTestingOperatorAndState(strategy, initialTime);
    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

    /**
     * PaneInfo are:
     *     isFirst (pane in window),
     *     isLast, Timing (of triggering),
     *     index (of pane in the window),
     *     onTimeIndex (if it the 1st,2nd, ... pane that was fired on time)
     * */

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 5),
        new Instant(initialTime + 1), null, PaneInfo.createPane(true, false, PaneInfo.Timing.EARLY)), initialTime + 1));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 5),
        new Instant(initialTime + 10000), null, PaneInfo.createPane(true, false, PaneInfo.Timing.EARLY)), initialTime + 10000));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 5),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(false, false, PaneInfo.Timing.EARLY, 1, -1)), initialTime + 19500));

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1),
        new Instant(initialTime + 1200), null, PaneInfo.createPane(false, true, PaneInfo.Timing.ON_TIME, 1, 0)), initialTime + 1200));

    expectedOutput.add(new Watermark(initialTime + 10000));

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(false, true, PaneInfo.Timing.ON_TIME, 2, 0)), initialTime + 19500));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key2", 1),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME)), initialTime + 19500));

    expectedOutput.add(new Watermark(initialTime + 20000));
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

    testHarness.close();
  }

  @Test
  public void testCompoundAccumulatingPanesProgram() throws Exception {
    WindowingStrategy strategy = fixedWindowWithCompoundTriggerStrategyAcc;
    long initialTime = 0L;
    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        createTestingOperatorAndState(strategy, initialTime);
    ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 5),
        new Instant(initialTime + 1), null, PaneInfo.createPane(true, false, PaneInfo.Timing.EARLY)), initialTime + 1));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 5),
        new Instant(initialTime + 10000), null, PaneInfo.createPane(true, false, PaneInfo.Timing.EARLY)), initialTime + 10000));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 10),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(false, false, PaneInfo.Timing.EARLY, 1, -1)), initialTime + 19500));

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 6),
        new Instant(initialTime + 1200), null, PaneInfo.createPane(false, true, PaneInfo.Timing.ON_TIME, 1, 0)), initialTime + 1200));

    expectedOutput.add(new Watermark(initialTime + 10000));

    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 11),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(false, true, PaneInfo.Timing.ON_TIME, 2, 0)), initialTime + 19500));
    expectedOutput.add(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key2", 1),
        new Instant(initialTime + 19500), null, PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME)), initialTime + 19500));

    expectedOutput.add(new Watermark(initialTime + 20000));
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

    testHarness.close();
  }

  private OneInputStreamOperatorTestHarness createTestingOperatorAndState(WindowingStrategy strategy, long initialTime) throws Exception {
    Pipeline pipeline = FlinkTestPipeline.createForStreaming();

    KvCoder<String, Integer> inputCoder = KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of());

    FlinkGroupAlsoByWindowWrapper gbwOperaror =
        FlinkGroupAlsoByWindowWrapper.createForTesting(
            pipeline.getOptions(),
            pipeline.getCoderRegistry(),
            strategy,
            inputCoder,
            combiner.<String>asKeyedFn());

    OneInputStreamOperatorTestHarness<WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>> testHarness =
        new OneInputStreamOperatorTestHarness<>(gbwOperaror);
    testHarness.open();

    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1000), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1200), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1200), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 1200), null, PaneInfo.NO_FIRING), initialTime + 20));

    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 10000), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 12100), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 14200), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 15300), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 16500), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 19500), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 19500), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 19500), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 19500), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 19500), null, PaneInfo.NO_FIRING), initialTime + 20));
    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key1", 1), new Instant(initialTime + 19500), null, PaneInfo.NO_FIRING), initialTime + 20));

    testHarness.processElement(new StreamRecord<>(makeWindowedValue(strategy, KV.of("key2", 1), new Instant(initialTime + 19500), null, PaneInfo.NO_FIRING), initialTime + 20));

    testHarness.processWatermark(new Watermark(initialTime + 10000));
    testHarness.processWatermark(new Watermark(initialTime + 20000));

    return testHarness;
  }

  private static class ResultSortComparator implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 instanceof Watermark && o2 instanceof Watermark) {
        Watermark w1 = (Watermark) o1;
        Watermark w2 = (Watermark) o2;
        return (int) (w1.getTimestamp() - w2.getTimestamp());
      } else {
        StreamRecord<WindowedValue<KV<String, Integer>>> sr0 = (StreamRecord<WindowedValue<KV<String, Integer>>>) o1;
        StreamRecord<WindowedValue<KV<String, Integer>>> sr1 = (StreamRecord<WindowedValue<KV<String, Integer>>>) o2;

        int comparison = (int) (sr0.getValue().getTimestamp().getMillis() - sr1.getValue().getTimestamp().getMillis());
        if (comparison != 0) {
          return comparison;
        }

        comparison = sr0.getValue().getValue().getKey().compareTo(sr1.getValue().getValue().getKey());
        if(comparison == 0) {
          comparison = Integer.compare(
              sr0.getValue().getValue().getValue(),
              sr1.getValue().getValue().getValue());
        }
        if(comparison == 0) {
          Collection windowsA = sr0.getValue().getWindows();
          Collection windowsB = sr1.getValue().getWindows();

          if(windowsA.size() != 1 || windowsB.size() != 1) {
            throw new IllegalStateException("A value cannot belong to more than one windows after grouping.");
          }

          BoundedWindow windowA = (BoundedWindow) windowsA.iterator().next();
          BoundedWindow windowB = (BoundedWindow) windowsB.iterator().next();
          comparison = Long.compare(windowA.maxTimestamp().getMillis(), windowB.maxTimestamp().getMillis());
        }
        return comparison;
      }
    }
  }

  private <T> WindowedValue<T> makeWindowedValue(WindowingStrategy strategy,
                           T output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
    final Instant inputTimestamp = timestamp;
    final WindowFn windowFn = strategy.getWindowFn();

    if (timestamp == null) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    if (windows == null) {
      try {
        windows = windowFn.assignWindows(windowFn.new AssignContext() {
          @Override
          public Object element() {
            throw new UnsupportedOperationException(
                "WindowFn attempted to access input element when none was available");
          }

          @Override
          public Instant timestamp() {
            if (inputTimestamp == null) {
              throw new UnsupportedOperationException(
                  "WindowFn attempted to access input timestamp when none was available");
            }
            return inputTimestamp;
          }

          @Override
          public BoundedWindow window() {
            throw new UnsupportedOperationException(
                "WindowFn attempted to access input windows when none were available");
          }
        });
      } catch (Exception e) {
        throw UserCodeException.wrap(e);
      }
    }

    return WindowedValue.of(output, timestamp, windows, pane);
  }
}
