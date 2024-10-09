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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.beam.runners.flink.translation.wrappers.streaming.StreamRecordStripper.stripStreamRecordFromWindowedValue;
import static org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING;
import static org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing.ON_TIME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.joda.time.Duration.standardMinutes;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator.MultiOutputOutputManagerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindowDoFnOperator}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class WindowDoFnOperatorTest {

  @Test
  public void testRestore() throws Exception {
    // test harness
    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KeyedWorkItem<Long, Long>>, WindowedValue<KV<Long, Long>>>
        testHarness = createTestHarness(getWindowDoFnOperator());
    testHarness.open();

    // process elements
    IntervalWindow window = new IntervalWindow(new Instant(0), Duration.millis(10_000));
    testHarness.processWatermark(0L);
    testHarness.processElement(
        Item.builder().key(1L).timestamp(1L).value(100L).window(window).build().toStreamRecord());
    testHarness.processElement(
        Item.builder().key(1L).timestamp(2L).value(20L).window(window).build().toStreamRecord());
    testHarness.processElement(
        Item.builder().key(2L).timestamp(3L).value(77L).window(window).build().toStreamRecord());

    // create snapshot
    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
    testHarness.close();

    // restore from the snapshot
    testHarness = createTestHarness(getWindowDoFnOperator());
    testHarness.initializeState(snapshot);
    testHarness.open();

    // close window
    testHarness.processWatermark(10_000L);

    Iterable<WindowedValue<KV<Long, Long>>> output =
        stripStreamRecordFromWindowedValue(testHarness.getOutput());

    assertEquals(2, Iterables.size(output));
    assertThat(
        output,
        containsInAnyOrder(
            WindowedValue.of(
                KV.of(1L, 120L),
                new Instant(9_999),
                window,
                PaneInfo.createPane(true, true, ON_TIME)),
            WindowedValue.of(
                KV.of(2L, 77L),
                new Instant(9_999),
                window,
                PaneInfo.createPane(true, true, ON_TIME))));
    // cleanup
    testHarness.close();
  }

  @Test
  public void testTimerCleanupOfPendingTimerList() throws Exception {
    // test harness
    WindowDoFnOperator<Long, Long, Long> windowDoFnOperator = getWindowDoFnOperator();
    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KeyedWorkItem<Long, Long>>, WindowedValue<KV<Long, Long>>>
        testHarness = createTestHarness(windowDoFnOperator);
    testHarness.open();

    DoFnOperator<KeyedWorkItem<Long, Long>, KV<Long, Long>>.FlinkTimerInternals timerInternals =
        windowDoFnOperator.timerInternals;

    // process elements
    IntervalWindow window = new IntervalWindow(new Instant(0), Duration.millis(100));
    IntervalWindow window2 = new IntervalWindow(new Instant(100), Duration.millis(100));
    testHarness.processWatermark(0L);

    // Use two different keys to check for correct watermark hold calculation
    testHarness.processElement(
        Item.builder().key(1L).timestamp(1L).value(100L).window(window).build().toStreamRecord());
    testHarness.processElement(
        Item.builder()
            .key(2L)
            .timestamp(150L)
            .value(150L)
            .window(window2)
            .build()
            .toStreamRecord());

    testHarness.processWatermark(1);

    // Note that the following is 1 because the state is key-partitioned
    assertThat(Iterables.size(timerInternals.pendingTimersById.keys()), is(1));

    assertThat(testHarness.numKeyedStateEntries(), is(6));
    // close bundle
    testHarness.setProcessingTime(
        testHarness.getProcessingTime()
            + 2 * FlinkPipelineOptions.defaults().getMaxBundleTimeMills());
    assertThat(windowDoFnOperator.getCurrentOutputWatermark(), is(1L));

    // close window
    testHarness.processWatermark(100L);

    // Note that the following is zero because we only the first key is active
    assertThat(Iterables.size(timerInternals.pendingTimersById.keys()), is(0));

    assertThat(testHarness.numKeyedStateEntries(), is(3));

    // close bundle
    testHarness.setProcessingTime(
        testHarness.getProcessingTime()
            + 2 * FlinkPipelineOptions.defaults().getMaxBundleTimeMills());
    assertThat(windowDoFnOperator.getCurrentOutputWatermark(), is(100L));

    testHarness.processWatermark(200L);

    // All the state has been cleaned up
    assertThat(testHarness.numKeyedStateEntries(), is(0));

    assertThat(
        stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        containsInAnyOrder(
            WindowedValue.of(
                KV.of(1L, 100L), new Instant(99), window, PaneInfo.createPane(true, true, ON_TIME)),
            WindowedValue.of(
                KV.of(2L, 150L),
                new Instant(199),
                window2,
                PaneInfo.createPane(true, true, ON_TIME))));

    // cleanup
    testHarness.close();
  }

  private WindowDoFnOperator<Long, Long, Long> getWindowDoFnOperator() {
    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(standardMinutes(1)));

    TupleTag<KV<Long, Long>> outputTag = new TupleTag<>("main-output");

    SystemReduceFn<Long, Long, long[], Long, BoundedWindow> reduceFn =
        SystemReduceFn.combining(
            VarLongCoder.of(),
            AppliedCombineFn.withInputCoder(
                Sum.ofLongs(),
                CoderRegistry.createDefault(),
                KvCoder.of(VarLongCoder.of(), VarLongCoder.of())));

    Coder<IntervalWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();
    SingletonKeyedWorkItemCoder<Long, Long> workItemCoder =
        SingletonKeyedWorkItemCoder.of(VarLongCoder.of(), VarLongCoder.of(), windowCoder);
    FullWindowedValueCoder<KeyedWorkItem<Long, Long>> inputCoder =
        WindowedValue.getFullCoder(workItemCoder, windowCoder);
    FullWindowedValueCoder<KV<Long, Long>> outputCoder =
        WindowedValue.getFullCoder(KvCoder.of(VarLongCoder.of(), VarLongCoder.of()), windowCoder);

    return new WindowDoFnOperator<Long, Long, Long>(
        reduceFn,
        "stepName",
        (Coder) inputCoder,
        outputTag,
        emptyList(),
        new MultiOutputOutputManagerFactory<>(
            outputTag,
            outputCoder,
            new SerializablePipelineOptions(FlinkPipelineOptions.defaults())),
        windowingStrategy,
        emptyMap(),
        emptyList(),
        FlinkPipelineOptions.defaults(),
        VarLongCoder.of(),
        new WorkItemKeySelector(
            VarLongCoder.of(), new SerializablePipelineOptions(FlinkPipelineOptions.defaults())));
  }

  private KeyedOneInputStreamOperatorTestHarness<
          ByteBuffer, WindowedValue<KeyedWorkItem<Long, Long>>, WindowedValue<KV<Long, Long>>>
      createTestHarness(WindowDoFnOperator<Long, Long, Long> windowDoFnOperator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        windowDoFnOperator,
        (KeySelector<WindowedValue<KeyedWorkItem<Long, Long>>, ByteBuffer>)
            o -> {
              try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                VarLongCoder.of().encode(o.getValue().key(), baos);
                return ByteBuffer.wrap(baos.toByteArray());
              }
            },
        new GenericTypeInfo<>(ByteBuffer.class));
  }

  private static class Item {

    static ItemBuilder builder() {
      return new ItemBuilder();
    }

    private long key;
    private long value;
    private long timestamp;
    private IntervalWindow window;

    StreamRecord<WindowedValue<KeyedWorkItem<Long, Long>>> toStreamRecord() {
      WindowedValue<Long> item = WindowedValue.of(value, new Instant(timestamp), window, NO_FIRING);
      WindowedValue<KeyedWorkItem<Long, Long>> keyedItem =
          WindowedValue.of(
              new SingletonKeyedWorkItem<>(key, item), new Instant(timestamp), window, NO_FIRING);
      return new StreamRecord<>(keyedItem);
    }

    private static final class ItemBuilder {

      private long key;
      private long value;
      private long timestamp;
      private IntervalWindow window;

      ItemBuilder key(long key) {
        this.key = key;
        return this;
      }

      ItemBuilder value(long value) {
        this.value = value;
        return this;
      }

      ItemBuilder timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      ItemBuilder window(IntervalWindow window) {
        this.window = window;
        return this;
      }

      Item build() {
        Item item = new Item();
        item.key = this.key;
        item.value = this.value;
        item.window = this.window;
        item.timestamp = this.timestamp;
        return item;
      }
    }
  }
}
