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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Final step of a batch GroupByKey or Combine in the global window with the default trigger.
 *
 * <p>In batch execution with sorted inputs, Flink feeds a keyed operator all the records of a key
 * before moving to the next key. With the global window and the default trigger, each key produces
 * exactly one on-time pane once all its records are seen. This operator therefore combines the
 * records of the current key in plain fields, and emits the result when the key changes or the
 * input ends. Unlike {@link WindowDoFnOperator}, it does not use keyed state, timers or watermark
 * holds.
 *
 * <p>It must only be used on a keyed stream in batch execution, where Flink sorts the input by key.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SortedBatchCombineOperator<K, AccumT, OutputT>
    extends AbstractStreamOperator<WindowedValue<KV<K, OutputT>>>
    implements OneInputStreamOperator<WindowedValue<KV<K, AccumT>>, WindowedValue<KV<K, OutputT>>>,
        BoundedOneInput {

  private static final long serialVersionUID = 1L;

  /** Combines the accumulators of one key. */
  public interface KeyCombiner<AccumT, OutputT> extends Serializable {
    void add(AccumT accumulator, PipelineOptions options);

    /** Returns the output for the accumulators added since the last call, and resets. */
    OutputT finish(PipelineOptions options);
  }

  /** Merges accumulators with a {@link GlobalCombineFn} and extracts its output. */
  public static class MergingCombiner<AccumT, OutputT> implements KeyCombiner<AccumT, OutputT> {
    private static final long serialVersionUID = 1L;

    /** Number of accumulators merged at once, bounding the memory used per key. */
    static final int MERGE_BATCH_SIZE = 32;

    private final GlobalCombineFn<?, AccumT, OutputT> combineFn;
    private transient @Nullable GlobalCombineFnRunner<?, AccumT, OutputT> runner;
    private transient @Nullable List<AccumT> pending;

    public MergingCombiner(GlobalCombineFn<?, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    private GlobalCombineFnRunner<?, AccumT, OutputT> runner() {
      if (runner == null) {
        runner = GlobalCombineFnRunners.create(combineFn);
      }
      return runner;
    }

    private List<AccumT> pending() {
      if (pending == null) {
        pending = new ArrayList<>(MERGE_BATCH_SIZE);
      }
      return pending;
    }

    @Override
    public void add(AccumT accumulator, PipelineOptions options) {
      List<AccumT> accumulators = pending();
      accumulators.add(accumulator);
      if (accumulators.size() >= MERGE_BATCH_SIZE) {
        AccumT merged = merge(accumulators, options);
        accumulators.clear();
        accumulators.add(merged);
      }
    }

    @Override
    public OutputT finish(PipelineOptions options) {
      List<AccumT> accumulators = pending();
      AccumT merged = merge(accumulators, options);
      accumulators.clear();
      return runner().extractOutput(merged, options, SIDE_INPUTS, WINDOWS);
    }

    private AccumT merge(List<AccumT> accumulators, PipelineOptions options) {
      return accumulators.size() == 1
          ? accumulators.get(0)
          : runner().mergeAccumulators(accumulators, options, SIDE_INPUTS, WINDOWS);
    }
  }

  /** Collects the accumulators of a key, like {@code SystemReduceFn.buffering}. */
  public static class BufferingCombiner<AccumT> implements KeyCombiner<AccumT, Iterable<AccumT>> {
    private static final long serialVersionUID = 1L;

    private transient @Nullable List<AccumT> pending;

    @Override
    public void add(AccumT accumulator, PipelineOptions options) {
      if (pending == null) {
        pending = new ArrayList<>();
      }
      pending.add(accumulator);
    }

    @Override
    public Iterable<AccumT> finish(PipelineOptions options) {
      List<AccumT> result = pending == null ? new ArrayList<>() : pending;
      pending = null;
      return result;
    }
  }

  private static final SideInputReader SIDE_INPUTS = NullSideInputReader.empty();

  private static final List<BoundedWindow> WINDOWS =
      Collections.singletonList(GlobalWindow.INSTANCE);

  private final String stepName;
  private final KeyCombiner<AccumT, OutputT> combiner;
  private final TimestampCombiner timestampCombiner;
  private final SerializablePipelineOptions serializedOptions;

  private transient @Nullable PipelineOptions options;
  transient @Nullable FlinkMetricContainer metricContainer;

  // The key whose records are being combined.
  private transient boolean hasCurrentKey;
  private transient @Nullable Object currentFlinkKey;
  private transient @Nullable K currentKey;
  private transient @Nullable Instant outputTimestamp;

  public SortedBatchCombineOperator(
      String stepName,
      KeyCombiner<AccumT, OutputT> combiner,
      TimestampCombiner timestampCombiner,
      SerializablePipelineOptions serializedOptions) {
    this.stepName = stepName;
    this.combiner = combiner;
    this.timestampCombiner = timestampCombiner;
    this.serializedOptions = serializedOptions;
  }

  @Override
  public void open() throws Exception {
    super.open();
    options = serializedOptions.get();
    if (!options.as(FlinkPipelineOptions.class).getDisableMetrics()) {
      metricContainer = new FlinkMetricContainer(getRuntimeContext());
    }
  }

  @Override
  public void processElement(StreamRecord<WindowedValue<KV<K, AccumT>>> record) throws Exception {
    WindowedValue<KV<K, AccumT>> element = record.getValue();
    // Set by Flink from the key selector of the keyed stream before each record.
    Object flinkKey = getCurrentKey();
    if (hasCurrentKey && !flinkKey.equals(currentFlinkKey)) {
      emitCurrentKey();
    }
    KV<K, AccumT> kv = element.getValue();
    Instant timestamp = timestampCombiner.assign(GlobalWindow.INSTANCE, element.getTimestamp());
    if (!hasCurrentKey) {
      hasCurrentKey = true;
      currentFlinkKey = flinkKey;
      currentKey = kv.getKey();
      outputTimestamp = timestamp;
    } else {
      outputTimestamp = timestampCombiner.combine(outputTimestamp, timestamp);
    }
    try (Closeable ignored = scopedMetrics()) {
      combiner.add(kv.getValue(), options);
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // The output of the current key may be behind any watermark, so watermarks are held back until
    // the end of the input. With sorted inputs, Flink only sends the final watermark, after the
    // last
    // record but before endInput(), so the last key must be emitted before forwarding it.
    if (mark.getTimestamp() == Watermark.MAX_WATERMARK.getTimestamp()) {
      if (hasCurrentKey) {
        emitCurrentKey();
      }
      super.processWatermark(mark);
    }
  }

  @Override
  public void endInput() throws Exception {
    if (hasCurrentKey) {
      emitCurrentKey();
    }
    if (metricContainer != null) {
      metricContainer.updateMetrics(stepName);
    }
  }

  @Override
  public void finish() throws Exception {
    if (metricContainer != null) {
      metricContainer.registerMetricsForPipelineResult();
    }
    super.finish();
  }

  private void emitCurrentKey() throws IOException {
    OutputT result;
    try (Closeable ignored = scopedMetrics()) {
      result = combiner.finish(options);
    }
    output.collect(
        new StreamRecord<>(
            WindowedValues.of(
                KV.of(currentKey, result),
                outputTimestamp,
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    hasCurrentKey = false;
    currentFlinkKey = null;
    currentKey = null;
    outputTimestamp = null;
  }

  private Closeable scopedMetrics() {
    return metricContainer == null
        ? () -> {}
        : MetricsEnvironment.scopedMetricsContainer(metricContainer.getMetricsContainer(stepName));
  }
}
