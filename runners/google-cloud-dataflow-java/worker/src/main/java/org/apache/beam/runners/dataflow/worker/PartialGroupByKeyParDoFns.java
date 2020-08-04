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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.getBytes;

import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.GroupingTable;
import org.apache.beam.runners.dataflow.worker.util.common.worker.GroupingTables;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.SimplePartialGroupByKeyParDoFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** A factory class that creates {@link ParDoFn} for {@link PartialGroupByKeyInstruction}. */
public class PartialGroupByKeyParDoFns {
  public static <K, InputT, AccumT> ParDoFn create(
      PipelineOptions options,
      KvCoder<K, ?> inputElementCoder,
      @Nullable CloudObject cloudUserFn,
      @Nullable List<SideInputInfo> sideInputInfos,
      List<Receiver> receivers,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    AppliedCombineFn<K, InputT, AccumT, ?> combineFn;
    SideInputReader sideInputReader;
    StepContext stepContext;
    if (cloudUserFn == null) {
      combineFn = null;
      sideInputReader = NullSideInputReader.empty();
      stepContext = null;
    } else {
      Object deserializedFn =
          SerializableUtils.deserializeFromByteArray(
              getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN), "serialized combine fn");
      @SuppressWarnings("unchecked")
      AppliedCombineFn<K, InputT, AccumT, ?> combineFnUnchecked =
          ((AppliedCombineFn<K, InputT, AccumT, ?>) deserializedFn);
      combineFn = combineFnUnchecked;

      sideInputReader =
          executionContext.getSideInputReader(
              sideInputInfos, combineFn.getSideInputViews(), operationContext);
      stepContext = executionContext.getStepContext(operationContext);
    }
    return create(
        options, inputElementCoder, combineFn, sideInputReader, receivers.get(0), stepContext);
  }

  @VisibleForTesting
  static <K, InputT, AccumT> ParDoFn create(
      PipelineOptions options,
      KvCoder<K, ?> inputElementCoder,
      @Nullable AppliedCombineFn<K, InputT, AccumT, ?> combineFn,
      SideInputReader sideInputReader,
      Receiver receiver,
      @Nullable StepContext stepContext)
      throws Exception {
    long maxSizeBytes =
        options.as(SdkHarnessOptions.class).getGroupingTableMaxSizeMb() * (1024L * 1024L);

    Coder<K> keyCoder = inputElementCoder.getKeyCoder();
    Coder<?> valueCoder = inputElementCoder.getValueCoder();
    if (combineFn == null) {
      @SuppressWarnings("unchecked")
      Coder<InputT> inputCoder = (Coder<InputT>) valueCoder;
      GroupingTable<?, ?, ?> groupingTable =
          GroupingTables.bufferingAndSampling(
              new WindowingCoderGroupingKeyCreator<>(keyCoder),
              PairInfo.create(),
              new CoderSizeEstimator<>(WindowedValue.getValueOnlyCoder(keyCoder)),
              new CoderSizeEstimator<>(inputCoder),
              0.001, /*sizeEstimatorSampleRate*/
              maxSizeBytes /*maxSizeBytes*/);
      return new SimplePartialGroupByKeyParDoFn<>(groupingTable, receiver);
    } else {
      GroupingTables.Combiner<WindowedValue<K>, InputT, AccumT, ?> valueCombiner =
          new ValueCombiner<>(
              GlobalCombineFnRunners.create(combineFn.getFn()), sideInputReader, options);

      GroupingTable<WindowedValue<K>, InputT, AccumT> groupingTable =
          GroupingTables.combiningAndSampling(
              new WindowingCoderGroupingKeyCreator<>(keyCoder),
              PairInfo.create(),
              valueCombiner,
              new CoderSizeEstimator<>(WindowedValue.getValueOnlyCoder(keyCoder)),
              new CoderSizeEstimator<>(combineFn.getAccumulatorCoder()),
              0.001, /*sizeEstimatorSampleRate*/
              maxSizeBytes /*maxSizeBytes*/);
      if (sideInputReader.isEmpty()) {
        return new SimplePartialGroupByKeyParDoFn<>(groupingTable, receiver);
      } else if (options.as(StreamingOptions.class).isStreaming()) {
        StreamingSideInputFetcher<KV<K, InputT>, ?> sideInputFetcher =
            new StreamingSideInputFetcher<>(
                combineFn.getSideInputViews(),
                combineFn.getKvCoder(),
                combineFn.getWindowingStrategy(),
                (StreamingModeExecutionContext.StreamingModeStepContext) stepContext);
        return new StreamingSideInputPGBKParDoFn<>(groupingTable, receiver, sideInputFetcher);
      } else {
        return new BatchSideInputPGBKParDoFn<>(groupingTable, receiver);
      }
    }
  }

  /** Implements PGBKOp.Combiner via Combine.KeyedCombineFn. */
  public static class ValueCombiner<K, InputT, AccumT, OutputT>
      implements GroupingTables.Combiner<WindowedValue<K>, InputT, AccumT, OutputT> {
    private final GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFn;
    private final SideInputReader sideInputReader;
    private final PipelineOptions options;

    private ValueCombiner(
        GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFn,
        SideInputReader sideInputReader,
        PipelineOptions options) {
      this.combineFn = combineFn;
      this.sideInputReader = sideInputReader;
      this.options = options;
    }

    @Override
    public AccumT createAccumulator(WindowedValue<K> windowedKey) {
      return this.combineFn.createAccumulator(options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT add(WindowedValue<K> windowedKey, AccumT accumulator, InputT value) {
      return this.combineFn.addInput(
          accumulator, value, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT merge(WindowedValue<K> windowedKey, Iterable<AccumT> accumulators) {
      return this.combineFn.mergeAccumulators(
          accumulators, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT compact(WindowedValue<K> windowedKey, AccumT accumulator) {
      return this.combineFn.compact(
          accumulator, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public OutputT extract(WindowedValue<K> windowedKey, AccumT accumulator) {
      return this.combineFn.extractOutput(
          accumulator, options, sideInputReader, windowedKey.getWindows());
    }
  }

  /** Implements PGBKOp.PairInfo via KVs. */
  public static class PairInfo implements GroupingTables.PairInfo {
    private static PairInfo theInstance = new PairInfo();

    public static PairInfo create() {
      return theInstance;
    }

    private PairInfo() {}

    @Override
    public Object getKeyFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.withValue(windowedKv.getValue().getKey());
    }

    @Override
    public Object getValueFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.getValue().getValue();
    }

    @Override
    public Object makeOutputPair(Object key, Object values) {
      WindowedValue<?> windowedKey = (WindowedValue<?>) key;
      return windowedKey.withValue(KV.of(windowedKey.getValue(), values));
    }
  }

  /** Implements PGBKOp.GroupingKeyCreator via Coder. */
  // TODO: Actually support window merging in the combiner table.
  public static class WindowingCoderGroupingKeyCreator<K>
      implements GroupingTables.GroupingKeyCreator<WindowedValue<K>> {

    private static final Instant ignored = BoundedWindow.TIMESTAMP_MIN_VALUE;

    private final Coder<K> coder;

    public WindowingCoderGroupingKeyCreator(Coder<K> coder) {
      this.coder = coder;
    }

    @Override
    public Object createGroupingKey(WindowedValue<K> key) throws Exception {
      // Ignore timestamp for grouping purposes.
      // The PGBK output will inherit the timestamp of one of its inputs.
      return WindowedValue.of(
          coder.structuralValue(key.getValue()), ignored, key.getWindows(), key.getPane());
    }
  }

  /** Implements PGBKOp.SizeEstimator via Coder. */
  public static class CoderSizeEstimator<T> implements GroupingTables.SizeEstimator<T> {
    /** Basic implementation of {@link ElementByteSizeObserver} for use in size estimation. */
    private static class Observer extends ElementByteSizeObserver {
      private long observedSize = 0;

      @Override
      protected void reportElementSize(long elementSize) {
        observedSize += elementSize;
      }
    }

    final Coder<T> coder;

    public CoderSizeEstimator(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public long estimateSize(T value) throws Exception {
      // First try using byte size observer
      Observer observer = new Observer();
      coder.registerByteSizeObserver(value, observer);

      if (!observer.getIsLazy()) {
        observer.advance();
        return observer.observedSize;
      } else {
        // Coder byte size observation is lazy (requires iteration for observation) so fall back to
        // counting output stream
        CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream());
        coder.encode(value, os);
        return os.getCount();
      }
    }
  }

  static class BatchSideInputPGBKParDoFn<K, InputT, AccumT, W extends BoundedWindow>
      implements ParDoFn {
    private final GroupingTable<WindowedValue<K>, InputT, AccumT> groupingTable;
    private final Receiver receiver;

    public BatchSideInputPGBKParDoFn(
        GroupingTable<WindowedValue<K>, InputT, AccumT> groupingTable, Receiver receiver) {
      this.groupingTable = groupingTable;
      this.receiver = receiver;
    }

    @Override
    public void startBundle(Receiver... receivers) throws Exception {}

    @Override
    public void processElement(Object elem) throws Exception {
      @SuppressWarnings({"unchecked"})
      WindowedValue<KV<K, InputT>> input = (WindowedValue<KV<K, InputT>>) elem;
      for (BoundedWindow w : input.getWindows()) {
        WindowedValue<KV<K, InputT>> windowsExpandedInput =
            WindowedValue.of(input.getValue(), input.getTimestamp(), w, input.getPane());
        groupingTable.put(windowsExpandedInput, receiver);
      }
    }

    @Override
    public void processTimers() {}

    @Override
    public void finishBundle() throws Exception {
      groupingTable.flush(receiver);
    }

    @Override
    public void abort() throws Exception {}
  }

  static class StreamingSideInputPGBKParDoFn<K, InputT, AccumT, W extends BoundedWindow>
      implements ParDoFn {
    private final GroupingTable<WindowedValue<K>, InputT, AccumT> groupingTable;
    private final Receiver receiver;
    private final StreamingSideInputFetcher<KV<K, InputT>, W> sideInputFetcher;

    StreamingSideInputPGBKParDoFn(
        GroupingTable<WindowedValue<K>, InputT, AccumT> groupingTable,
        Receiver receiver,
        StreamingSideInputFetcher<KV<K, InputT>, W> sideInputFetcher) {
      this.groupingTable = groupingTable;
      this.receiver = receiver;
      this.sideInputFetcher = sideInputFetcher;
    }

    @Override
    public void startBundle(Receiver... receivers) throws Exception {
      // Find the set of ready windows.
      Set<W> readyWindows = sideInputFetcher.getReadyWindows();

      Iterable<BagState<WindowedValue<KV<K, InputT>>>> elementsBags =
          sideInputFetcher.prefetchElements(readyWindows);

      // Put elements into the grouping table now that all side inputs are ready.
      for (BagState<WindowedValue<KV<K, InputT>>> elementsBag : elementsBags) {
        Iterable<WindowedValue<KV<K, InputT>>> elements = elementsBag.read();
        for (WindowedValue<KV<K, InputT>> elem : elements) {
          groupingTable.put(elem, receiver);
        }
        elementsBag.clear();
      }
      sideInputFetcher.releaseBlockedWindows(readyWindows);
    }

    @Override
    public void processElement(Object elem) throws Exception {
      @SuppressWarnings({"unchecked"})
      WindowedValue<KV<K, InputT>> input = (WindowedValue<KV<K, InputT>>) elem;
      for (BoundedWindow w : input.getWindows()) {
        WindowedValue<KV<K, InputT>> windowsExpandedInput =
            WindowedValue.of(input.getValue(), input.getTimestamp(), w, input.getPane());

        if (!sideInputFetcher.storeIfBlocked(windowsExpandedInput)) {
          groupingTable.put(windowsExpandedInput, receiver);
        }
      }
    }

    @Override
    public void processTimers() {}

    @Override
    public void finishBundle() throws Exception {
      groupingTable.flush(receiver);
      sideInputFetcher.persist();
    }

    @Override
    public void abort() throws Exception {}
  }
}
