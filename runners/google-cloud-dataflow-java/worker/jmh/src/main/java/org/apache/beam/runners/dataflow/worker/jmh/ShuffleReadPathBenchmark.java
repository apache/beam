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
package org.apache.beam.runners.dataflow.worker.jmh;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.GroupingShuffleReader;
import org.apache.beam.runners.dataflow.worker.WorkerJmhAccess;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowFn;
import org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowsDoFns;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.GroupingShuffleEntryIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.KeyGroupedShuffleEntries;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.NativeReaderIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntryReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShufflePosition;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for Dataflow Runner v1 shuffle read and GroupAlsoByWindow iteration hot paths:
 * {@link GroupingShuffleEntryIterator}, {@link GroupingShuffleReader}, and {@link
 * BatchGroupAlsoByWindowsDoFns}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ShuffleReadPathBenchmark {

  private static final int NUM_KEYS = 8;
  private static final int VALUES_PER_KEY = 64;

  private static final class ArrayReiterator<T> implements Reiterator<T> {
    private final T[] entries;
    private int index;

    ArrayReiterator(T[] entries, int index) {
      this.entries = entries;
      this.index = index;
    }

    @Override
    public boolean hasNext() {
      return index < entries.length;
    }

    @Override
    public T next() {
      if (index >= entries.length) {
        throw new NoSuchElementException();
      }
      return entries[index++];
    }

    @Override
    public Reiterator<T> copy() {
      return new ArrayReiterator<>(entries, index);
    }
  }

  private static final class ArrayShuffleEntryReader implements ShuffleEntryReader {
    private final ShuffleEntry[] entries;

    ArrayShuffleEntryReader(ShuffleEntry[] entries) {
      this.entries = entries;
    }

    @Override
    public Reiterator<ShuffleEntry> read(
        @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
      return new ArrayReiterator<>(entries, 0);
    }

    @Override
    public void close() {}
  }

  private static final class ArrayReiterable<T> implements Reiterable<T> {
    private final T[] entries;

    ArrayReiterable(T[] entries) {
      this.entries = entries;
    }

    @Override
    public Reiterator<T> iterator() {
      return new ArrayReiterator<>(entries, 0);
    }
  }

  @State(Scope.Thread)
  public static class ShuffleState {
    PipelineOptions options;
    BatchModeExecutionContext execCtx;
    DataflowOperationContext opCtx;
    Closeable activation;

    ShuffleEntry[] shuffleEntries;
    GroupingShuffleReader<Long, WindowedValue<Long>> shuffleReader;

    BatchGroupAlsoByWindowFn<Long, Long, Iterable<Long>> gabwGlobal;
    BatchGroupAlsoByWindowFn<Long, Long, Iterable<Long>> gabwFixed;
    KV<Long, Iterable<WindowedValue<Long>>> globalGroup;
    KV<Long, Iterable<WindowedValue<Long>>> fixedGroup;

    long bytesRead;
    long valueSum;

    @Setup
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setUp() throws Exception {
      options = PipelineOptionsFactory.create();
      execCtx = BatchModeExecutionContext.forTesting(options, "S_SHUFFLE");
      activation = execCtx.getExecutionStateTracker().activate();
      opCtx = execCtx.createOperationContext(NameContext.create("S_SHUFFLE", "s1", "s1", "Read"));

      Coder<WindowedValue<Long>> valueWvCoder =
          WindowedValues.getFullCoder(BigEndianLongCoder.of(), GlobalWindow.Coder.INSTANCE);
      Coder<WindowedValue<KV<Long, Iterable<WindowedValue<Long>>>>> readerCoder =
          WindowedValues.getValueOnlyCoder(
              KvCoder.of(BigEndianLongCoder.of(), IterableCoder.of(valueWvCoder)));

      shuffleReader =
          new GroupingShuffleReader<>(
              options, new byte[0], null, null, readerCoder, execCtx, opCtx, null, false);

      List<ShuffleEntry> list = new ArrayList<>(NUM_KEYS * VALUES_PER_KEY);
      for (int k = 0; k < NUM_KEYS; k++) {
        ByteString keyBytes =
            ByteString.copyFrom(CoderUtils.encodeToByteArray(BigEndianLongCoder.of(), (long) k));
        for (int v = 0; v < VALUES_PER_KEY; v++) {
          WindowedValue<Long> wv = WindowedValues.valueInGlobalWindow((long) (k * 1000 + v));
          ByteString valBytes = ByteString.copyFrom(CoderUtils.encodeToByteArray(valueWvCoder, wv));
          byte[] posBytes = new byte[] {(byte) k, (byte) v};
          list.add(
              new ShuffleEntry(
                  ByteArrayShufflePosition.of(posBytes), keyBytes, ByteString.EMPTY, valBytes));
        }
      }
      shuffleEntries = list.toArray(new ShuffleEntry[0]);

      gabwGlobal =
          BatchGroupAlsoByWindowsDoFns.createForIterable(
              WindowingStrategy.globalDefault(), null, BigEndianLongCoder.of());
      gabwFixed =
          BatchGroupAlsoByWindowsDoFns.createForIterable(
              WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1))),
              null,
              BigEndianLongCoder.of());

      WindowedValue<Long>[] globalValues = new WindowedValue[VALUES_PER_KEY];
      WindowedValue<Long>[] fixedValues = new WindowedValue[VALUES_PER_KEY];
      Instant baseTs = new Instant(1_700_000_000_000L);
      IntervalWindow w1 = new IntervalWindow(baseTs, baseTs.plus(Duration.standardMinutes(1)));
      for (int v = 0; v < VALUES_PER_KEY; v++) {
        globalValues[v] = WindowedValues.valueInGlobalWindow((long) v);
        fixedValues[v] =
            WindowedValues.of(
                (long) v, baseTs.plus(Duration.millis(v * 100L)), w1, PaneInfo.NO_FIRING);
      }
      globalGroup = KV.of(1L, new ArrayReiterable<>(globalValues));
      fixedGroup = KV.of(1L, new ArrayReiterable<>(fixedValues));
    }

    @TearDown
    public void tearDown() throws Exception {
      if (activation != null) {
        activation.close();
      }
    }
  }

  @Benchmark
  public void groupingShuffleEntryIterator_iterateAll(ShuffleState state, Blackhole bh) {
    GroupingShuffleEntryIterator it =
        new GroupingShuffleEntryIterator(
            new ArrayShuffleEntryReader(state.shuffleEntries), null, null) {
          @Override
          protected void notifyElementRead(long byteSize) {
            state.bytesRead += byteSize;
          }

          @Override
          protected void commitBytesRead(long bytesRead) {
            state.bytesRead += bytesRead;
          }
        };
    long count = 0;
    while (it.advance()) {
      KeyGroupedShuffleEntries group = it.getCurrent();
      Iterator<ShuffleEntry> values = group.values.iterator();
      while (values.hasNext()) {
        count += values.next().length();
      }
    }
    bh.consume(count);
  }

  @Benchmark
  public void groupingShuffleReader_iterateAll(ShuffleState state, Blackhole bh) throws Exception {
    long sum = 0;
    try (NativeReaderIterator<WindowedValue<KV<Long, Reiterable<WindowedValue<Long>>>>> it =
        WorkerJmhAccess.createGroupingShuffleReaderIterator(
            state.shuffleReader, new ArrayShuffleEntryReader(state.shuffleEntries))) {
      for (boolean hasNext = it.start(); hasNext; hasNext = it.advance()) {
        KV<Long, Reiterable<WindowedValue<Long>>> kv = it.getCurrent().getValue();
        for (WindowedValue<Long> v : kv.getValue()) {
          sum += v.getValue();
        }
      }
    }
    bh.consume(sum);
  }

  @Benchmark
  public void batchGroupAlsoByWindow_globalWindow(ShuffleState state, Blackhole bh)
      throws Exception {
    state.valueSum = 0;
    state.gabwGlobal.processElement(
        state.globalGroup,
        state.options,
        state.execCtx.getStepContext(state.opCtx),
        NullSideInputReader.empty(),
        wv -> {
          long s = 0;
          for (Long v : wv.getValue().getValue()) {
            s += v;
          }
          state.valueSum += s;
        });
    bh.consume(state.valueSum);
  }

  @Benchmark
  public void batchGroupAlsoByWindow_fixedWindow(ShuffleState state, Blackhole bh)
      throws Exception {
    state.valueSum = 0;
    state.gabwFixed.processElement(
        state.fixedGroup,
        state.options,
        state.execCtx.getStepContext(state.opCtx),
        NullSideInputReader.empty(),
        wv -> {
          long s = 0;
          for (Long v : wv.getValue().getValue()) {
            s += v;
          }
          state.valueSum += s;
        });
    bh.consume(state.valueSum);
  }
}
