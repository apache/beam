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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.GroupingShuffleReader;
import org.apache.beam.runners.dataflow.worker.WorkerJmhAccess;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowFn;
import org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowsDoFns;
import org.apache.beam.runners.dataflow.worker.util.common.worker.BatchingShuffleEntryReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.GroupingShuffleEntryIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.KeyGroupedShuffleEntries;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.NativeReaderIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleBatchReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntryReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShufflePosition;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValueReceiver;
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
 * {@link BatchingShuffleEntryReader}, {@link GroupingShuffleEntryIterator}, {@link
 * GroupingShuffleReader}, and {@link BatchGroupAlsoByWindowsDoFns}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ShuffleReadPathBenchmark {

  private static final int NUM_KEYS = 8;
  private static final int VALUES_PER_KEY = 64;
  private static final int TINY_KEYS = 256;
  private static final int FIXED_NUM_WINDOWS = 16;
  private static final int VALUES_PER_WINDOW = 16;
  private static final int SLIDING_VALUES = 128;
  private static final int BATCH_PAGE_SIZE = 64;

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

  private static final class PagedShuffleBatchReader implements ShuffleBatchReader {
    private final List<List<ShuffleEntry>> pages;
    private final ShufflePosition[] pagePositions;

    PagedShuffleBatchReader(ShuffleEntry[] allEntries, int pageSize) {
      int numPages = Math.max(1, (allEntries.length + pageSize - 1) / pageSize);
      this.pages = new ArrayList<>(numPages);
      this.pagePositions = new ShufflePosition[numPages];
      for (int p = 0; p < numPages; p++) {
        int from = p * pageSize;
        int to = Math.min(allEntries.length, from + pageSize);
        this.pages.add(new ArrayList<>(Arrays.asList(allEntries).subList(from, to)));
        this.pagePositions[p] = ByteArrayShufflePosition.of(intBytes(p));
      }
    }

    @Override
    public Batch read(
        @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
      int pageIdx = 0;
      if (startPosition != null) {
        ByteString pos = ((ByteArrayShufflePosition) startPosition).getPosition();
        pageIdx =
            ((pos.byteAt(0) & 0xFF) << 24)
                | ((pos.byteAt(1) & 0xFF) << 16)
                | ((pos.byteAt(2) & 0xFF) << 8)
                | (pos.byteAt(3) & 0xFF);
      }
      ShufflePosition next = pageIdx + 1 < pages.size() ? pagePositions[pageIdx + 1] : null;
      return new Batch(pages.get(pageIdx), next);
    }
  }

  private static byte[] intBytes(int v) {
    return new byte[] {(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
  }

  private static byte[] encodePos(int k, int v) {
    return new byte[] {
      (byte) (k >>> 24),
      (byte) (k >>> 16),
      (byte) (k >>> 8),
      (byte) k,
      (byte) (v >>> 24),
      (byte) (v >>> 16),
      (byte) (v >>> 8),
      (byte) v
    };
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
    StepContext stepCtx;
    Closeable activation;

    ShuffleEntry[] shuffleEntries;
    ShuffleEntry[] tinyKeyEntries;
    BatchingShuffleEntryReader batchingEntryReader;
    GroupingShuffleReader<Long, WindowedValue<Long>> shuffleReader;

    BatchGroupAlsoByWindowFn<Long, Long, Iterable<Long>> gabwGlobal;
    BatchGroupAlsoByWindowFn<Long, Long, Iterable<Long>> gabwFixed;
    BatchGroupAlsoByWindowFn<Long, Long, Iterable<Long>> gabwSliding;
    KV<Long, Iterable<WindowedValue<Long>>> globalGroup;
    KV<Long, Iterable<WindowedValue<Long>>> fixedSingleWindowGroup;
    KV<Long, Iterable<WindowedValue<Long>>> fixedMultiWindowGroup;
    KV<Long, Iterable<WindowedValue<Long>>> slidingGroup;

    long valueSum;
    WindowedValueReceiver<KV<Long, Iterable<Long>>> sumSink;

    @Setup
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setUp() throws Exception {
      options = PipelineOptionsFactory.create();
      execCtx = BatchModeExecutionContext.forTesting(options, "S_SHUFFLE");
      activation = execCtx.getExecutionStateTracker().activate();
      opCtx = execCtx.createOperationContext(NameContext.create("S_SHUFFLE", "s1", "s1", "Read"));
      stepCtx = execCtx.getStepContext(opCtx);
      sumSink =
          wv -> {
            long s = 0;
            for (Long v : wv.getValue().getValue()) {
              s += v;
            }
            valueSum += s;
          };

      Coder<WindowedValue<Long>> valueWvCoder =
          WindowedValues.getFullCoder(BigEndianLongCoder.of(), GlobalWindow.Coder.INSTANCE);
      Coder<WindowedValue<KV<Long, Iterable<WindowedValue<Long>>>>> readerCoder =
          WindowedValues.getValueOnlyCoder(
              KvCoder.of(BigEndianLongCoder.of(), IterableCoder.of(valueWvCoder)));

      shuffleReader =
          new GroupingShuffleReader<>(
              options, new byte[0], null, null, readerCoder, execCtx, opCtx, null, false);

      // Allocate a distinct ByteString per ShuffleEntry (mirroring production ByteArrayReader) so
      // key.equals() cannot short-circuit on reference equality.
      List<ShuffleEntry> list = new ArrayList<>(NUM_KEYS * VALUES_PER_KEY);
      for (int k = 0; k < NUM_KEYS; k++) {
        byte[] rawKey = CoderUtils.encodeToByteArray(BigEndianLongCoder.of(), (long) k);
        for (int v = 0; v < VALUES_PER_KEY; v++) {
          WindowedValue<Long> wv = WindowedValues.valueInGlobalWindow((long) (k * 1000 + v));
          ByteString valBytes = ByteString.copyFrom(CoderUtils.encodeToByteArray(valueWvCoder, wv));
          list.add(
              new ShuffleEntry(
                  ByteArrayShufflePosition.of(encodePos(k, v)),
                  ByteString.copyFrom(rawKey),
                  ByteString.EMPTY,
                  valBytes));
        }
      }
      shuffleEntries = list.toArray(new ShuffleEntry[0]);
      batchingEntryReader =
          new BatchingShuffleEntryReader(
              new PagedShuffleBatchReader(shuffleEntries, BATCH_PAGE_SIZE));

      List<ShuffleEntry> tiny = new ArrayList<>(TINY_KEYS);
      for (int k = 0; k < TINY_KEYS; k++) {
        byte[] rawKey = CoderUtils.encodeToByteArray(BigEndianLongCoder.of(), (long) k);
        WindowedValue<Long> wv = WindowedValues.valueInGlobalWindow((long) k);
        ByteString valBytes = ByteString.copyFrom(CoderUtils.encodeToByteArray(valueWvCoder, wv));
        tiny.add(
            new ShuffleEntry(
                ByteArrayShufflePosition.of(encodePos(k, 0)),
                ByteString.copyFrom(rawKey),
                ByteString.EMPTY,
                valBytes));
      }
      tinyKeyEntries = tiny.toArray(new ShuffleEntry[0]);

      gabwGlobal =
          BatchGroupAlsoByWindowsDoFns.createForIterable(
              WindowingStrategy.globalDefault(), null, BigEndianLongCoder.of());
      gabwFixed =
          BatchGroupAlsoByWindowsDoFns.createForIterable(
              WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1))),
              null,
              BigEndianLongCoder.of());
      gabwSliding =
          BatchGroupAlsoByWindowsDoFns.createForIterable(
              WindowingStrategy.of(
                  SlidingWindows.of(Duration.standardMinutes(1))
                      .every(Duration.standardSeconds(15))),
              null,
              BigEndianLongCoder.of());

      Instant baseTs = new Instant(1_700_000_000_000L);
      WindowedValue<Long>[] globalValues = new WindowedValue[VALUES_PER_KEY];
      WindowedValue<Long>[] fixedSingleValues = new WindowedValue[VALUES_PER_KEY];
      Instant winEnd = baseTs.plus(Duration.standardMinutes(1));
      for (int v = 0; v < VALUES_PER_KEY; v++) {
        globalValues[v] =
            WindowedValues.timestampedValueInGlobalWindow(
                (long) v, baseTs.plus(Duration.millis(v * 100L)));
        // Distinct IntervalWindow instance per element (mirrors IntervalWindowCoder.decode()).
        fixedSingleValues[v] =
            WindowedValues.of(
                (long) v,
                baseTs.plus(Duration.millis(v * 100L)),
                new IntervalWindow(baseTs, winEnd),
                PaneInfo.NO_FIRING);
      }
      globalGroup = KV.of(1L, new ArrayReiterable<>(globalValues));
      fixedSingleWindowGroup = KV.of(1L, new ArrayReiterable<>(fixedSingleValues));

      WindowedValue<Long>[] fixedMultiValues =
          new WindowedValue[FIXED_NUM_WINDOWS * VALUES_PER_WINDOW];
      for (int w = 0; w < FIXED_NUM_WINDOWS; w++) {
        Instant start = baseTs.plus(Duration.standardMinutes(w));
        Instant end = start.plus(Duration.standardMinutes(1));
        for (int v = 0; v < VALUES_PER_WINDOW; v++) {
          fixedMultiValues[w * VALUES_PER_WINDOW + v] =
              WindowedValues.of(
                  (long) (w * VALUES_PER_WINDOW + v),
                  start.plus(Duration.millis(v * 1000L)),
                  new IntervalWindow(start, end),
                  PaneInfo.NO_FIRING);
        }
      }
      fixedMultiWindowGroup = KV.of(1L, new ArrayReiterable<>(fixedMultiValues));

      // 4 overlapping sliding windows (size=60s, period=15s) per element.
      WindowedValue<Long>[] slidingValues = new WindowedValue[SLIDING_VALUES];
      long periodMs = 15_000L;
      long sizeMs = 60_000L;
      for (int v = 0; v < SLIDING_VALUES; v++) {
        long tsMs = baseTs.getMillis() + v * 5_000L;
        long lastStart = (tsMs / periodMs) * periodMs;
        List<BoundedWindow> ws = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
          long s = lastStart - i * periodMs;
          if (tsMs < s + sizeMs) {
            ws.add(new IntervalWindow(new Instant(s), new Instant(s + sizeMs)));
          }
        }
        slidingValues[v] = WindowedValues.of((long) v, new Instant(tsMs), ws, PaneInfo.NO_FIRING);
      }
      slidingGroup = KV.of(1L, new ArrayReiterable<>(slidingValues));
    }

    @TearDown
    public void tearDown() throws Exception {
      if (activation != null) {
        activation.close();
      }
    }
  }

  @Benchmark
  public void batchingShuffleEntryReader_iterateAll(ShuffleState state, Blackhole bh) {
    Reiterator<ShuffleEntry> it = state.batchingEntryReader.read(null, null);
    long count = 0;
    while (it.hasNext()) {
      count += it.next().length();
    }
    bh.consume(count);
  }

  @Benchmark
  public void groupingShuffleEntryIterator_iterateAll(ShuffleState state, Blackhole bh) {
    long[] bytes = new long[2];
    GroupingShuffleEntryIterator it =
        new GroupingShuffleEntryIterator(
            new ArrayShuffleEntryReader(state.shuffleEntries), null, null) {
          @Override
          protected void notifyElementRead(long byteSize) {
            bytes[0] += byteSize;
          }

          @Override
          protected void commitBytesRead(long bytesRead) {
            bytes[1] += bytesRead;
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
    bh.consume(count ^ bytes[0] ^ bytes[1]);
  }

  @Benchmark
  public void groupingShuffleEntryIterator_tinyKeys(ShuffleState state, Blackhole bh) {
    long[] bytes = new long[2];
    GroupingShuffleEntryIterator it =
        new GroupingShuffleEntryIterator(
            new ArrayShuffleEntryReader(state.tinyKeyEntries), null, null) {
          @Override
          protected void notifyElementRead(long byteSize) {
            bytes[0] += byteSize;
          }

          @Override
          protected void commitBytesRead(long bytesRead) {
            bytes[1] += bytesRead;
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
    bh.consume(count ^ bytes[0] ^ bytes[1]);
  }

  @Benchmark
  public void groupingShuffleReader_iterateAll(ShuffleState state, Blackhole bh) throws Exception {
    long sum = 0;
    try (NativeReaderIterator<WindowedValue<KV<Long, Reiterable<WindowedValue<Long>>>>> it =
        WorkerJmhAccess.createGroupingShuffleReaderIterator(
            state.shuffleReader, state.batchingEntryReader)) {
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
        state.stepCtx,
        NullSideInputReader.empty(),
        state.sumSink);
    bh.consume(state.valueSum);
  }

  @Benchmark
  public void batchGroupAlsoByWindow_fixedWindow(ShuffleState state, Blackhole bh)
      throws Exception {
    state.valueSum = 0;
    state.gabwFixed.processElement(
        state.fixedSingleWindowGroup,
        state.options,
        state.stepCtx,
        NullSideInputReader.empty(),
        state.sumSink);
    bh.consume(state.valueSum);
  }

  @Benchmark
  public void batchGroupAlsoByWindow_fixedWindow_multiWindow(ShuffleState state, Blackhole bh)
      throws Exception {
    state.valueSum = 0;
    state.gabwFixed.processElement(
        state.fixedMultiWindowGroup,
        state.options,
        state.stepCtx,
        NullSideInputReader.empty(),
        state.sumSink);
    bh.consume(state.valueSum);
  }

  @Benchmark
  public void batchGroupAlsoByWindow_slidingWindow(ShuffleState state, Blackhole bh)
      throws Exception {
    state.valueSum = 0;
    state.gabwSliding.processElement(
        state.slidingGroup,
        state.options,
        state.stepCtx,
        NullSideInputReader.empty(),
        state.sumSink);
    bh.consume(state.valueSum);
  }
}
