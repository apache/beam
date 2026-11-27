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
package org.apache.beam.sdk.jmh.transforms;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.OutputBuilderSuppliers;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for {@link DoFnOutputReceivers}, {@link OutputBuilderSuppliers}, and {@link
 * WindowedValues} (including {@link WindowedValues.FullWindowedValueCoder}) per-element hot paths.
 *
 * <p>Run with {@code -prof gc} to measure per-element allocation rates and with {@code -t 4} (e.g.
 * under {@code MIXED_POLYMORPHIC}) to observe multi-threaded secondary-super contention
 * (JDK-8180450).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class OutputReceiverBenchmark {

  private static final int RING_SIZE = 16;
  private static final int RING_MASK = RING_SIZE - 1;

  private static final class NoopDoFn extends DoFn<String, String> {}

  /** Unsynchronized resettable byte buffer avoiding monitor-lock overhead of JDK BAOS. */
  private static final class UnsyncByteArrayOutputStream extends OutputStream {
    private byte[] buf;
    private int count;

    UnsyncByteArrayOutputStream(int initialCapacity) {
      this.buf = new byte[initialCapacity];
    }

    @Override
    public void write(int b) {
      ensureCapacity(count + 1);
      buf[count++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      ensureCapacity(count + len);
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    private void ensureCapacity(int minCapacity) {
      if (minCapacity > buf.length) {
        buf = Arrays.copyOf(buf, Math.max(minCapacity, buf.length << 1));
      }
    }

    void reset() {
      count = 0;
    }

    int size() {
      return count;
    }

    byte[] toByteArray() {
      return Arrays.copyOf(buf, count);
    }

    byte[] buffer() {
      return buf;
    }
  }

  /** Unsynchronized resettable byte-array stream avoiding monitor-lock overhead of JDK BAIS. */
  private static final class UnsyncByteArrayInputStream extends InputStream {
    private final byte[] buf;
    private int pos;

    UnsyncByteArrayInputStream(byte[] buf) {
      this.buf = buf;
    }

    @Override
    public int read() {
      return pos < buf.length ? (buf[pos++] & 0xFF) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) {
        return 0;
      }
      if (pos >= buf.length) {
        return -1;
      }
      int available = Math.min(len, buf.length - pos);
      System.arraycopy(buf, pos, b, off, available);
      pos += available;
      return available;
    }

    @Override
    public void reset() {
      pos = 0;
    }
  }

  private static final class RecordingByteSizeObserver extends ElementByteSizeObserver {
    long lastSize;

    @Override
    protected void reportElementSize(long elementByteSize) {
      lastSize = elementByteSize;
    }
  }

  /** Thread-local benchmark state for {@link DoFnOutputReceivers} fast-path vs builder checks. */
  @State(Scope.Thread)
  public static class ReceiverState {
    /** Selects the template {@link WindowedValue} shape backing the {@link OutputReceiver}s. */
    @Param({
      "GLOBAL_MIN",
      "GLOBAL_TIMESTAMPED",
      "SINGLE_INTERVAL",
      "MULTI_INTERVAL",
      "MIXED_POLYMORPHIC",
      "BUILDER_TEMPLATE"
    })
    public String templateKind = "SINGLE_INTERVAL";

    final TupleTag<String> mainTag = new TupleTag<>("main");
    final String[] values = new String[RING_SIZE];
    final Row[] rows = new Row[RING_SIZE];
    final Instant[] timestamps = new Instant[RING_SIZE];
    final WindowedValue<?>[] templates = new WindowedValue<?>[RING_SIZE];

    @SuppressWarnings("unchecked")
    final OutputReceiver<String>[] taggedReceivers =
        (OutputReceiver<String>[]) new OutputReceiver<?>[RING_SIZE];

    @SuppressWarnings("unchecked")
    final OutputReceiver<String>[] untaggedReceivers =
        (OutputReceiver<String>[]) new OutputReceiver<?>[RING_SIZE];

    @SuppressWarnings("unchecked")
    final OutputReceiver<Row>[] rowReceivers =
        (OutputReceiver<Row>[]) new OutputReceiver<?>[RING_SIZE];

    Collection<? extends BoundedWindow> explicitWindows;
    DoFn<String, String>.WindowedContext context;
    int index;

    /**
     * Initializes receivers, contexts, and non-constant input rings to prevent constant folding.
     */
    @Setup
    public void setUp(Blackhole bh) {
      PipelineOptions options = PipelineOptionsFactory.create();
      DoFn<String, String> doFn = new NoopDoFn();
      Schema schema = Schema.builder().addStringField("v").build();
      SchemaCoder<String> schemaCoder =
          SchemaCoder.of(
              schema,
              TypeDescriptor.of(String.class),
              s -> Row.withSchema(schema).addValue(s).build(),
              r -> r.getString(0));
      // Keep the DoFn stub free of io.opentelemetry.context.Context bytecode descriptors.
      context =
          doFn.new WindowedContext() {
            @Override
            public PipelineOptions getPipelineOptions() {
              return options;
            }

            @Override
            public void output(String output) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void outputWithTimestamp(String output, Instant timestamp) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void outputWindowedValue(
                String output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo paneInfo) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void outputWindowedValue(WindowedValue<String> windowedValue) {
              bh.consume(windowedValue);
            }

            @Override
            public <T> void output(TupleTag<T> tag, T output) {
              throw new UnsupportedOperationException();
            }

            @Override
            public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
              throw new UnsupportedOperationException();
            }

            @Override
            public <T> void outputWindowedValue(
                TupleTag<T> tag,
                T output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo paneInfo) {
              throw new UnsupportedOperationException();
            }

            @Override
            public <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedValue) {
              bh.consume(windowedValue);
            }
          };

      Instant baseTs = new Instant(1_700_000_000_000L);
      IntervalWindow w1 = new IntervalWindow(baseTs, baseTs.plus(Duration.standardMinutes(1)));
      IntervalWindow w2 =
          new IntervalWindow(
              baseTs.plus(Duration.standardMinutes(1)), baseTs.plus(Duration.standardMinutes(2)));
      explicitWindows = Collections.singletonList(w1);

      for (int i = 0; i < RING_SIZE; i++) {
        values[i] = "output-element-" + i;
        rows[i] = Row.withSchema(schema).addValue(values[i]).build();
        timestamps[i] = baseTs.plus(Duration.millis((i + 1) * 1000L));
        WindowedValue<?> template = createTemplate(templateKind, i, baseTs, w1, w2);
        templates[i] = template;
        taggedReceivers[i] =
            DoFnOutputReceivers.windowedReceiver(
                context, OutputBuilderSuppliers.supplierForElement(template), mainTag);
        untaggedReceivers[i] =
            DoFnOutputReceivers.windowedReceiver(
                context, OutputBuilderSuppliers.supplierForElement(template), null);
        rowReceivers[i] =
            DoFnOutputReceivers.rowReceiver(
                context, OutputBuilderSuppliers.supplierForElement(template), mainTag, schemaCoder);
      }
      index = 0;
    }

    private static WindowedValue<?> createTemplate(
        String kind, int slot, Instant ts, IntervalWindow w1, IntervalWindow w2) {
      String actualKind = kind;
      if ("MIXED_POLYMORPHIC".equals(kind)) {
        switch (slot & 3) {
          case 0:
            actualKind = "GLOBAL_MIN";
            break;
          case 1:
            actualKind = "GLOBAL_TIMESTAMPED";
            break;
          case 2:
            actualKind = "SINGLE_INTERVAL";
            break;
          default:
            actualKind = "MULTI_INTERVAL";
            break;
        }
      }
      switch (actualKind) {
        case "GLOBAL_MIN":
          return WindowedValues.valueInGlobalWindow("in-" + slot);
        case "GLOBAL_TIMESTAMPED":
          return WindowedValues.timestampedValueInGlobalWindow("in-" + slot, ts);
        case "SINGLE_INTERVAL":
          return WindowedValues.of("in-" + slot, ts, w1, PaneInfo.NO_FIRING);
        case "MULTI_INTERVAL":
          return WindowedValues.of("in-" + slot, ts, Arrays.asList(w1, w2), PaneInfo.NO_FIRING);
        case "BUILDER_TEMPLATE":
          return WindowedValues.<String>builder()
              .setValue("in-" + slot)
              .setTimestamp(ts)
              .setWindow(w1)
              .setPaneInfo(PaneInfo.NO_FIRING);
        default:
          throw new IllegalArgumentException("Unknown templateKind: " + kind);
      }
    }

    int nextSlot() {
      return index++ & RING_MASK;
    }
  }

  /** Thread-local benchmark state for {@link WindowedValues.FullWindowedValueCoder}. */
  @State(Scope.Thread)
  public static class CoderState {
    /** Selects the encoded {@link WindowedValue} shape. */
    @Param({"GLOBAL_MIN", "SINGLE_INTERVAL", "MULTI_INTERVAL"})
    public String windowKind = "SINGLE_INTERVAL";

    Coder<WindowedValue<String>> coder;
    WindowedValue<String>[] values;
    UnsyncByteArrayInputStream[] inputStreams;
    UnsyncByteArrayOutputStream out;
    RecordingByteSizeObserver observer;
    int index;

    /** Initializes coder instances, sample elements, and pre-encoded byte buffers. */
    @Setup
    public void setUp() throws IOException {
      Instant ts = new Instant(1_700_000_000_000L);
      IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
      IntervalWindow w2 =
          new IntervalWindow(
              ts.plus(Duration.standardMinutes(1)), ts.plus(Duration.standardMinutes(2)));
      @SuppressWarnings("unchecked")
      WindowedValue<String>[] typedValues =
          (WindowedValue<String>[]) new WindowedValue<?>[RING_SIZE];
      values = typedValues;
      inputStreams = new UnsyncByteArrayInputStream[RING_SIZE];
      out = new UnsyncByteArrayOutputStream(256);
      observer = new RecordingByteSizeObserver();

      if ("GLOBAL_MIN".equals(windowKind)) {
        coder = WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
      } else {
        coder = WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());
      }

      for (int i = 0; i < RING_SIZE; i++) {
        String payload = "payload-element-" + i;
        if ("GLOBAL_MIN".equals(windowKind)) {
          values[i] = WindowedValues.valueInGlobalWindow(payload);
        } else if ("SINGLE_INTERVAL".equals(windowKind)) {
          values[i] =
              WindowedValues.of(payload, ts.plus(Duration.millis(i)), w1, PaneInfo.NO_FIRING);
        } else {
          values[i] =
              WindowedValues.of(
                  payload,
                  ts.plus(Duration.millis(i)),
                  Arrays.asList(w1, w2),
                  PaneInfo.ON_TIME_AND_ONLY_FIRING);
        }
        out.reset();
        coder.encode(values[i], out);
        inputStreams[i] = new UnsyncByteArrayInputStream(out.toByteArray());
      }
      index = 0;
    }

    int nextSlot() {
      return index++ & RING_MASK;
    }
  }

  @Benchmark
  public void windowedReceiverOutputTagged(ReceiverState s) {
    int i = s.nextSlot();
    s.taggedReceivers[i].output(s.values[i]);
  }

  @Benchmark
  public void windowedReceiverOutputUntagged(ReceiverState s) {
    int i = s.nextSlot();
    s.untaggedReceivers[i].output(s.values[i]);
  }

  @Benchmark
  public void windowedReceiverOutputViaBuilder(ReceiverState s) {
    int i = s.nextSlot();
    s.taggedReceivers[i].builder(s.values[i]).output();
  }

  @Benchmark
  public void windowedReceiverOutputWithTimestamp(ReceiverState s) {
    int i = s.nextSlot();
    s.taggedReceivers[i].outputWithTimestamp(s.values[i], s.timestamps[i]);
  }

  @Benchmark
  public void windowedReceiverOutputWithTimestampViaBuilder(ReceiverState s) {
    int i = s.nextSlot();
    s.taggedReceivers[i].builder(s.values[i]).setTimestamp(s.timestamps[i]).output();
  }

  @Benchmark
  public void windowedReceiverOutputWindowedValue(ReceiverState s) {
    int i = s.nextSlot();
    s.taggedReceivers[i].outputWindowedValue(
        s.values[i], s.timestamps[i], s.explicitWindows, PaneInfo.NO_FIRING);
  }

  @Benchmark
  public void windowedReceiverOutputWindowedValueViaBuilder(ReceiverState s) {
    int i = s.nextSlot();
    s.taggedReceivers[i]
        .builder(s.values[i])
        .setTimestamp(s.timestamps[i])
        .setWindows(s.explicitWindows)
        .setPaneInfo(PaneInfo.NO_FIRING)
        .output();
  }

  @Benchmark
  public void rowReceiverOutput(ReceiverState s) {
    int i = s.nextSlot();
    s.rowReceivers[i].output(s.rows[i]);
  }

  @Benchmark
  public void rowReceiverOutputViaBuilder(ReceiverState s) {
    int i = s.nextSlot();
    s.rowReceivers[i].builder(s.rows[i]).output();
  }

  @Benchmark
  public void rowReceiverOutputWithTimestamp(ReceiverState s) {
    int i = s.nextSlot();
    s.rowReceivers[i].outputWithTimestamp(s.rows[i], s.timestamps[i]);
  }

  @Benchmark
  public void windowedReceiverPerElementConstruction(ReceiverState s) {
    int i = s.nextSlot();
    OutputReceiver<String> receiver =
        DoFnOutputReceivers.windowedReceiver(
            s.context, OutputBuilderSuppliers.supplierForElement(s.templates[i]), s.mainTag);
    receiver.output(s.values[i]);
  }

  @Benchmark
  public int fullWindowedValueCoderEncode(CoderState s) throws IOException {
    int i = s.nextSlot();
    s.out.reset();
    s.coder.encode(s.values[i], s.out);
    int size = s.out.size();
    byte[] buf = s.out.buffer();
    return size ^ (buf[0] & 0xFF) ^ (buf[size - 1] & 0xFF);
  }

  @Benchmark
  public WindowedValue<String> fullWindowedValueCoderDecode(CoderState s) throws IOException {
    UnsyncByteArrayInputStream in = s.inputStreams[s.nextSlot()];
    in.reset();
    return s.coder.decode(in);
  }

  @Benchmark
  public long fullWindowedValueCoderByteSizeObserver(CoderState s) throws Exception {
    int i = s.nextSlot();
    s.coder.registerByteSizeObserver(s.values[i], s.observer);
    s.observer.advance();
    return s.observer.lastSize;
  }
}
