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

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.OutputBuilderSupplier;
import org.apache.beam.sdk.util.OutputBuilderSuppliers;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for {@link DoFnOutputReceivers}, {@link WindowedValues}, and {@link PaneInfo}
 * per-element hot paths.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class OutputReceiverBenchmark {

  private static final class NoopDoFn extends DoFn<String, String> {}

  @State(Scope.Thread)
  public static class ReceiverState {
    final PipelineOptions options = PipelineOptionsFactory.create();
    final DoFn<String, String> doFn = new NoopDoFn();
    final TupleTag<String> mainTag = new TupleTag<>("main");
    final Instant ts = new Instant(1_700_000_000_000L);
    final Instant outTs = new Instant(1_700_000_001_000L);
    final IntervalWindow window = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    final WindowedValue<String> elem =
        WindowedValues.of("input-element", ts, window, PaneInfo.NO_FIRING);
    final OutputBuilderSupplier builderSupplier = OutputBuilderSuppliers.supplierForElement(elem);

    DoFn<String, String>.WindowedContext context;
    OutputReceiver<String> cachedReceiver;
    WindowedValue<?> lastOutput;

    @Setup
    public void setUp() {
      context =
          doFn.new WindowedContext() {
            @Override
            public PipelineOptions getPipelineOptions() {
              return options;
            }

            @Override
            public void output(String output) {
              lastOutput = elem.withValue(output);
            }

            @Override
            public void outputWithTimestamp(String output, Instant timestamp) {
              lastOutput =
                  WindowedValues.of(
                      output,
                      timestamp,
                      elem.getWindows(),
                      elem.getPaneInfo(),
                      elem.getRecordId(),
                      elem.getRecordOffset(),
                      elem.causedByDrain(),
                      elem.getOpenTelemetryContext(),
                      elem.getValueKind());
            }

            @Override
            public void outputWindowedValue(
                String output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo paneInfo) {
              lastOutput = WindowedValues.of(output, timestamp, windows, paneInfo);
            }

            @Override
            public void outputWindowedValue(WindowedValue<String> windowedValue) {
              lastOutput = windowedValue;
            }

            @Override
            public <T> void output(TupleTag<T> tag, T output) {
              lastOutput = elem.withValue(output);
            }

            @Override
            public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
              lastOutput =
                  WindowedValues.of(
                      output,
                      timestamp,
                      elem.getWindows(),
                      elem.getPaneInfo(),
                      elem.getRecordId(),
                      elem.getRecordOffset(),
                      elem.causedByDrain(),
                      elem.getOpenTelemetryContext(),
                      elem.getValueKind());
            }

            @Override
            public <T> void outputWindowedValue(
                TupleTag<T> tag,
                T output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo paneInfo) {
              lastOutput = WindowedValues.of(output, timestamp, windows, paneInfo);
            }

            @Override
            public <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedValue) {
              lastOutput = windowedValue;
            }
          };
      cachedReceiver = DoFnOutputReceivers.windowedReceiver(context, builderSupplier, mainTag);
    }
  }

  @Benchmark
  public void windowedReceiverOutput(ReceiverState s, Blackhole bh) {
    s.cachedReceiver.output("output-element");
    bh.consume(s.lastOutput);
  }

  @Benchmark
  public void windowedReceiverOutputWithTimestamp(ReceiverState s, Blackhole bh) {
    s.cachedReceiver.outputWithTimestamp("output-element", s.outTs);
    bh.consume(s.lastOutput);
  }

  @Benchmark
  public Collection<? extends BoundedWindow> singleWindowGetWindows(ReceiverState s) {
    return s.elem.getWindows();
  }

  @Benchmark
  public PaneInfo paneInfoWithElementMetadataNoChange() {
    return PaneInfo.NO_FIRING.withElementMetadata(false);
  }
}
