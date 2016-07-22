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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collection;

/**
 * A wrapper for the {@link org.apache.beam.sdk.transforms.ParDo.Bound} Beam transformation.
 * */
public class FlinkParDoBoundWrapper<IN, OUT> extends FlinkAbstractParDoWrapper<IN, OUT, OUT> {

  public FlinkParDoBoundWrapper(PipelineOptions options, WindowingStrategy<?, ?> windowingStrategy, OldDoFn<IN, OUT> doFn) {
    super(options, windowingStrategy, doFn);
  }

  @Override
  public void outputWithTimestampHelper(WindowedValue<IN> inElement, OUT output, Instant timestamp, Collector<WindowedValue<OUT>> collector) {
    checkTimestamp(inElement, timestamp);
    collector.collect(makeWindowedValue(
        output,
        timestamp,
        inElement.getWindows(),
        inElement.getPane()));
  }

  @Override
  public <T> void sideOutputWithTimestampHelper(WindowedValue<IN> inElement, T output, Instant timestamp, Collector<WindowedValue<OUT>> outCollector, TupleTag<T> tag) {
    // ignore the side output, this can happen when a user does not register
    // side outputs but then outputs using a freshly created TupleTag.
    throw new RuntimeException("sideOutput() not not available in ParDo.Bound().");
  }

  @Override
  public WindowingInternals<IN, OUT> windowingInternalsHelper(final WindowedValue<IN> inElement, final Collector<WindowedValue<OUT>> collector) {
    return new WindowingInternals<IN, OUT>() {
      @Override
      public StateInternals stateInternals() {
        throw new NullPointerException("StateInternals are not available for ParDo.Bound().");
      }

      @Override
      public void outputWindowedValue(OUT output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
        collector.collect(makeWindowedValue(output, timestamp, windows, pane));
      }

      @Override
      public TimerInternals timerInternals() {
        throw new NullPointerException("TimeInternals are not available for ParDo.Bound().");
      }

      @Override
      public Collection<? extends BoundedWindow> windows() {
        return inElement.getWindows();
      }

      @Override
      public PaneInfo pane() {
        return inElement.getPane();
      }

      @Override
      public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
        throw new RuntimeException("writePCollectionViewData() not supported in Streaming mode.");
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
        throw new RuntimeException("sideInput() not implemented.");
      }
    };
  }
}
