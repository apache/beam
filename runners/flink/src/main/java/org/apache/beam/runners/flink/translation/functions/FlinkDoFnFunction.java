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
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableList;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Encapsulates a {@link org.apache.beam.sdk.transforms.DoFn}
 * inside a Flink {@link org.apache.flink.api.common.functions.RichMapPartitionFunction}.
 */
public class FlinkDoFnFunction<IN, OUT> extends RichMapPartitionFunction<IN, OUT> {

  private final DoFn<IN, OUT> doFn;
  private final SerializedPipelineOptions serializedOptions;

  public FlinkDoFnFunction(DoFn<IN, OUT> doFn, PipelineOptions options) {
    this.doFn = doFn;
    this.serializedOptions = new SerializedPipelineOptions(options);
  }

  @Override
  public void mapPartition(Iterable<IN> values, Collector<OUT> out) throws Exception {
    ProcessContext context = new ProcessContext(doFn, out);
    this.doFn.startBundle(context);
    for (IN value : values) {
      context.inValue = value;
      doFn.processElement(context);
    }
    this.doFn.finishBundle(context);
  }
  
  private class ProcessContext extends DoFn<IN, OUT>.ProcessContext {

    IN inValue;
    Collector<OUT> outCollector;

    public ProcessContext(DoFn<IN, OUT> fn, Collector<OUT> outCollector) {
      fn.super();
      super.setupDelegateAggregators();
      this.outCollector = outCollector;
    }

    @Override
    public IN element() {
      return this.inValue;
    }


    @Override
    public Instant timestamp() {
      return Instant.now();
    }

    @Override
    public BoundedWindow window() {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public PaneInfo pane() {
      return PaneInfo.NO_FIRING;
    }

    @Override
    public WindowingInternals<IN, OUT> windowingInternals() {
      return new WindowingInternals<IN, OUT>() {
        @Override
        public StateInternals stateInternals() {
          return null;
        }

        @Override
        public void outputWindowedValue(OUT output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {

        }

        @Override
        public TimerInternals timerInternals() {
          return null;
        }

        @Override
        public Collection<? extends BoundedWindow> windows() {
          return ImmutableList.of(GlobalWindow.INSTANCE);
        }

        @Override
        public PaneInfo pane() {
          return PaneInfo.NO_FIRING;
        }

        @Override
        public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
          throw new RuntimeException("sideInput() not implemented.");
        }
      };
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return serializedOptions.getPipelineOptions();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      List<T> sideInput = getRuntimeContext().getBroadcastVariable(view.getTagInternal().getId());
      List<WindowedValue<?>> windowedValueList = new ArrayList<>(sideInput.size());
      for (T input : sideInput) {
        windowedValueList.add(WindowedValue.of(input, Instant.now(), ImmutableList.of(GlobalWindow.INSTANCE), pane()));
      }
      return view.fromIterableInternal(windowedValueList);
    }

    @Override
    public void output(OUT output) {
      outCollector.collect(output);
    }

    @Override
    public void outputWithTimestamp(OUT output, Instant timestamp) {
      // not FLink's way, just output normally
      output(output);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      // ignore the side output, this can happen when a user does not register
      // side outputs but then outputs using a freshly created TupleTag.
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      sideOutput(tag, output);
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      SerializableFnAggregatorWrapper<AggInputT, AggOutputT> wrapper = new SerializableFnAggregatorWrapper<>(combiner);
      getRuntimeContext().addAccumulator(name, wrapper);
      return wrapper;
    }


  }
}
