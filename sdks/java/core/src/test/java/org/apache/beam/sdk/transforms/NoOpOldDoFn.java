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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.joda.time.Instant;

/**
 * A {@link OldDoFn} that does nothing with provided elements. Used for testing
 * methods provided by the OldDoFn abstract class.
 *
 * @param <InputT> unused.
 * @param <OutputT> unused.
 */
class NoOpOldDoFn<InputT, OutputT> extends OldDoFn<InputT, OutputT> {
  @Override
  public void processElement(OldDoFn<InputT, OutputT>.ProcessContext c) throws Exception {
  }

  /**
   * Returns a new NoOp Context.
   */
  public OldDoFn<InputT, OutputT>.Context context() {
    return new NoOpDoFnContext();
  }

  /**
   * Returns a new NoOp Process Context.
   */
  public OldDoFn<InputT, OutputT>.ProcessContext processContext() {
    return new NoOpDoFnProcessContext();
  }

  /**
   * A {@link OldDoFn.Context} that does nothing and returns exclusively null.
   */
  private class NoOpDoFnContext extends OldDoFn<InputT, OutputT>.Context {
    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }
    @Override
    public void output(OutputT output) {
    }
    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
    }
    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
    }
    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output,
        Instant timestamp) {
    }
    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT>
        createAggregatorInternal(String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
      return null;
    }
  }

  /**
   * A {@link OldDoFn.ProcessContext} that does nothing and returns exclusively
   * null.
   */
  private class NoOpDoFnProcessContext extends OldDoFn<InputT, OutputT>.ProcessContext {
    @Override
    public InputT element() {
      return null;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return null;
    }

    @Override
    public Instant timestamp() {
      return null;
    }

    @Override
    public BoundedWindow window() {
      return null;
    }

    @Override
    public PaneInfo pane() {
      return null;
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }

    @Override
    public void output(OutputT output) {}

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {}

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {}

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output,
        Instant timestamp) {}

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT>
        createAggregatorInternal(String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
      return null;
    }

  }
}
