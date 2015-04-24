/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.joda.time.Instant;

/**
 * A {@link DoFn} that does nothing with provided elements. Used for testing
 * methods provided by the DoFn abstract class.
 *
 * @param <I> unused.
 * @param <O> unused.
 */
class NoOpDoFn<I, O> extends DoFn<I, O> {
  private static final long serialVersionUID = 0L;

  @Override
  public void processElement(DoFn<I, O>.ProcessContext c) throws Exception {
  }

  /**
   * Returns a new NoOp Context.
   */
  public DoFn<I, O>.Context context() {
    return new NoOpDoFnContext();
  }

  /**
   * Returns a new NoOp Process Context.
   */
  public DoFn<I, O>.ProcessContext processContext() {
    return new NoOpDoFnProcessContext();
  }

  /**
   * A {@link DoFn.Context} that does nothing and returns exclusively null.
   */
  private class NoOpDoFnContext extends DoFn<I, O>.Context {
    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }
    @Override
    public void output(O output) {
    }
    @Override
    public void outputWithTimestamp(O output, Instant timestamp) {
    }
    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
    }
    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output,
        Instant timestamp) {
    }
    @Override
    protected <VI, VO> Aggregator<VI, VO> createAggregatorInternal(String name,
        CombineFn<VI, ?, VO> combiner) {
      return null;
    }
  }

  /**
   * A {@link DoFn.ProcessContext} that does nothing and returns exclusively
   * null.
   */
  private class NoOpDoFnProcessContext extends DoFn<I, O>.ProcessContext {
    @Override
    public I element() {
      return null;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return null;
    }

    @Override
    public com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState keyedState() {
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
    public WindowingInternals<I, O> windowingInternals() {
      return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }

    @Override
    public void output(O output) {}

    @Override
    public void outputWithTimestamp(O output, Instant timestamp) {}

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {}

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output,
        Instant timestamp) {}

    @Override
    protected <VI, VO> Aggregator<VI, VO> createAggregatorInternal(String name,
        CombineFn<VI, ?, VO> combiner) {
      return null;
    }

  }
}
