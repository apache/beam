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
package org.apache.beam.runners.spark.translation;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.runners.spark.util.SparkSideInputReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/**
 * An abstract for the SparkRunner implementation of {@link
 * org.apache.beam.sdk.transforms.Combine.CombineFn}.
 */
class SparkAbstractCombineFn implements Serializable {
  private final SerializablePipelineOptions options;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;
  final WindowingStrategy<?, BoundedWindow> windowingStrategy;

  SparkAbstractCombineFn(
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.options = options;
    this.sideInputs = sideInputs;
    this.windowingStrategy = (WindowingStrategy<?, BoundedWindow>) windowingStrategy;
  }

  // each Spark task should get it's own copy of this SparkKeyedCombineFn, and since Spark tasks
  // are single-threaded, it is safe to reuse the context.
  // the combine context is not Serializable so we'll use lazy initialization.
  // ** DO NOT attempt to turn this into a Singleton as Spark may run multiple tasks in parallel
  // in the same JVM (Executor). **
  // ** DO NOT use combineContext directly inside this class, use ctxtForInput instead. **
  private transient SparkCombineContext combineContext;

  SparkCombineContext ctxtForInput(WindowedValue<?> input) {
    if (combineContext == null) {
      combineContext = new SparkCombineContext(options.get(), new SparkSideInputReader(sideInputs));
    }
    return combineContext.forInput(input);
  }

  static <T> Iterable<WindowedValue<T>> sortByWindows(Iterable<WindowedValue<T>> iter) {
    List<WindowedValue<T>> sorted = Lists.newArrayList(iter);
    sorted.sort(Comparator.comparing(o -> Iterables.getOnlyElement(o.getWindows()).maxTimestamp()));
    return sorted;
  }

  static boolean isIntersecting(IntervalWindow union, IntervalWindow window) {
    return union == null || union.intersects(window);
  }

  static IntervalWindow merge(IntervalWindow union, IntervalWindow window) {
    return union == null ? window : union.span(window);
  }

  /** An implementation of {@link CombineWithContext.Context} for the SparkRunner. */
  private static class SparkCombineContext extends CombineWithContext.Context {
    private final PipelineOptions pipelineOptions;
    private final SideInputReader sideInputReader;

    SparkCombineContext(PipelineOptions pipelineOptions, SideInputReader sideInputReader) {
      this.pipelineOptions = pipelineOptions;
      this.sideInputReader = sideInputReader;
    }

    private WindowedValue<?> input = null;

    SparkCombineContext forInput(WindowedValue<?> input) {
      this.input = input;
      return this;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      checkNotNull(input, "Input in SparkCombineContext must not be null!");
      // validate element window.
      final Collection<? extends BoundedWindow> elementWindows = input.getWindows();
      checkState(
          elementWindows.size() == 1,
          "sideInput can only be called when the main " + "input element is in exactly one window");
      return sideInputReader.get(view, elementWindows.iterator().next());
    }
  }
}
