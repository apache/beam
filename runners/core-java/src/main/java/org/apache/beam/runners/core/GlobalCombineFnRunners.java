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
package org.apache.beam.runners.core;

import java.util.Collection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * Static utility methods that provide {@link GlobalCombineFnRunner} implementations for different
 * combine functions.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class GlobalCombineFnRunners {
  /** Returns a {@link GlobalCombineFnRunner} from a {@link GlobalCombineFn}. */
  public static <InputT, AccumT, OutputT> GlobalCombineFnRunner<InputT, AccumT, OutputT> create(
      GlobalCombineFn<InputT, AccumT, OutputT> globalCombineFn) {
    if (globalCombineFn instanceof CombineFnWithContext) {
      return new CombineFnWithContextRunner<>(
          (CombineFnWithContext<InputT, AccumT, OutputT>) globalCombineFn);
    } else if (globalCombineFn instanceof CombineFn) {
      return new CombineFnRunner<>((CombineFn<InputT, AccumT, OutputT>) globalCombineFn);
    } else {
      throw new IllegalStateException(
          String.format("Unknown type of CombineFn: %s", globalCombineFn.getClass()));
    }
  }

  /**
   * Returns a {@code Combine.Context} from {@code PipelineOptions}, {@code SideInputReader}, and
   * the main input window.
   */
  private static CombineWithContext.Context createFromComponents(
      final PipelineOptions options,
      final SideInputReader sideInputReader,
      final BoundedWindow mainInputWindow) {
    return new CombineWithContext.Context() {
      @Override
      public PipelineOptions getPipelineOptions() {
        return options;
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view) {
        if (!sideInputReader.contains(view)) {
          throw new IllegalArgumentException("calling sideInput() with unknown view");
        }

        BoundedWindow sideInputWindow =
            view.getWindowMappingFn().getSideInputWindow(mainInputWindow);
        return sideInputReader.get(view, sideInputWindow);
      }
    };
  }

  /**
   * An implementation of {@link GlobalCombineFnRunner} with {@link CombineFn}.
   *
   * <p>It forwards functions calls to the {@link CombineFn}.
   */
  private static class CombineFnRunner<InputT, AccumT, OutputT>
      implements org.apache.beam.runners.core.GlobalCombineFnRunner<InputT, AccumT, OutputT> {
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private CombineFnRunner(CombineFn<InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public String toString() {
      return combineFn.toString();
    }

    @Override
    public AccumT createAccumulator(
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFn.createAccumulator();
    }

    @Override
    public AccumT addInput(
        AccumT accumulator,
        InputT input,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFn.addInput(accumulator, input);
    }

    @Override
    public AccumT mergeAccumulators(
        Iterable<AccumT> accumulators,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public OutputT extractOutput(
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFn.extractOutput(accumulator);
    }

    @Override
    public AccumT compact(
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFn.compact(accumulator);
    }
  }

  /**
   * An implementation of {@link org.apache.beam.runners.core.GlobalCombineFnRunner} with {@link
   * CombineFnWithContext}.
   *
   * <p>It forwards functions calls to the {@link CombineFnWithContext}.
   */
  private static class CombineFnWithContextRunner<InputT, AccumT, OutputT>
      implements org.apache.beam.runners.core.GlobalCombineFnRunner<InputT, AccumT, OutputT> {
    private final CombineFnWithContext<InputT, AccumT, OutputT> combineFnWithContext;

    private CombineFnWithContextRunner(
        CombineFnWithContext<InputT, AccumT, OutputT> combineFnWithContext) {
      this.combineFnWithContext = combineFnWithContext;
    }

    @Override
    public String toString() {
      return combineFnWithContext.toString();
    }

    @Override
    public AccumT createAccumulator(
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnWithContext.createAccumulator(
          createFromComponents(options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public AccumT addInput(
        AccumT accumulator,
        InputT input,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnWithContext.addInput(
          accumulator,
          input,
          createFromComponents(options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public AccumT mergeAccumulators(
        Iterable<AccumT> accumulators,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnWithContext.mergeAccumulators(
          accumulators,
          createFromComponents(options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public OutputT extractOutput(
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnWithContext.extractOutput(
          accumulator,
          createFromComponents(options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public AccumT compact(
        AccumT accumulator,
        PipelineOptions options,
        SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return combineFnWithContext.compact(
          accumulator,
          createFromComponents(options, sideInputReader, Iterables.getOnlyElement(windows)));
    }
  }
}
