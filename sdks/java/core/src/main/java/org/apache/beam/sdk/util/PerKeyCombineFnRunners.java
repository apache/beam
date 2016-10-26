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
package org.apache.beam.sdk.util;

import com.google.common.collect.Iterables;
import java.util.Collection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.PerKeyCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.RequiresContextInternal;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Static utility methods that provide {@link PerKeyCombineFnRunner} implementations
 * for different keyed combine functions.
 */
public class PerKeyCombineFnRunners {
  /**
   * Returns a {@link PerKeyCombineFnRunner} from a {@link PerKeyCombineFn}.
   */
  public static <K, InputT, AccumT, OutputT> PerKeyCombineFnRunner<K, InputT, AccumT, OutputT>
  create(PerKeyCombineFn<K, InputT, AccumT, OutputT> perKeyCombineFn) {
    if (perKeyCombineFn instanceof RequiresContextInternal) {
      return new KeyedCombineFnWithContextRunner<>(
          (KeyedCombineFnWithContext<K, InputT, AccumT, OutputT>) perKeyCombineFn);
    } else {
      return new KeyedCombineFnRunner<>(
          (KeyedCombineFn<K, InputT, AccumT, OutputT>) perKeyCombineFn);
    }
  }

  /**
   * An implementation of {@link PerKeyCombineFnRunner} with {@link KeyedCombineFn}.
   *
   * <p>It forwards functions calls to the {@link KeyedCombineFn}.
   */
  private static class KeyedCombineFnRunner<K, InputT, AccumT, OutputT>
      implements PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> {
    private final KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn;

    private KeyedCombineFnRunner(
        KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn) {
      this.keyedCombineFn = keyedCombineFn;
    }

    @Override
    public KeyedCombineFn<K, InputT, AccumT, OutputT> fn() {
      return keyedCombineFn;
    }

    @Override
    public AccumT createAccumulator(K key, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFn.createAccumulator(key);
    }

    @Override
    public AccumT addInput(
        K key, AccumT accumulator, InputT input, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFn.addInput(key, accumulator, input);
    }

    @Override
    public AccumT mergeAccumulators(
        K key, Iterable<AccumT> accumulators, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFn.mergeAccumulators(key, accumulators);
    }

    @Override
    public OutputT extractOutput(K key, AccumT accumulator, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFn.extractOutput(key, accumulator);
    }

    @Override
    public AccumT compact(K key, AccumT accumulator, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFn.compact(key, accumulator);
    }

    @Override
    public OutputT apply(K key, Iterable<? extends InputT> inputs, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFn.apply(key, inputs);
    }

    @Override
    public AccumT addInputs(K key, Iterable<InputT> inputs, OldDoFn<?, ?>.ProcessContext c) {
      AccumT accum = keyedCombineFn.createAccumulator(key);
      for (InputT input : inputs) {
        accum = keyedCombineFn.addInput(key, accum, input);
      }
      return accum;
    }

    @Override
    public String toString() {
      return keyedCombineFn.toString();
    }

    @Override
    public AccumT createAccumulator(K key, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFn.createAccumulator(key);
    }

    @Override
    public AccumT addInput(K key, AccumT accumulator, InputT input, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFn.addInput(key, accumulator, input);
    }

    @Override
    public AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFn.mergeAccumulators(key, accumulators);
    }

    @Override
    public OutputT extractOutput(K key, AccumT accumulator, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFn.extractOutput(key, accumulator);
    }

    @Override
    public AccumT compact(K key, AccumT accumulator, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFn.compact(key, accumulator);
    }
  }

  /**
   * An implementation of {@link PerKeyCombineFnRunner} with {@link KeyedCombineFnWithContext}.
   *
   * <p>It forwards functions calls to the {@link KeyedCombineFnWithContext}.
   */
  private static class KeyedCombineFnWithContextRunner<K, InputT, AccumT, OutputT>
      implements PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> {
    private final KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> keyedCombineFnWithContext;

    private KeyedCombineFnWithContextRunner(
        KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> keyedCombineFnWithContext) {
      this.keyedCombineFnWithContext = keyedCombineFnWithContext;
    }

    @Override
    public KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> fn() {
      return keyedCombineFnWithContext;
    }

    @Override
    public AccumT createAccumulator(K key, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFnWithContext.createAccumulator(key,
          CombineContextFactory.createFromProcessContext(c));
    }

    @Override
    public AccumT addInput(
        K key, AccumT accumulator, InputT value, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFnWithContext.addInput(key, accumulator, value,
          CombineContextFactory.createFromProcessContext(c));
    }

    @Override
    public AccumT mergeAccumulators(
        K key, Iterable<AccumT> accumulators, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFnWithContext.mergeAccumulators(
          key, accumulators, CombineContextFactory.createFromProcessContext(c));
    }

    @Override
    public OutputT extractOutput(K key, AccumT accumulator, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFnWithContext.extractOutput(key, accumulator,
          CombineContextFactory.createFromProcessContext(c));
    }

    @Override
    public AccumT compact(K key, AccumT accumulator, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFnWithContext.compact(key, accumulator,
          CombineContextFactory.createFromProcessContext(c));
    }

    @Override
    public OutputT apply(K key, Iterable<? extends InputT> inputs, OldDoFn<?, ?>.ProcessContext c) {
      return keyedCombineFnWithContext.apply(key, inputs,
          CombineContextFactory.createFromProcessContext(c));
    }

    @Override
    public AccumT addInputs(K key, Iterable<InputT> inputs, OldDoFn<?, ?>.ProcessContext c) {
      CombineWithContext.Context combineContext = CombineContextFactory.createFromProcessContext(c);
      AccumT accum = keyedCombineFnWithContext.createAccumulator(key, combineContext);
      for (InputT input : inputs) {
        accum = keyedCombineFnWithContext.addInput(key, accum, input, combineContext);
      }
      return accum;
    }

    @Override
    public String toString() {
      return keyedCombineFnWithContext.toString();
    }

    @Override
    public AccumT createAccumulator(K key, PipelineOptions options, SideInputReader sideInputReader,
        Collection<? extends BoundedWindow> windows) {
      return keyedCombineFnWithContext.createAccumulator(key,
        CombineContextFactory.createFromComponents(
          options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public AccumT addInput(K key, AccumT accumulator, InputT input, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFnWithContext.addInput(key, accumulator, input,
        CombineContextFactory.createFromComponents(
          options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFnWithContext.mergeAccumulators(key, accumulators,
        CombineContextFactory.createFromComponents(
          options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public OutputT extractOutput(K key, AccumT accumulator, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFnWithContext.extractOutput(key, accumulator,
        CombineContextFactory.createFromComponents(
          options, sideInputReader, Iterables.getOnlyElement(windows)));
    }

    @Override
    public AccumT compact(K key, AccumT accumulator, PipelineOptions options,
        SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows) {
      return keyedCombineFnWithContext.compact(key, accumulator,
        CombineContextFactory.createFromComponents(
          options, sideInputReader, Iterables.getOnlyElement(windows)));
    }
  }
}
