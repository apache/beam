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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.CombineFnBase.PerKeyCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;

import java.io.Serializable;
import java.util.Collection;

/**
 * An interface that runs a {@link PerKeyCombineFn} with unified APIs.
 *
 * <p>Different keyed combine functions have their own implementations.
 * For example, the implementation can skip allocating {@code Combine.Context},
 * if the keyed combine function doesn't use it.
 */
public interface PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> extends Serializable {
  /**
   * Returns the {@link PerKeyCombineFn} it holds.
   *
   * <p>It can be a {@code KeyedCombineFn} or a {@code KeyedCombineFnWithContext}.
   */
  public PerKeyCombineFn<K, InputT, AccumT, OutputT> fn();

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to create the accumulator in a {@link DoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code DoFn.ProcessContext}
   * if it is required.
   */
  public AccumT createAccumulator(K key, DoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to add the input in a {@link DoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code DoFn.ProcessContext}
   * if it is required.
   */
  public AccumT addInput(K key, AccumT accumulator, InputT input, DoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to merge accumulators in a {@link DoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code DoFn.ProcessContext}
   * if it is required.
   */
  public AccumT mergeAccumulators(
      K key, Iterable<AccumT> accumulators, DoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to extract the output in a {@link DoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code DoFn.ProcessContext}
   * if it is required.
   */
  public OutputT extractOutput(K key, AccumT accumulator, DoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to compact the accumulator in a {@link DoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code DoFn.ProcessContext}
   * if it is required.
   */
  public AccumT compact(K key, AccumT accumulator, DoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to combine the inputs and extract output
   * in a {@link DoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code DoFn.ProcessContext}
   * if it is required.
   */
  public OutputT apply(K key, Iterable<? extends InputT> inputs, DoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to add all inputs in a {@link DoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code DoFn.ProcessContext}
   * if it is required.
   */
  public AccumT addInputs(K key, Iterable<InputT> inputs, DoFn<?, ?>.ProcessContext c);

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to create the accumulator.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  public AccumT createAccumulator(K key, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to add the input.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  public AccumT addInput(K key, AccumT accumulator, InputT value, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to merge accumulators.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  public AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to extract the output.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  public OutputT extractOutput(K key, AccumT accumulator, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to compact the accumulator.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  public AccumT compact(K key, AccumT accumulator, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);
}
