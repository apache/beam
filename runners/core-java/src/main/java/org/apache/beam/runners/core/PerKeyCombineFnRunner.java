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

import java.io.Serializable;
import java.util.Collection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase.PerKeyCombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SideInputReader;

/**
 * An interface that runs a {@link PerKeyCombineFn} with unified APIs.
 *
 * <p>Different keyed combine functions have their own implementations.
 * For example, the implementation can skip allocating {@code Combine.Context},
 * if the keyed combine function doesn't use it.
 */
public interface PerKeyCombineFnRunner<K, InputT, AccumT, OutputT> extends Serializable {
  /**
   * Forwards the call to a {@link PerKeyCombineFn} to create the accumulator.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT createAccumulator(K key, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to add the input.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT addInput(K key, AccumT accumulator, InputT value, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to merge accumulators.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to extract the output.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  OutputT extractOutput(K key, AccumT accumulator, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to compact the accumulator.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT compact(K key, AccumT accumulator, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);
}
