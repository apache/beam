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
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * An interface that runs a {@link GlobalCombineFn} with unified APIs.
 *
 * <p>Different combine functions have their own implementations. For example, the implementation
 * can skip allocating {@code Combine.Context}, if the combine function doesn't use it.
 */
public interface GlobalCombineFnRunner<InputT, AccumT, OutputT> extends Serializable {
  /**
   * Forwards the call to a {@link GlobalCombineFn} to create the accumulator.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT createAccumulator(PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link GlobalCombineFn} to add the input.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT addInput(AccumT accumulator, InputT value, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link GlobalCombineFn} to merge accumulators.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT mergeAccumulators(Iterable<AccumT> accumulators, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link GlobalCombineFn} to extract the output.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  OutputT extractOutput(AccumT accumulator, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);

  /**
   * Forwards the call to a {@link GlobalCombineFn} to compact the accumulator.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from
   * {@link PipelineOptions} and {@link SideInputReader} if it is required.
   */
  AccumT compact(AccumT accumulator, PipelineOptions options,
      SideInputReader sideInputReader, Collection<? extends BoundedWindow> windows);
}
