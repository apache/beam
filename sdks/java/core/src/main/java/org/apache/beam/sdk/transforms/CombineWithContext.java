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

import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * This class contains combine functions that have access to {@code PipelineOptions} and side inputs
 * through {@code CombineWithContext.Context}.
 *
 * <p>{@link CombineFnWithContext} is for users to extend.
 */
public class CombineWithContext {

  /**
   * Information accessible to all methods in {@code CombineFnWithContext} and {@code
   * KeyedCombineFnWithContext}.
   */
  public abstract static class Context {
    /**
     * Returns the {@code PipelineOptions} specified with the {@link PipelineRunner} invoking this
     * {@code KeyedCombineFn}.
     */
    public abstract PipelineOptions getPipelineOptions();

    /**
     * Returns the value of the side input for the window corresponding to the main input's window
     * in which values are being combined.
     */
    public abstract <T> T sideInput(PCollectionView<T> view);
  }

  /**
   * An internal interface for signaling that a {@code GloballyCombineFn} or a {@code
   * PerKeyCombineFn} needs to access {@code CombineWithContext.Context}.
   *
   * <p>For internal use only.
   */
  public interface RequiresContextInternal {}

  /**
   * A combine function that has access to {@code PipelineOptions} and side inputs through {@code
   * CombineWithContext.Context}.
   *
   * <p>See the equivalent {@link CombineFn} for details about combine functions.
   */
  public abstract static class CombineFnWithContext<InputT, AccumT, OutputT>
      extends CombineFnBase.AbstractGlobalCombineFn<InputT, AccumT, OutputT>
      implements RequiresContextInternal {
    /**
     * Returns a new, mutable accumulator value, representing the accumulation of zero input values.
     *
     * <p>It is equivalent to {@link CombineFn#createAccumulator}, but it has additional access to
     * {@code CombineWithContext.Context}.
     */
    public abstract AccumT createAccumulator(Context c);

    /**
     * Adds the given input value to the given accumulator, returning the new accumulator value.
     *
     * <p>It is equivalent to {@link CombineFn#addInput}, but it has additional access to {@code
     * CombineWithContext.Context}.
     */
    public abstract AccumT addInput(AccumT accumulator, InputT input, Context c);

    /**
     * Returns an accumulator representing the accumulation of all the input values accumulated in
     * the merging accumulators.
     *
     * <p>It is equivalent to {@link CombineFn#mergeAccumulators}, but it has additional access to
     * {@code CombineWithContext.Context}.
     */
    public abstract AccumT mergeAccumulators(Iterable<AccumT> accumulators, Context c);

    /**
     * Returns the output value that is the result of combining all the input values represented by
     * the given accumulator.
     *
     * <p>It is equivalent to {@link CombineFn#extractOutput}, but it has additional access to
     * {@code CombineWithContext.Context}.
     */
    public abstract OutputT extractOutput(AccumT accumulator, Context c);

    /**
     * Returns an accumulator that represents the same logical value as the input accumulator, but
     * may have a more compact representation.
     *
     * <p>It is equivalent to {@link CombineFn#compact}, but it has additional access to {@code
     * CombineWithContext.Context}.
     */
    public AccumT compact(AccumT accumulator, Context c) {
      return accumulator;
    }

    /**
     * Applies this {@code CombineFnWithContext} to a collection of input values to produce a
     * combined output value.
     */
    public OutputT apply(Iterable<? extends InputT> inputs, Context c) {
      AccumT accum = createAccumulator(c);
      for (InputT input : inputs) {
        accum = addInput(accum, input, c);
      }
      return extractOutput(accum, c);
    }

    @Override
    public OutputT defaultValue() {
      throw new UnsupportedOperationException(
          "Override this function to provide the default value.");
    }
  }
}
