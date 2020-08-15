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
package org.apache.beam.sdk.state;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;

/**
 * A specification of a persistent state cell. This includes information necessary to encode the
 * value and details about the intended access pattern.
 *
 * @param <StateT> The type of state being described.
 */
@Experimental(Kind.STATE)
public interface StateSpec<StateT extends State> extends Serializable {

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Use the {@code binder} to create an instance of {@code StateT} appropriate for this address.
   */
  @Internal
  StateT bind(String id, StateBinder binder);

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Perform case analysis on this {@link StateSpec} using the provided {@link Cases}.
   */
  @Internal
  <ResultT> ResultT match(Cases<ResultT> cases);

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Given {code coders} are inferred from type arguments defined for this class. Coders which
   * are already set should take precedence over offered coders.
   *
   * @param coders Array of coders indexed by the type arguments order. Entries might be null if the
   *     coder could not be inferred.
   */
  @Internal
  void offerCoders(Coder[] coders);

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Validates that this {@link StateSpec} has been specified correctly and finalizes it.
   * Automatically invoked when the pipeline is built.
   */
  @Internal
  void finishSpecifying();

  /** Cases for doing a "switch" on the type of {@link StateSpec}. */
  interface Cases<ResultT> {
    ResultT dispatchValue(Coder<?> valueCoder);

    ResultT dispatchBag(Coder<?> elementCoder);

    ResultT dispatchOrderedList(Coder<?> elementCoder);

    ResultT dispatchCombining(Combine.CombineFn<?, ?, ?> combineFn, Coder<?> accumCoder);

    ResultT dispatchMap(Coder<?> keyCoder, Coder<?> valueCoder);

    ResultT dispatchSet(Coder<?> elementCoder);

    /** A base class for a visitor with a default method for cases it is not interested in. */
    abstract class WithDefault<ResultT> implements Cases<ResultT> {

      protected abstract ResultT dispatchDefault();

      @Override
      public ResultT dispatchValue(Coder<?> valueCoder) {
        return dispatchDefault();
      }

      @Override
      public ResultT dispatchBag(Coder<?> elementCoder) {
        return dispatchDefault();
      }

      @Override
      public ResultT dispatchCombining(Combine.CombineFn<?, ?, ?> combineFn, Coder<?> accumCoder) {
        return dispatchDefault();
      }

      @Override
      public ResultT dispatchMap(Coder<?> keyCoder, Coder<?> valueCoder) {
        return dispatchDefault();
      }

      @Override
      public ResultT dispatchSet(Coder<?> elementCoder) {
        return dispatchDefault();
      }
    }
  }
}
