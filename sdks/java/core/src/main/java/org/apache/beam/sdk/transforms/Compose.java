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

import java.io.Serializable;
import java.util.Comparator;

/**
 * Function composition for Serializable {@code Comparator} objects and for
 * {@code SerializableFunction} objects.
 */
public class Compose {
  /**
   * Compose a Serializable {@code Comparator} with a {@code SerializableFunction}.  The function
   * is run on the input data, and the output of the function is run through the Comparator.
   *
   * @param comparator Comparator object
   * @param fn         function to map raw inputs into a format understandable by the Comparator
   * @param <T>        type of Comparator
   * @param <CompT>    Comparator type
   * @param <X>        raw input type
   * @return Comparator
   */
  public static <T, CompT extends Comparator<T> & Serializable, X> Comp<X> of
  (final CompT comparator, final SerializableFunction<X, T> fn) {
    return new Comp<X>() {
      @Override
      public int compare(final X a, final X b) {
        return comparator.compare(fn.apply(a), fn.apply(b));
      }
    };
  }

  /**
   * Wrap a function in an Fn object to prepare it for function composition.
   *
   * @param function  function to wrap
   * @param <InputT>  input type of the function
   * @param <OutputT> output type of the function
   * @return Fn object which can have .apply() called on it to compose it with a second function.
   */
  public static <InputT, OutputT> Fn<InputT, OutputT> of(
      final SerializableFunction<InputT, OutputT> function) {
    return new Fn<InputT, OutputT>() {
      @Override
      public OutputT apply(final InputT in) {
        return function.apply(in);
      }
    };
  }

  /**
   * Serializable Comparator class that allows composition.
   *
   * @param <T> the type of the comparator
   */
  public abstract static class Comp<T> implements Comparator<T>, Serializable {
    /**
     * Compose this Comparator with another function.
     *
     * @param inner     the inner function.
     * @param <PrelimT> the input type of the inner function
     * @return a composed function which first calls inner, then calls this
     */
    public <PrelimT> Comp<PrelimT> apply(
        final SerializableFunction<PrelimT, T> inner) {
      final Comp<T> outer = this;
      return new Comp<PrelimT>() {
        @Override
        public int compare(PrelimT a, PrelimT b) {
          return outer.compare(inner.apply(a), inner.apply(b));
        }
      };
    }
  }

  /**
   * Wrapper for a {@code SerializableFunction} that allows composition.
   *
   * @param <InputT>  Input type
   * @param <OutputT> Output type
   */
  public abstract static class Fn<InputT, OutputT>
      implements SerializableFunction<InputT, OutputT> {
    /**
     * Compose this function with another function.
     *
     * @param inner     the inner function.
     * @param <PrelimT> the input type of the inner function
     * @return a composed function which first calls inner, then calls this
     */
    public <PrelimT> Fn<PrelimT, OutputT> of(
        final SerializableFunction<PrelimT, InputT> inner) {
      final Fn<InputT, OutputT> outer = this;
      return new Fn<PrelimT, OutputT>() {
        @Override
        public OutputT apply(final PrelimT input) {
          return outer.apply(inner.apply(input));
        }
      };
    }
  }
}
