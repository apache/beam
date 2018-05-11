/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.util;

import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.executor.util.SingleValueContext;
import java.io.Serializable;
import java.util.stream.Stream;

/** Apply a folding function. */
public class Fold implements Serializable {

  /**
   * Return a {@link CombinableReduceFunction} that performs a fold operation and emits result after
   * fold of all input data.
   *
   * @param <T> element type
   * @param fold the fold function
   * @return the {@link CombinableReduceFunction}
   */
  public static <T> CombinableReduceFunction<T> of(BinaryFunction<T, T, T> fold) {
    return s ->
        s.reduce(fold::apply)
            .orElseThrow(() -> new IllegalStateException("Received empty stream on input!"));
  }

  /**
   * Return a {@link CombinableReduceFunction} that performs a fold operation and emits result after
   * fold of all input data.
   *
   * @param <T> element type
   * @param identity the zero element
   * @param fold the fold function
   * @return the {@link CombinableReduceFunction}
   */
  public static <T> CombinableReduceFunction<T> of(T identity, BinaryFunction<T, T, T> fold) {

    return s -> s.reduce(identity, fold::apply);
  }

  /**
   * Return a {@link ReduceFunctor} that performs a fold operation and emits result after fold of
   * all input data.
   *
   * @param <T> element type
   * @param identity the zero element
   * @param fold the fold function
   * @return the {@link CombinableReduceFunction}
   */
  public static <T> ReduceFunctor<T, T> of(T identity, BinaryFunctor<T, T, T> fold) {

    return (Stream<T> s, Collector<T> ctx) -> {
      SingleValueContext<T> wrap = new SingleValueContext<>(ctx.asContext());
      T ret =
          s.reduce(
              identity,
              (a, b) -> {
                fold.apply(a, b, wrap);
                return wrap.getAndResetValue();
              });
      ctx.collect(ret);
    };
  }

  /**
   * Return a {@link ReduceFunctor} that performs a fold operation and emits partial results after
   * each input element.
   *
   * @param <IN> type of input value
   * @param <OUT> type of output value
   * @param identity the zero element
   * @param fold the fold function
   * @return the {@link ReduceFunctor}
   */
  @SuppressWarnings("ReturnValueIgnored") // TODO : remove
  public static <IN, OUT> ReduceFunctor<IN, OUT> whileEmittingEach(
      OUT identity, BinaryFunction<OUT, IN, OUT> fold) {

    return (s, ctx) ->
        s.reduce(
            identity,
            (a, b) -> {
              OUT v = fold.apply(a, b);
              ctx.collect(v);
              return v;
            },
            (a, b) -> {
              if (b != null) {
                throw new UnsupportedOperationException("Please use sequential streams only!");
              }
              return a;
            });
  }
}
