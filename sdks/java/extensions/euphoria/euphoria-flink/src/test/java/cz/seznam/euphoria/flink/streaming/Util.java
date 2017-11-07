/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;

class Util {
  static <W, F, S> Dataset<Triple<W, F, S>>
  extractWindows(Dataset<Pair<F, S>> input, Class<W> expectedWindowType) {
    return FlatMap.of(input)
        .using((UnaryFunctor<Pair<F, S>, Triple<W, F, S>>) (elem, context) -> {
          Object actualWindow = context.getWindow();
          if (actualWindow != null && !expectedWindowType.isAssignableFrom(actualWindow.getClass())) {
            throw new IllegalStateException(
                    "Encountered window of type " + actualWindow.getClass()
                    + " but expected only " + expectedWindowType);
          }
          @SuppressWarnings("unchecked")
          Triple<W, F, S> out = Triple.of((W) actualWindow, elem.getFirst(), elem.getSecond());
          context.collect(out);
        })
        .output();
  }

  static <W, T> Dataset<Pair<W, T>>
  extractWindowsToPair(Dataset<T> input, Class<W> expectedWindowType) {
    return FlatMap.of(input)
        .using((UnaryFunctor<T, Pair<W, T>>) (elem, context) -> {
          Object actualWindow = context.getWindow();
          if (actualWindow != null && !expectedWindowType.isAssignableFrom(actualWindow.getClass())) {
            throw new IllegalStateException(
                    "Encountered window of type " + actualWindow.getClass()
                    + " but expected only " + expectedWindowType);
          }
          @SuppressWarnings("unchecked")
          Pair<W, T> out = Pair.of((W) actualWindow, elem);
          context.collect(out);
        })
        .output();
  }

}
