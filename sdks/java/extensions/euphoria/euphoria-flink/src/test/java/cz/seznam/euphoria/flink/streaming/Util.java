/**
 * Copyright 2016 Seznam a.s.
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
  extractWindows(Dataset<Pair<F, S>> input, Class<W> windowType) {
    return FlatMap.of(input)
        .using((UnaryFunctor<Pair<F, S>, Triple<W, F, S>>) (elem, context)
            -> context.collect(Triple.of((W) context.getWindow(), elem.getFirst(), elem.getSecond())))
        .output();
  }
}
