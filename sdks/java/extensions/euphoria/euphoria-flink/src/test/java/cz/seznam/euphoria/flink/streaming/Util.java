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
