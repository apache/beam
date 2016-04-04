
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.client.io.Collector;
import java.io.Serializable;

/**
 * Functor of single argument.
 * Functor can produce zero or more elements in return to a call, for which it
 * uses a collector.
 */
@FunctionalInterface
public interface UnaryFunctor<IN, OUT> extends Serializable {

  void apply(IN elem, Collector<OUT> collector);

}
