

package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.client.io.Collector;
import java.io.Serializable;

/**
 * Functor of two arguments.
 */
@FunctionalInterface
public interface BinaryFunctor<LEFT, RIGHT, OUT> extends Serializable {

  void apply(LEFT left, RIGHT right, Collector<OUT> collector);

}
