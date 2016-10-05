

package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.client.io.Context;
import java.io.Serializable;

/**
 * Functor of two arguments.
 */
@FunctionalInterface
public interface BinaryFunctor<LEFT, RIGHT, OUT> extends Serializable {

  void apply(LEFT left, RIGHT right, Context<OUT> context);

}
