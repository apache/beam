
package cz.seznam.euphoria.core.client.functional;

import java.io.Serializable;

/**
 * Function of single argument.
 */
@FunctionalInterface
public interface UnaryFunction<IN, OUT> extends Serializable {
  
  /** Return the result of this function. */
  OUT apply(IN what);

}
