
package cz.seznam.euphoria.core.client.functional;

import java.io.Serializable;

/**
 * Function of two arguments.
 */
@FunctionalInterface
public interface BinaryFunction<LEFT, RIGHT, OUT> extends Serializable {


  /** Return the result of this function. */
  OUT apply(LEFT left, RIGHT right);

}
