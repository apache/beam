
package cz.seznam.euphoria.core.client.functional;

import java.io.Serializable;

/**
 * Function taking three arguments.
 */
@FunctionalInterface
public interface TernaryFunction<X, Y, Z, RET> extends Serializable {

  RET apply(X first, Y second, Z third);

}
