
package cz.seznam.euphoria.core.client.functional;

import java.io.Serializable;

/**
 * Function taking zero arguments.
 */
public interface VoidFunction<T> extends Serializable {

  T apply();

}
