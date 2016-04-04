
package cz.seznam.euphoria.core.client.dataset;

import java.io.Serializable;

/**
 * Trigger for window.
 */
@FunctionalInterface
public interface Trigger<T> extends Serializable {

  void fire();

}
