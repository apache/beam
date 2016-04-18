
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.dataset.Window;

/**
 * Operator aware of windows.
 */
public interface WindowAware<IN, W extends Window<?>> {

  Windowing<IN, ?, W> getWindowing();

}
