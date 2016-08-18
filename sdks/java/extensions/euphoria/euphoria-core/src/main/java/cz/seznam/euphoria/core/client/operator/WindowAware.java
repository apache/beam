
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.dataset.WindowContext;

/**
 * Operator aware of windows.
 */
public interface WindowAware<IN, W extends WindowContext<?, ?>> {

  Windowing<IN, ?, ?, W> getWindowing();

}
