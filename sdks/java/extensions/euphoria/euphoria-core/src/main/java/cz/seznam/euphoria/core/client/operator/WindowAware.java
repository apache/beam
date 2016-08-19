
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;

/**
 * Operator aware of windows.
 */
public interface WindowAware<IN, W extends WindowContext<?, ?>> {

  Windowing<IN, ?, ?, W> getWindowing();

}
