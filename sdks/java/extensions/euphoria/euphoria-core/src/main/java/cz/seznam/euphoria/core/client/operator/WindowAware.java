
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.ElementWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;

/**
 * Operator aware of windows.
 */
public interface WindowAware<IN, W extends WindowContext<?, ?>> {

  ElementWindowing<IN, ?, ?, W> getWindowing();

}
