
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * Operator aware of windows.
 */
public interface WindowAware<IN, W extends Window> {

  Windowing<IN, W> getWindowing();

}
