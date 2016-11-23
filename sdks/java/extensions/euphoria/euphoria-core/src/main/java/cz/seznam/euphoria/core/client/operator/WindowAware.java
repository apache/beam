
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Operator aware of windows.
 */
public interface WindowAware<IN, W extends Window> {

  Windowing<IN, W> getWindowing();

  /**
   * Window time assigner extracts timestamp from given element.
   */
  UnaryFunction<IN, Long> getEventTimeAssigner();

}
