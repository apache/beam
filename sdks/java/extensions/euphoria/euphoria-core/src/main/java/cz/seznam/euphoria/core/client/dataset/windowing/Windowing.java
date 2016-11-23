
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.Serializable;
import java.util.Set;


/**
 * A windowing policy of a dataset.
 */
public interface Windowing<T, W extends Window> extends Serializable {

  /**
   * Assign windows to given input element.
   * The element will always have assigned old window, which can be reused
   * by this windowing.
   * The default windowing assigned on input is derived from batch windowing.
   * @param el The element to which windows should be assigned.
   * @return set of windows to be assign this element into, never {@code null}.
   */
  Set<W> assignWindowsToElement(WindowedElement<?, T> el);

  /**
   * Retrieve instance of {@link Trigger} associated with the current windowing
   * strategy.
   */
  Trigger<W> getTrigger();
}
