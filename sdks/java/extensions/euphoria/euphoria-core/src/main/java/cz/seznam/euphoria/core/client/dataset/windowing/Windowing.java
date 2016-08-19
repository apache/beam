
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.executor.TriggerScheduler;

import java.io.Serializable;
import java.util.Set;


/**
 * A windowing policy of a dataset.
 * You can either override method `assignWindowsToElement` if you want
 * to operate on the {@code WindowedElement}, or override
 * `assignWindows` if you want to operate on raw data. There is no
 * need to override both methods.
 */
public interface Windowing<T, GROUP, LABEL, W extends WindowContext<GROUP, LABEL>>
    extends Serializable {


  /**
   * Assign window IDs to given input element.
   * The element will always have assigned old window ID, which can be reused
   * by this windowing.
   * @returns set of windows to be assign this element into, never null.
   */
  default Set<WindowID<GROUP, LABEL>> assignWindowsToElement(
      WindowedElement<GROUP, LABEL, T> input) {
    return assignWindows(input.get());
  }

  /**
   * Assign window IDs to given input data.
   * @returns set of windows to be assign this element into, never null.
   */
  default Set<WindowID<GROUP, LABEL>> assignWindows(T data) {
    throw new UnsupportedOperationException(
        "You have to override either `assignWindowsToElement` or `assignWindowsToData`");
  }

  /**
   * Create the window context for given window ID.
   * The context is created when processing elements belonging to the
   * same group (i.e. after grouping the elements).
   */
  W createWindowContext(WindowID<GROUP, LABEL> id);


  /**
   * Update triggering by given input. This is needed to enable the windowing
   * to move triggering in watermarking processing schemes based on event time.
   */
  default void updateTriggering(TriggerScheduler triggering, T input) {
    triggering.updateProcessed(System.currentTimeMillis());
  }

}
