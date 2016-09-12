
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.io.Serializable;
import java.util.Optional;
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

  static enum Type {
    /** Processing time based. */
    PROCESSING,
    /** Event time. */
    EVENT
  }

  /**
   * Assign window IDs to given input element.
   * The element will always have assigned old window ID, which can be reused
   * by this windowing.
   * The default windowing assigned on input is derived from batch windowing.
   * @returns set of windows to be assign this element into, never null.
   */
  Set<WindowID<GROUP, LABEL>> assignWindowsToElement(
      WindowedElement<?, ?, T> input);

  /**
   * Create the window context for given window ID.
   * The context is created when processing elements belonging to the
   * same group (i.e. after grouping the elements).
   */
  W createWindowContext(WindowID<GROUP, LABEL> id);


  /**
   * Retrieve time characteristic of this windowing.
   */
  default Type getType() {
    return Type.PROCESSING;
  }


  /**
   * Retrieve time-assignment function if event time is used.
   */
  default Optional<UnaryFunction<T, Long>> getTimestampAssigner() {
    return Optional.empty();
  }


}
