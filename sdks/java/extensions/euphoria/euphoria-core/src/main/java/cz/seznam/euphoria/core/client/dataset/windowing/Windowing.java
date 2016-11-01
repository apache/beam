
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.triggers.Trigger;

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
public interface Windowing<T, W extends Window> extends Serializable {

  enum Type {
    /** Processing time based. */
    PROCESSING,
    /** Event time. */
    EVENT
  }

  /**
   * Assign windows to given input element.
   * The element will always have assigned old window, which can be reused
   * by this windowing.
   * The default windowing assigned on input is derived from batch windowing.
   * @return set of windows to be assign this element into, never null.
   */
  Set<W> assignWindowsToElement(WindowedElement<?, T> input);

  /**
   * Retrieve instance of {@link Trigger} associated with the current windowing
   * strategy.
   */
  Trigger<W> getTrigger();

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
