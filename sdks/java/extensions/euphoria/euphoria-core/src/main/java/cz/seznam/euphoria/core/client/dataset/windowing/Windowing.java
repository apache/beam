
package cz.seznam.euphoria.core.client.dataset.windowing;

import java.util.Set;

/**
 * Windowing based on the data element only.
 * This is useful for cases where the assigning function wants to operate
 * on the element itself, not on the {@code WindowedElement} wrapper.
 */
public interface Windowing<T, GROUP, LABEL, W extends WindowContext<GROUP, LABEL>>
    extends ElementWindowing<T, GROUP, LABEL, W> {

  @Override
  public default Set<WindowID<GROUP, LABEL>> assignWindowsToElement(
      WindowedElement<GROUP, LABEL, T> input) {
    
    return assignWindows(input.get());
  }

  /**
   * Assign the element into windows.
   */
  Set<WindowID<GROUP, LABEL>> assignWindows(T element);


}
