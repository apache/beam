
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Count tumbling windowing.
 */
public final class Count<T> implements Windowing<T, Batch.BatchWindow> {

  private final int maxCount;

  private Count(int maxCount) {
    this.maxCount = maxCount;
  }

  @Override
  public Set<Batch.BatchWindow> assignWindowsToElement(WindowedElement<?, T> input) {
    return singleton(Batch.BatchWindow.get());
  }

  @Override
  public Trigger<Batch.BatchWindow> getTrigger() {
    return new CountTrigger<>(maxCount);
  }

  public static <T> Count<T> of(int count) {
    return new Count<>(count);
  }
  
}
