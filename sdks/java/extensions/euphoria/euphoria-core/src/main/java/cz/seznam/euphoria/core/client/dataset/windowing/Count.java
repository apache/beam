
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Pair;
import java.io.Serializable;
import java.util.Collection;
import static java.util.Collections.singleton;

import java.util.Collections;
import java.util.Set;

/**
 * Count tumbling windowing.
 */
public final class Count<T> implements MergingWindowing<T, Count.CountWindow> {

  private final int maxCount;

  private Count(int maxCount) {
    this.maxCount = maxCount;
  }

  public static final class CountWindow extends Window
          implements Serializable {


    // ~ no equals/hashCode ... every instance is unique
    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }
  } // ~ end of Counted

  @Override
  public Set<CountWindow> assignWindowsToElement(
      WindowedElement<?, T> input) {
    return singleton(new CountWindow());
  }

  @Override
  public Trigger<T, CountWindow> getTrigger() {
    return new CountTrigger<>(maxCount);
  }

  @Override
  public Collection<Pair<Collection<CountWindow>, CountWindow>>
  mergeWindows(Collection<CountWindow> actives)
  {
    return Collections.singletonList(Pair.of(actives, new CountWindow()));
  }

  public static <T> Count<T> of(int count) {
    return new Count<>(count);
  }
  
}
