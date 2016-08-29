
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.TriggerScheduler;
import java.io.Serializable;
import java.util.Collection;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Count tumbling windowing.
 */
public final class Count<T> implements
    AlignedWindowing<T, Count.Counted, Count.CountWindowContext>,
    MergingWindowing<T, Void, Count.Counted, Count.CountWindowContext> {

  private final int size;

  private Count(int size) {
    this.size = size;
  }

  public static final class Counted implements Serializable {
    // ~ no equals/hashCode ... every instance is unique
  } // ~ end of Counted

  public static class CountWindowContext extends WindowContext<Void, Counted> {

    int currentCount;

    CountWindowContext(int currentCount) {
      super(WindowID.aligned(new Counted()));
      this.currentCount = currentCount;
    }

    @Override
    public String toString() {
      return "CountWindowContext { currentCount = " + currentCount
          + ", label = " + getWindowID().getLabel() + " }";
    }
  } // ~ end of CountWindowContext

  @Override
  public Set<WindowID<Void, Counted>> assignWindowsToElement(
      WindowedElement<?, ?, T> input) {
    return singleton(WindowID.aligned(new Counted()));
  }

  @Override
  public Collection<Pair<Collection<CountWindowContext>, CountWindowContext>>
  mergeWindows(Collection<CountWindowContext> actives)
  {
    Iterator<CountWindowContext> iter = actives.iterator();
    CountWindowContext r = null;
    while (r == null && iter.hasNext()) {
      CountWindowContext w = iter.next();
      if (w.currentCount < size) {
        r = w;
      }
    }
    if (r == null) {
      return actives.stream()
          .map(a -> Pair.of((Collection<CountWindowContext>) singleton(a), a))
          .collect(Collectors.toList());
    }

    Set<CountWindowContext> merged = null;
    iter = actives.iterator();
    while (iter.hasNext()) {
      CountWindowContext w = iter.next();
      if (r != w && r.currentCount + w.currentCount <= size) {
        r.currentCount += w.currentCount;
        if (merged == null) {
          merged = new HashSet<>();
        }
        merged.add(w);
      }
    }
    if (merged != null && !merged.isEmpty()) {
      merged.add(r);
      return singletonList(Pair.of(merged, r));
    }
    return null;
  }

  @Override
  public CountWindowContext createWindowContext(WindowID<Void, Counted> id) {
    return new CountWindowContext(1);
  }


  @Override
  public boolean isComplete(CountWindowContext window) {
    return window.currentCount >= size;
  }

  public static <T> Count<T> of(int count) {
    return new Count<>(count);
  }
  
}
