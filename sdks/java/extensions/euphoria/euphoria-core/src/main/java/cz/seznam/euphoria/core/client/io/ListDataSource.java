
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * A {@code DataSource} that is backed up by simple list.
 */
public class ListDataSource<T> implements DataSource<T> {

  // global storage for all existing ListDataSources
  private static final Map<ListDataSource<?>, List<List<?>>> storage =
      Collections.synchronizedMap(new WeakHashMap<>());

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T> ListDataSource<T> bounded(List<T>... partitions) {
    return new ListDataSource<>(true, Lists.newArrayList(partitions));
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T> ListDataSource<T> unbounded(List<T>... partitions) {
    return new ListDataSource<>(false, Lists.newArrayList(partitions));
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T> ListDataSource<T> of(boolean bounded, List<T> ... partitions) {
    return new ListDataSource<>(bounded, Lists.newArrayList(partitions));
  }

  final boolean bounded;
  long sleepMs = 0;
  long finalSleepMs = 0;

  private final int id = System.identityHashCode(this);

  @SuppressWarnings("unchecked")
  private ListDataSource(boolean bounded, ArrayList<List<T>> partitions) {
    this.bounded = bounded;

    // save partitions to static storage
    storage.put(this, (List) partitions);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ListDataSource) {
      ListDataSource that = (ListDataSource) o;
      return this.id == that.id;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public List<Partition<T>> getPartitions() {
    final int n = storage.get(this).size();
    List<Partition<T>> partitions = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final int partition = i;
      partitions.add(new Partition<T>() {

        @Override
        public Set<String> getLocations(){
          return Sets.newHashSet("localhost");
        }

        @Override
        @SuppressWarnings("unchecked")
        public Reader<T> openReader() throws IOException {
          List<T> data =
              (List<T>) storage.get(ListDataSource.this).get(partition);
          return new Reader<T>() {
            int pos = 0;
            boolean lastHasNext = true;

            @Override
            public void close() throws IOException {
              // nop
            }

            @Override
            public boolean hasNext() {
              boolean hasNext = pos < data.size();
              if (hasNext != lastHasNext && finalSleepMs > 0) {
                lastHasNext = hasNext;
                try {
                  Thread.sleep(finalSleepMs);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
              return hasNext;
            }

            @Override
            public T next() {
              try {
                if (sleepMs > 0) {
                  Thread.sleep(sleepMs);
                }
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
              }
              return (T) data.get(pos++);
            }

          };
        }

      });
    }
    return partitions;
  }

  @Override
  public boolean isBounded() {
    return bounded;
  }

  /**
   * Set sleep time between emitting of elements.
   */
  public ListDataSource<T> withReadDelay(Duration timeout) {
    this.sleepMs = timeout.toMillis();
    return this;
  }

  /**
   * Sets the sleep time to wait after having served the last element.
   */
  public ListDataSource<T> withFinalDelay(Duration timeout) {
    this.finalSleepMs = timeout.toMillis();
    return this;
  }
}
