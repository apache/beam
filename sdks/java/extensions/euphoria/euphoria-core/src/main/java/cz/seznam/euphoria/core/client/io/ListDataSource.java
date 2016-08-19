
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

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
    return new ListDataSource<>(true, Arrays.asList(partitions));
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T> ListDataSource<T> unbounded(List<T>... partitions) {
    return new ListDataSource<>(false, Arrays.asList(partitions));
  }


  final boolean bounded;
  long sleepMs = 0;

  private final int id = System.identityHashCode(this);

  @SuppressWarnings("unchecked")
  private ListDataSource(boolean bounded, List<List<T>> partitions) {
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
    return storage.get(this).stream().map(data -> {
      return new Partition<T>() {

        @Override
        public Set<String> getLocations(){
          return Sets.newHashSet("localhost");
        }

        @Override
        @SuppressWarnings("unchecked")
        public Reader<T> openReader() throws IOException {
          return new Reader<T>() {

            int pos = 0;

            @Override
            public void close() throws IOException {
              // nop
            }

            @Override
            public boolean hasNext() {
              return pos < data.size();
            }

            @Override
            public T next() {
              try {
                if (sleepMs > 0) {
                  Thread.sleep(sleepMs);
                }
              } catch (InterruptedException ex) {
                // nop
              }
              return (T) data.get(pos++);
            }

          };
        }

      };
    }).collect(Collectors.toList());
  }

  @Override
  public boolean isBounded() {
    return bounded;
  }

  /**
   * Set sleep time between emitting of elements.
   */
  public ListDataSource<T> setSleepTime(long timeout) {
    this.sleepMs = timeout;
    return this;
  }

}
