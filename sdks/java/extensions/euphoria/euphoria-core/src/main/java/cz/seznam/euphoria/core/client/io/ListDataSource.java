
package cz.seznam.euphoria.core.client.io;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@code DataSource} that is backed up by simple list.
 */
public class ListDataSource<T> implements DataSource<T> {

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T> DataSource<T> bounded(List<T>... partitions) {
    return new ListDataSource<>(Arrays.asList(partitions), true);
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static <T> DataSource<T> unbounded(List<T>... partitions) {
    return new ListDataSource<>(Arrays.asList(partitions), false);
  }


  final Collection<List<T>> partitions;
  final boolean bounded;

  private ListDataSource(Collection<List<T>> partitions, boolean bounded) {
    this.partitions = partitions;
    this.bounded = bounded;
  }

  @Override
  public List<Partition<T>> getPartitions() {
    return partitions.stream().map(data -> {
      return new Partition<T>() {

        @Override
        public Set<String> getLocations(){
          return Sets.newHashSet("localhost");
        }

        @Override
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
              return data.get(pos++);
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

}
