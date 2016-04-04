
package cz.seznam.euphoria.core.client.io;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.util.Settings;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A mock factory creating a stream datasource.
 */
public class MockBatchDataSourceFactory implements DataSourceFactory {

  @Override
  public <T> DataSource<T> get(URI uri, Settings settings) {

    int p = Runtime.getRuntime().availableProcessors();
    final List<Partition<T>> partitions = new ArrayList<>(p);
    for (int i = 0; i < p; i++) {
      partitions.add(new Partition<T>() {

        @Override
        public Set<String> getLocations() {
          return Sets.newHashSet("localhost");
        }

        @Override
        public Iterator<T> iterator() {
          return new Iterator<T>() {

            @Override
            public boolean hasNext() {
              return true;
            }

            @Override
            public T next() {
              return null;
            }
          };
        }

      });
    }
    return new DataSource<T>() {

      @Override
      public List<Partition<T>> getPartitions() {
        return partitions;
      }

      @Override
      public boolean isBounded() {
        return true;
      }

    };
  }

}
