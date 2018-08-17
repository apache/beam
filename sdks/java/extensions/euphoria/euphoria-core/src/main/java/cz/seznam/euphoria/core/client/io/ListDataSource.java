/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;

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
 *
 * @param <T> the type of elements this source provides
 */
public class ListDataSource<T> implements DataSource<T> {

  // global storage for all existing ListDataSources
  private static final Map<ListDataSource<?>, List<List<?>>> storage =
      Collections.synchronizedMap(new WeakHashMap<>());

  @SafeVarargs
  public static <T> ListDataSource<T> bounded(List<T>... partitions) {
    return of(true, partitions);
  }

  @SafeVarargs
  public static <T> ListDataSource<T> unbounded(List<T>... partitions) {
    return of(false, partitions);
  }

  @SafeVarargs
  public static <T> ListDataSource<T> of(boolean bounded, List<T> ... partitions) {
    return of(bounded, Lists.newArrayList(partitions));
  }
  
  public static <T> ListDataSource<T> of(boolean bounded, List<List<T>> partitions) {
    return new ListDataSource<>(bounded, partitions);
  }

  private final boolean bounded;
  private long sleepMs = 0;
  private long finalSleepMs = 0;

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
              return data.get(pos++);
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
   * Converts this list data source into a bounded data source.
   *
   * @return this instance if it is already bounded, otherwise this source's data
   *          as a bounded list data source
   */
  @SuppressWarnings("unchecked")
  public ListDataSource<T> toBounded() {
    if (bounded) {
      return this;
    }
    List<List<?>> list = storage.get(this);
    return ListDataSource.bounded(list.toArray(new List[list.size()]));
  }

  /**
   * Convert this list data source to an unbouded source unless it is already unbounded.
   *
   * @return this instance if it is an unbounded source, otherwise this source's data
   *          in a new, unbouded list data source
   */
  @SuppressWarnings("unchecked")
  public ListDataSource<T> toUnbounded() {
    if (!bounded) {
      return this;
    }
    List<List<?>> list = storage.get(this);
    return ListDataSource.unbounded(list.toArray(new List[list.size()]));
  }

  /**
   * Set sleep time between emitting of elements.
   *
   * @param timeout the duration to sleep between delivering individual elements
   *
   * @return this instance (for method chaining purposes)
   */
  public ListDataSource<T> withReadDelay(Duration timeout) {
    this.sleepMs = timeout.toMillis();
    return this;
  }

  /**
   * Sets the sleep time to wait after having served the last element.
   *
   * @param timeout the time to sleep before signaling end-of-stream
   *
   * @return this instance (for method chaining purposes)
   */
  public ListDataSource<T> withFinalDelay(Duration timeout) {
    this.finalSleepMs = timeout.toMillis();
    return this;
  }
}
