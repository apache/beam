/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/**
 * A {@code DataSource} that is backed up by simple list.
 *
 * @param <T> the type of elements this source provides
 */
@Audience({Audience.Type.CLIENT, Audience.Type.TESTS})
@SuppressWarnings("unchecked")
public class ListDataSource<T> implements BoundedDataSource<T>, UnboundedDataSource<T, Integer> {

  // global storage for all existing ListDataSources
  private static final Map<ListDataSource<?>, List<List<?>>> storage =
      Collections.synchronizedMap(new WeakHashMap<>());
  private final boolean bounded;
  private final int id = System.identityHashCode(this);
  private final int partition;
  private final ListDataSource<T> parent;
  private long sleepMs = 0;
  private long finalSleepMs = 0;

  @SuppressWarnings("unchecked")
  private ListDataSource(boolean bounded, List<List<T>> partitions) {
    this.bounded = bounded;
    this.parent = null;
    this.partition = -1;

    // save partitions to static storage
    storage.put(this, (List) partitions);
  }

  private ListDataSource(ListDataSource<T> parent, int partition) {
    this.bounded = parent.bounded;
    this.parent = parent;
    this.partition = partition;
  }

  @SafeVarargs
  public static <T> ListDataSource<T> bounded(List<T>... partitions) {
    return of(true, partitions);
  }

  @SafeVarargs
  public static <T> ListDataSource<T> unbounded(List<T>... partitions) {
    return of(false, partitions);
  }

  @SafeVarargs
  public static <T> ListDataSource<T> of(boolean bounded, List<T>... partitions) {
    return of(bounded, Lists.newArrayList(partitions));
  }

  public static <T> ListDataSource<T> of(boolean bounded, List<List<T>> partitions) {
    return new ListDataSource<>(bounded, partitions);
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
  public List<UnboundedPartition<T, Integer>> getPartitions() {
    final int n = storage.get(this).size();
    List<UnboundedPartition<T, Integer>> partitions = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final int partition = i;
      partitions.add(
          () -> new UnboundedListReader((List<T>) storage.get(ListDataSource.this).get(partition)));
    }
    return partitions;
  }

  @Override
  public List<BoundedDataSource<T>> split(long desiredSplitBytes) {
    int partition = 0;
    List<BoundedDataSource<T>> ret = new ArrayList<>();
    for (List l : storage.get(this)) {
      ret.add(new ListDataSource(this, partition++));
    }
    return ret;
  }

  @Override
  public Set<String> getLocations() {
    return Collections.singleton("localhost");
  }

  @SuppressWarnings("unchecked")
  @Override
  public BoundedReader<T> openReader() throws IOException {
    final List<Integer> partitions;
    final ListDataSource<T> ref;
    if (partition == -1) {
      partitions =
          IntStream.range(0, storage.get(this).size())
              .mapToObj(Integer::valueOf)
              .collect(Collectors.toList());
      ref = this;
    } else {
      partitions = Arrays.asList(partition);
      ref = parent;
    }
    return new BoundedListReader(
        (List)
            partitions
                .stream()
                .flatMap(i -> storage.get(ref).get(i).stream())
                .collect(Collectors.toList()));
  }

  @Override
  public boolean isBounded() {
    return bounded;
  }

  @Override
  public ListDataSource<T> asBounded() {
    if (isBounded()) {
      return this;
    }
    throw new UnsupportedOperationException("Source is unbounded.");
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListDataSource<T> asUnbounded() {
    if (!isBounded()) {
      return this;
    }
    throw new UnsupportedOperationException("Source is bounded.");
  }

  /**
   * Set sleep time between emitting of elements.
   *
   * @param timeout the duration to sleep between delivering individual elements
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
   * @return this instance (for method chaining purposes)
   */
  public ListDataSource<T> withFinalDelay(Duration timeout) {
    this.finalSleepMs = timeout.toMillis();
    return this;
  }

  private class DataIterator implements CloseableIterator<T> {

    final List<T> data;
    int pos = 0;
    boolean lastHasNext = true;
    T next = null;

    DataIterator(List<T> data) {
      this.data = data;
    }

    @Override
    public void close() throws IOException {
      // nop
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = pos < data.size();
      if (hasNext != lastHasNext) {
        lastHasNext = hasNext;
        try {
          Thread.sleep(finalSleepMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (hasNext) {
        next = data.get(pos);
        try {
          if (sleepMs > 0) {
            Thread.sleep(sleepMs);
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }

      return hasNext;
    }

    @Override
    public T next() {
      T ret = next;
      if (ret == null) {
        throw pos >= data.size()
            ? new NoSuchElementException()
            : new IllegalStateException(
                "Don't call `next` multiple times withou call to `hasNext`");
      }
      next = null;
      pos++;
      return ret;
    }
  }

  private class BoundedListReader extends DataIterator implements BoundedReader<T> {

    BoundedListReader(List<T> data) {
      super(data);
    }
  }

  private class UnboundedListReader extends DataIterator implements UnboundedReader<T, Integer> {

    public UnboundedListReader(List<T> data) {
      super(data);
    }

    @Override
    public Integer getCurrentOffset() {
      return pos;
    }

    @Override
    public void reset(Integer offset) {
      pos = offset;
    }

    @Override
    public void commitOffset(Integer offset) {
      // nop
    }
  }
}
