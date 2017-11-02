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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

/**
 * A data sink that stores data in list.
 */
public class ListDataSink<T> implements DataSink<T> {

  // global storage for all existing ListDataSinks
  private static final Map<ListDataSink<?>, Map<Integer, List<?>>> storage =
          Collections.synchronizedMap(new WeakHashMap<>());

  public static <T> ListDataSink<T> get() {
    return new ListDataSink<>();
  }

  class ListWriter implements Writer<T> {
    final List<T> output = new ArrayList<>();
    final List<T> commitOutputs;
    final int partitionId;

    ListWriter(int partitionId, List<T> commitOutputs) {
      this.partitionId = partitionId;
      this.commitOutputs = commitOutputs;
    }

    @Override
    public void write(T elem) throws IOException {
      output.add(elem);
    }

    @Override
    public synchronized void commit() throws IOException {
      commitOutputs.addAll(output);
    }

    @Override
    public void close() throws IOException {
      // nop
    }

  }


  private final int sinkId = System.identityHashCode(this);
  private final List<ListWriter> writers = Collections.synchronizedList(new ArrayList<>());

  @SuppressWarnings("unchecked")
  protected ListDataSink() {
    // save outputs to static storage
    storage.put((ListDataSink) this, Collections.synchronizedMap(new HashMap<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Writer<T> openWriter(int partitionId) {
    ArrayList tmp = new ArrayList<>();
    Map<Integer, List<?>> sinkData = storage.get(this);
    List partitionData = (List) sinkData.putIfAbsent(partitionId, tmp);
    ListWriter w = new ListWriter(partitionId, partitionData == null ? tmp : partitionData);
    writers.add(w);
    return w;
  }

  @Override
  public void commit() throws IOException {
    // nop
  }

  @Override
  public void rollback() {
    // nop
  }

  @SuppressWarnings("unchecked")
  public List<T> getOutputs() {
    return (List) storage.get(this).values()
        .stream().flatMap(v -> v.stream())
        .collect(Collectors.toList());
  }

  public List<T> getUncommittedOutputs() {
    synchronized (writers) {
      return writers.stream()
          .flatMap(w -> w.output.stream())
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ListDataSink)) return false;

    ListDataSink<?> that = (ListDataSink<?>) o;

    return sinkId == that.sinkId;
  }

  @Override
  public int hashCode() {
    return sinkId;
  }
}
