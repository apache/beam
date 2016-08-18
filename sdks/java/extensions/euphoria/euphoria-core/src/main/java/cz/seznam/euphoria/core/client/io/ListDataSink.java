
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

/**
 * A data sink that stores data in list.
 */
public class ListDataSink<T> implements DataSink<T> {

  // global storage for all existing ListDataSinks
  private static final Map<ListDataSink<?>, List<List<?>>> storage =
          Collections.synchronizedMap(new WeakHashMap<>());

  public static <T> ListDataSink<T> get(int numPartitions) {
    return new ListDataSink<>(numPartitions);
  }

  class ListWriter implements Writer<T> {
    final List<List<T>> sinkOutputs;

    final List<T> output = new ArrayList<>();
    final int partitionId;

    ListWriter(int partitionId, List<List<T>> sinkOutputs) {
      this.partitionId = partitionId;
      this.sinkOutputs = sinkOutputs;
    }

    @Override
    public void write(T elem) throws IOException {
      output.add(elem);
    }

    @Override
    public synchronized void commit() throws IOException {
      sinkOutputs.set(partitionId, output);
    }

    @Override
    public void close() throws IOException {
      // nop
    }

  }


  private final int sinkId = System.identityHashCode(this);
  private final List<ListWriter> writers = Collections.synchronizedList(new ArrayList<>());

  @SuppressWarnings("unchecked")
  private ListDataSink(int numPartitions) {
    List<List<T>> outputs = new ArrayList<>();

    for (int i = 0; i < numPartitions; i++) {
      outputs.add(null);
    }

    // save outputs to static storage
    storage.put((ListDataSink) this, (List) outputs);
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    ListWriter w = new ListWriter(partitionId, getOutputs());
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
  public List<List<T>> getOutputs() {
    return (List) storage.get(this);
  }

  public List<T> getOutput(int partition) {
    return getOutputs().get(partition);
  }

  public List<List<T>> getUncommittedOutputs() {
    synchronized (writers) {
      return writers.stream()
              .map(w -> w.output)
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
