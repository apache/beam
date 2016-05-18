
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A data sink that stores data in list.
 */
public class ListDataSink<T> implements DataSink<T> {

  
  public static <T> ListDataSink<T> get(int numPartitions) {
    return new ListDataSink<>(numPartitions);
  }

  class ListWriter implements Writer<T> {
    final List<T> output = new ArrayList<>();
    final int partitionId;

    ListWriter(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public void write(T elem) throws IOException {
      output.add(elem);
    }

    @Override
    public void commit() throws IOException{
      outputs.set(partitionId, output);
    }

    @Override
    public void close() throws IOException {
      // nop
    }

  }


  final List<List<T>> outputs = new ArrayList<>();
  final List<ListWriter> writers = Collections.synchronizedList(new ArrayList<>());

  private ListDataSink(int numPartitions) {
    for (int i = 0; i < numPartitions; i++) {
      outputs.add(null);
    }
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    ListWriter w = new ListWriter(partitionId);
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

  public List<List<T>> getOutputs() {
    return outputs;
  }

  public List<T> getOutput(int partition) {
    return outputs.get(partition);
  }

  public List<List<T>> getUncommittedOutputs() {
    return writers.stream()
        .map(w -> (List<T>) w.output)
        .collect(Collectors.toList());
  }
}
