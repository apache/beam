
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A data sink that stores data in list.
 */
public class ListDataSink<T> implements DataSink<T> {
  
  public static <T> ListDataSink<T> get(int numPartitions) {
    return new ListDataSink<>(numPartitions);
  }

  final List<List<T>> outputs = new ArrayList<>();

  private ListDataSink(int numPartitions) {
    for (int i = 0; i < numPartitions; i++) {
      outputs.add(null);
    }
  }

  @Override
  public Writer<T> openWriter(int partitionId) {

    final List<T> output = new ArrayList<>();
    outputs.set(partitionId, output);

    return new Writer<T>() {

      @Override
      public void write(T elem) throws IOException {
        output.add(elem);
      }

      @Override
      public void commit() throws IOException{
        // nop
      }

      @Override
      public void close() throws IOException {
        // nop
      }

    };
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

}
