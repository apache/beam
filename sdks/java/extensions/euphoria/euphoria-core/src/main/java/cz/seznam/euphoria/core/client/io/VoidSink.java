package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;

import java.io.IOException;
import java.net.URI;

public class VoidSink<T> implements DataSink<T> {

  public static class Factory implements DataSinkFactory {
    @Override
    public <T> DataSink<T> get(URI uri, Settings settings) {
      return new VoidSink<T>();
    }
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    return new Writer<T>() {
      @Override
      public void write(T elem) throws IOException {
        // ~ no-op
      }

      @Override
      public void commit() throws IOException {
        // ~ no-op
      }

      @Override
      public void close() throws IOException {
        // ~ no-op
      }
    };
  }

  @Override
  public void commit() throws IOException {
    // ~ no-op
  }

  @Override
  public void rollback() {
    // ~ no-op
  }

}
