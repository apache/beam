
package cz.seznam.euphoria.core.client.io;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writer for data to a particular partition.
 */
public interface Writer<T> extends Closeable {

  /** Write element to the output. */
  void write(T elem) throws IOException;

  /** Commit the write process. */
  void commit() throws IOException;

  /** Rollback the write process. Optional operation. */
  default void rollback() throws IOException {}

  @Override
  void close() throws IOException;
}
