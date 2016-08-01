
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.io.Serializable;

/**
 * Sink for a dataset.
 */
public interface DataSink<T> extends Serializable {

  /**
   *  Perform initialization before writing to the sink.
   *  Called before writing begins. It must be ensured that
   *  implementation of this method is idempotent (may be called
   *  more than once in the case of failure/retry).
   */
  default void initialize() {}

  /** Open {@link Writer} for given partition id (zero based). */
  Writer<T> openWriter(int partitionId);

  /** Commit all partitions. */
  void commit() throws IOException;

  /** Rollback all partitions. */
  void rollback() throws IOException;
}
