
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.io.Serializable;

/**
 * Sink for a dataset.
 */
public abstract class DataSink<T> implements Serializable {

  /** Open Writer for given partition id (zero based). */
  public abstract Writer<T> openWriter(int partitionId);

  /** Commit all partitions. */
  public abstract void commit() throws IOException;

  /** Rollback all partitions. */
  public abstract void rollback();

}
