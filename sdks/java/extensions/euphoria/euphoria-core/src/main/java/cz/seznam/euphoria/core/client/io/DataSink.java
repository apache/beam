
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;

/**
 * Sink for a dataset.
 */
public abstract class DataSink<T> {

  /** Open Writer for given partition id (zero based). */
  public abstract Writer<T> openWriter(int partId);

  /** Commit all partitions. */
  public abstract void commit() throws IOException;

  /** Rollbak all partitions. */
  public abstract void rollback();

}
