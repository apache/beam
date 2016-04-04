
package cz.seznam.euphoria.core.client.io;

import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for data in a given partition.
 */
public abstract class Writer<T> implements Closeable {
  
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
  private boolean closed = false;

  /** Write element to the output. */
  public abstract void write(T elem) throws IOException;

  /** Commit the write process. */
  public abstract void commit() throws IOException;

  /** Rollback the write process. Optional operation. */
  public void rollback() throws UnsupportedOperationException, IOException {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    
    try {
      closed = true;
      rollback();
    } catch (UnsupportedOperationException ex) {
      LOG.info("Failed to rollback the operation", ex);
    }
  }


}
