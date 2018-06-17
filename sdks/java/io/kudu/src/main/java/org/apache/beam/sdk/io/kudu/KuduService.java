package org.apache.beam.sdk.io.kudu;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.kudu.client.KuduException;

/** An interface for real, mock, or fake implementations of Kudu services. */
interface KuduService<T> extends Serializable {

  /**
   * Returns a {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader} that will read from Kudu
   * using the spec from {@link org.apache.beam.sdk.io.kudu.KuduIO.KuduSource}.
   */
  BoundedSource.BoundedReader<T> createReader(KuduIO.KuduSource<T> source);

  /** Create a {@link Writer} that writes entities into the KKudu instance. */
  Writer createWriter(KuduIO.Write<T> spec) throws KuduException;

  /** Returns a list containing a serialized scanner per tablet. */
  List<byte[]> createTabletScanners(KuduIO.Read<T> spec) throws KuduException;

  /** Writer for an entity. */
  interface Writer<T> extends AutoCloseable, Serializable {

    /**
     * Opens a new session for writing. This must be called exactly once before calling {@link
     * #write(Object)}.
     */
    void openSession() throws KuduException;

    /**
     * Writes the entity to Kudu. A call to {@link #openSession()} must be made before writing.
     * Writes may be asynchronous in which case implementations must surface errors when the session
     * is closed.
     */
    void write(T entity) throws KuduException;

    /** Closes the session, surfacing any errors that may have occurred during writing. */
    void closeSession() throws Exception;
  }
}
