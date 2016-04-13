
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * Single partition of dataset.
 */
public interface Partition<T> extends Serializable {

  /** Get location strings (hostnames) of this partition. */
  Set<String> getLocations();

  /**
   * Opens a reader over this partition. It the caller's
   * responsibility to close the reader once not needed anymore.
   */
  Reader<T> openReader() throws IOException;

}
