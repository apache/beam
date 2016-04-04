
package cz.seznam.euphoria.core.client.io;

import java.io.Serializable;
import java.util.Set;

/**
 * Single partition of dataset.
 */
public interface Partition<T> extends Iterable<T>, Serializable {

  /** Get location strings (hostnames) of this partition. */
  Set<String> getLocations();

}
