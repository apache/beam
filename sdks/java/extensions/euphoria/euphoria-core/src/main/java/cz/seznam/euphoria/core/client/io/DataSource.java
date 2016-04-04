
package cz.seznam.euphoria.core.client.io;

import java.io.Serializable;
import java.util.List;

/**
 * Source of data for dataset.
 */
public interface DataSource<T> extends Serializable {

  /** Retrieve list of all partitions of this source. */
  List<Partition<T>> getPartitions();

  /** Is this datasource bounded? */
  boolean isBounded();

}
