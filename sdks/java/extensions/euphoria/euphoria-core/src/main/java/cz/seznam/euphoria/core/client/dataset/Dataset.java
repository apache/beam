
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.io.Serializable;

/**
 * A dataset abstraction.
 */
public interface Dataset<T> extends Serializable {

  /**
   * Retrieve Flow associated with this dataset.
   */
  Flow getFlow();

  /**
   * Retrieve source of data associated with this dataset.
   * This might be null, if this dataset has no explicit source,
   * it is calculated. If this method returns null, getProducer returns non null
   * and vice versa.
   */
  DataSource<T> getSource();

  /** Retrieve operator that produced this dataset (if any). */
  Operator<?, T, ? extends Dataset<T>> getProducer();

  /**
   * Retrieve partitioning for this dataset.
   * The dataset might be partitioned by some other type
   * (using some extraction function).
   */
  <X> Partitioning<X> getPartitioning();
  

  /** Is this a bounded dataset? */
  boolean isBounded();


  /** Persist this dataset. */
  void persist(DataSink<T> sink);


  /** Checkpoint this dataset. */
  void checkpoint(DataSink<T> sink);


  /** Retrieve output sink for this dataset. */
  default DataSink<T> getOutputSink() {
    return null;
  }

  /** Retrieve checkpoint sink for this dataset. */
  default DataSink<T> getCheckpointSink() {
    return null;
  }
  
}
