/**
 * Copyright 2016 Seznam a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.PartitioningAware;

import java.io.Serializable;
import java.net.URI;
import java.util.Collection;

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
  Operator<?, T> getProducer();

  /**
   * Retrieve collection of consumers of this dataset.
   * This returns the list of currently known consumers (this can chnage
   * if another consumer is added to the flow).
   */
  Collection<Operator<?, ?>> getConsumers();

  /**
   * Retrieve partitioning for this dataset.
   * The dataset might be partitioned by some other type
   * (using some extraction function).
   */
  <X> Partitioning<X> getPartitioning();


  /** Is this a bounded dataset? */
  boolean isBounded();


  default void persist(URI uri) throws Exception {
    persist(getFlow().createOutput(uri));
  }

  /** Persist this dataset. */
  void persist(DataSink<T> sink);


  default void checkpoint(URI uri) throws Exception {
    checkpoint(getFlow().createOutput(uri));
  }

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
