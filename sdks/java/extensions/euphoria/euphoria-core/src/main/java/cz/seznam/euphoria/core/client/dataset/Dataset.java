/**
 * Copyright 2016 Seznam.cz, a.s.
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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.URI;
import java.util.Collection;

/**
 * A dataset abstraction.
 *
 * @param <T> type of elements of this data set
 */
public interface Dataset<T> extends Serializable {

  /**
   * @return the flow associated with this data set
   */
  Flow getFlow();

  /**
   * Retrieve source of data associated with this dataset.
   * This might be null, if this dataset has no explicit source,
   * it is calculated. If this method returns null, getProducer returns non null
   * and vice versa.
   *
   * @return this dataset's explicit source - if any
   */
  @Nullable
  DataSource<T> getSource();

  /**
   * @return the operator that produced this dataset - if any
   */
  @Nullable
  Operator<?, T> getProducer();

  /**
   * Retrieve collection of consumers of this dataset.
   *
   * @return the list of currently known consumers (this can change
   * if another consumer is added to the flow).
   */
  Collection<Operator<?, ?>> getConsumers();

  /**
   * Determines the parallelism of this data set - if known. Typically,
   * a data set is split into multiple partitions which can be processed
   * in parallel.
   *
   * @return {@code < 0} if the partition count is unknown, otherwise the
   *          count of partitions of this dataset (which can potentially
   *          be processed in parallel)
   */
  int getNumPartitions();

  /**
   * @return {@code true} if this is a bounded data set,
   *         {@code false} if it is unbounded.
   */
  boolean isBounded();

  default void persist(URI uri) throws Exception {
    persist(getFlow().createOutput(uri));
  }

  /**
   * Persist this dataset.
   *
   * @param sink the sink to use to persist this data set's data to
   */
  void persist(DataSink<T> sink);

  default void checkpoint(URI uri) throws Exception {
    checkpoint(getFlow().createOutput(uri));
  }

  /**
   * Checkpoint this dataset.
   *
   * @param sink the sink to use to checkpoint this data set's data to
   */
  void checkpoint(DataSink<T> sink);

  /**
   * Retrieve output sink for this dataset.
   *
   * @return {@code null} if there is no explicitly set sink this
   *          data set is supposed to be persisted to, otherwise the
   *          sink provided through {@link #persist(DataSink)}.
   */
  @Nullable
  default DataSink<T> getOutputSink() {
    return null;
  }

  /**
   * Retrieve checkpoint sink for this dataset.
   *
   * @return {@code null} if no checkpoint sink has been defined,
   *          otherwise the sink provided through {@link #checkpoint(DataSink)}
   */
  @Nullable
  default DataSink<T> getCheckpointSink() {
    return null;
  }
}
