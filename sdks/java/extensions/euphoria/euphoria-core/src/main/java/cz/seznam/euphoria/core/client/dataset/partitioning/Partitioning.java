/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.dataset.partitioning;

import java.io.Serializable;

/**
 * Partitioning of a dataset.
 *
 * @param <T> the type of elements this partitioning scheme is able to handle
 */
public interface Partitioning<T> extends Serializable {

  Partitioner DEFAULT_PARTITIONER = new DefaultPartitioner();

  /** @return the actual partitioner */
  @SuppressWarnings("unchecked")
  default Partitioner<T> getPartitioner() {
    // ~ unchecked cast is safe here, DEFAULT_PARTITIONER is not dependent on <T>
    return DEFAULT_PARTITIONER;
  }

  default int getNumPartitions() {
    return -1;
  }

  /**
   * @return true if the default partitioner is used - e.g. no other has been explicitly set.
   *         Should not be called after distribution.
   */
  default boolean hasDefaultPartitioner() {
    return getPartitioner() instanceof DefaultPartitioner; // DefaultPartitioner is final
  }
}
