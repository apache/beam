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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import org.apache.spark.Partitioner;

/**
 * Adapter between Euphoria {@link Partitioning} and Spark {@link Partitioner}
 */
class PartitioningWrapper extends Partitioner {

  private final cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner partitioner;
  private final int numPartitions;

  public PartitioningWrapper(Partitioning partitioning) {
    this.partitioner = partitioning.getPartitioner();
    this.numPartitions = partitioning.getNumPartitions();
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int getPartition(Object el) {
    KeyedWindow kw = (KeyedWindow) el;
    int partitionId = partitioner.getPartition(kw.key());

    return (partitionId & Integer.MAX_VALUE) % numPartitions();
  }
}