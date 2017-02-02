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
package cz.seznam.euphoria.flink.functions;

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;

public class PartitionerWrapper<T>
        implements org.apache.flink.api.common.functions.Partitioner<T>
{
  private final Partitioner<T> partitioner;

  public PartitionerWrapper(Partitioner<T> partitioner) {
    this.partitioner = partitioner;
  }

  @Override
  public int partition(T elem, int numPartitions) {
    return (partitioner.getPartition(elem) & Integer.MAX_VALUE) % numPartitions;
  }
}
