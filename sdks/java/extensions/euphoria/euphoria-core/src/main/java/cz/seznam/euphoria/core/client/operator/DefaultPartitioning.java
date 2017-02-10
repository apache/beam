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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;

import java.util.Objects;

/**
 * Default implementation of {@link Partitioning} with one partition and
 * hash partitioner
 */
public final class DefaultPartitioning<T> implements Partitioning<T> {

  private int numPartitions;
  private Partitioner<T> partitioner;

  public DefaultPartitioning() {
    this(1);
  }

  // ~ unchecked cast is safe, DEFAULT_PARTITIONER does not depend on <T>
  @SuppressWarnings("unchecked")
  public DefaultPartitioning(int numPartitions) {
    this(numPartitions, DEFAULT_PARTITIONER);
  }

  public DefaultPartitioning(int numPartitions, Partitioner<T> partitioner) {
    setNumPartitions(numPartitions);
    setPartitioner(partitioner);
  }

  @Override
  public Partitioner<T> getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(Partitioner<T> partitioner) {
    this.partitioner = Objects.requireNonNull(partitioner);
  }

  @Override
  public int getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    if (numPartitions <= 0) {
      throw new IllegalStateException("numPartitions must be greater than 0");
    }
    this.numPartitions = numPartitions;
  }
}
