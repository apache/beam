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

import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;

import java.util.Objects;

/**
 * Operator changing partitioning of a dataset.
 */
public interface PartitioningAware<KEY> {

  /** Retrieve output partitioning. */
  Partitioning<KEY> getPartitioning();

  abstract class PartitioningBuilder<KEY, BUILDER> implements OptionalMethodBuilder<BUILDER> {
    private final DefaultPartitioning<KEY> defaultPartitioning;
    private Partitioning<KEY> partitioning;

    public PartitioningBuilder(DefaultPartitioning<KEY> defaultPartitioning) {
      this(defaultPartitioning, null);
    }

    public PartitioningBuilder(DefaultPartitioning<KEY> defaultPartitioning,
                               Partitioning<KEY> partitioning)
    {
      this.defaultPartitioning = Objects.requireNonNull(defaultPartitioning);
      this.partitioning = partitioning;
    }

    public PartitioningBuilder(PartitioningBuilder<KEY, ?> other) {
      this.defaultPartitioning = other.defaultPartitioning;
      this.partitioning = other.partitioning;
    }

    Partitioning<KEY> getPartitioning() {
      if (partitioning == null) {
        return defaultPartitioning;
      }

      return partitioning;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setPartitioning(Partitioning<KEY> p) {
      this.partitioning = Objects.requireNonNull(p);
      return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setPartitioner(Partitioner<KEY> p) {
      this.defaultPartitioning.setPartitioner(p);
      return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setNumPartitions(int numPartitions) {
      this.defaultPartitioning.setNumPartitions(numPartitions);
      return (BUILDER) this;
    }
  }
}
