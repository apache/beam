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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import javax.annotation.Nullable;

/**
 * Operator with internal state.
 */
public abstract class StateAwareWindowWiseOperator<
    IN, WIN, KIN, KEY, OUT, W extends Window,
    OP extends StateAwareWindowWiseOperator<IN, WIN, KIN, KEY, OUT, W, OP>>
    extends WindowWiseOperator<IN, WIN, OUT, W>
    implements StateAware<KIN, KEY>
{
  
  protected final UnaryFunction<KIN, KEY> keyExtractor;
  protected Partitioning<KEY> partitioning;

  protected StateAwareWindowWiseOperator(
          String name,
          Flow flow,
          @Nullable Windowing<WIN, W> windowing,
          UnaryFunction<KIN, KEY> keyExtractor,
          Partitioning<KEY> partitioning) {
    
    super(name, flow, windowing);
    this.keyExtractor = keyExtractor;
    this.partitioning = partitioning;
  }


  @Override
  public UnaryFunction<KIN, KEY> getKeyExtractor() {
    return keyExtractor;
  }

  @Override
  public Partitioning<KEY> getPartitioning() {
    return partitioning;
  }

  @SuppressWarnings("unchecked")
  public OP setPartitioning(Partitioning<KEY> partitioning) {
    this.partitioning = partitioning;
    return (OP) this;
  }

  @SuppressWarnings("unchecked")
  public OP setPartitioner(Partitioner<KEY> partitioner) {
    int numPartitions = getPartitioning().getNumPartitions();
    this.partitioning = new Partitioning<KEY>() {
      @Override
      public Partitioner<KEY> getPartitioner() {
        return partitioner;
      }

      @Override
      public int getNumPartitions() {
        return numPartitions;
      }
    };
    return (OP) this;
  }

  @SuppressWarnings("unchecked")
  public OP setNumPartitions(int numPartitions) {
    final Partitioner<KEY> partitioner = getPartitioning().getPartitioner();
    this.partitioning = new Partitioning<KEY>() {
      @Override
      public Partitioner<KEY> getPartitioner() {
        return partitioner;
      }

      @Override
      public int getNumPartitions() {
        return numPartitions;
      }
    };
    return (OP) this;
  }


}
