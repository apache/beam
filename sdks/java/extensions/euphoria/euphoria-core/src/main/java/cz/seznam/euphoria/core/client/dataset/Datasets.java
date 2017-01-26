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
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.PartitioningAware;

/**
 * Various dataset related utils.
 */
public class Datasets {

  /** Create output dataset for given operator. */
  @SuppressWarnings("unchecked")
  public static <IN, OUT> Dataset<OUT> createOutputFor(
      Flow flow, Dataset<IN> input, Operator<IN, OUT> op) {

    return new OutputDataset<OUT>(flow, (Operator) op, input.isBounded()) {
      @Override
      @SuppressWarnings("unchecked")
      public <X> Partitioning<X> getPartitioning() {
        if (op instanceof PartitioningAware) {
          // only partitioning aware operators change the partitioning
          PartitioningAware<IN> pa = (PartitioningAware<IN>) op;
          return (Partitioning<X>) pa.getPartitioning();
        }
        return input.getPartitioning();
      }
    };
  }

  /** Create dataset from {@code DataSource}. */
  public static <T> Dataset<T> createInputFromSource(
      Flow flow, DataSource<T> source) {
    
    return new InputDataset<T>(flow, source, source.isBounded()) {
      @Override
      public <X> Partitioning<X> getPartitioning() {
        return new Partitioning<X>() {
          @Override
          public int getNumPartitions() {
            return source.getPartitions().size();
          }
        };
      }
    };
  }
}
