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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import org.apache.flink.api.java.DataSet;

class RepartitionTranslator implements BatchOperatorTranslator<Repartition> {

  @SuppressWarnings("unchecked")
  @Override
  public DataSet translate(FlinkOperator<Repartition> operator,
      BatchExecutorContext context) {
    
    DataSet<BatchElement> input =
        (DataSet<BatchElement>)context.getSingleInputStream(operator);
    
    Partitioning partitioning = operator.getOriginalOperator().getPartitioning();
    PartitionerWrapper flinkPartitioner = 
        new PartitionerWrapper<>(partitioning.getPartitioner());
    
    return input.partitionCustom(
            flinkPartitioner,
            Utils.wrapQueryable((BatchElement we) -> (Comparable) we.getElement(), Comparable.class))
        .setParallelism(operator.getParallelism());
  }
}
