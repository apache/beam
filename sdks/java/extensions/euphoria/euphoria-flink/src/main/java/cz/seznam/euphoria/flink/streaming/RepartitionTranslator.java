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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.FlinkElement;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;

class RepartitionTranslator implements StreamingOperatorTranslator<Repartition> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<Repartition> operator,
                                 StreamingExecutorContext context)
  {
    DataStream<FlinkElement> input =
        (DataStream<FlinkElement>) context.getSingleInputStream(operator);
    Partitioning partitioning = operator.getOriginalOperator().getPartitioning();

    PartitionerWrapper flinkPartitioner =
            new PartitionerWrapper<>(partitioning.getPartitioner());

    // ~ parallelism is not set directly to partitionCustom() transformation
    // but instead it's set on downstream operations
    // http://apache-flink-mailing-list-archive.1008284.n3.nabble.com/DataStream-partitionCustom-define-parallelism-td12597.html

    return input.partitionCustom(flinkPartitioner, elem -> elem.getElement());
  }
}
