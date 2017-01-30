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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.operator.Repartition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


class RepartitionTranslator implements SparkOperatorTranslator<Repartition> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(Repartition operator,
                              SparkExecutorContext context) {

    final JavaRDD<WindowedElement> input = (JavaRDD) context.getSingleInput(operator);
    Partitioning partitioning = operator.getPartitioning();

    if (partitioning.getNumPartitions() == 1) {
      // don't need to use partitioner
      return input.repartition(1);
    }

    // ~ map RDD<Object> to RDD<Tuple<Integer, Object>>
    // where Integer is the partition number
    JavaPairRDD<Integer, WindowedElement> pairs = input.mapToPair(
            new TupleByPartition(partitioning));

    pairs = pairs.partitionBy(new IntPartitioner(partitioning.getNumPartitions()));

    return pairs.values();
  }

  private static class TupleByPartition implements PairFunction<WindowedElement, Integer, WindowedElement> {

    private final Partitioning partitioning;

    public TupleByPartition(Partitioning partitioner) {
      this.partitioning = partitioner;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple2<Integer, WindowedElement> call(WindowedElement el)  {
      Partitioner partitioner = partitioning.getPartitioner();
      int partitionId = partitioner.getPartition(el.getElement());
      return new Tuple2<>(
              (partitionId & Integer.MAX_VALUE) % partitioning.getNumPartitions(), el);
    }
  }

  private static class IntPartitioner extends org.apache.spark.Partitioner {

    private final int numPartitions;

    public IntPartitioner(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
      return (int) key;
    }
  }
}
