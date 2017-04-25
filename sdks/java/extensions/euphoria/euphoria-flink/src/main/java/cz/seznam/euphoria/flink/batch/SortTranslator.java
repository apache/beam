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

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.Sort;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

/**
 * Translator of {@code Union} operator.
 */
class SortTranslator implements BatchOperatorTranslator<Sort> {
  
  static boolean wantTranslate(Sort operator) {
    return (operator.getWindowing() == null
                || (!(operator.getWindowing() instanceof MergingWindowing)
                    && !operator.getWindowing().getTrigger().isStateful()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(
      FlinkOperator<Sort> operator,
      BatchExecutorContext context) {
    
    int inputParallelism = Iterables.getOnlyElement(context.getInputOperators(operator)).getParallelism();
    DataSet input = Iterables.getOnlyElement(context.getInputStreams(operator));

    Sort origOperator = operator.getOriginalOperator();

    final Windowing windowing =
        origOperator.getWindowing() == null
            ? AttachedWindowing.INSTANCE
            : origOperator.getWindowing();

    // extracts partitionId
    final UnaryFunction<Object, Integer> udfKey = origOperator.getKeyExtractor();
    final UnaryFunction<Object, Comparable> udfSort = origOperator.getSortByExtractor();

    // ~ extract key/value + timestamp from input elements and assign windows
    ExtractEventTime timeAssigner = origOperator.getEventTimeAssigner();
    
    DataSet<BatchElement> wAssigned =
        input.flatMap((i, c) -> {
          BatchElement wel = (BatchElement) i;
          // assign timestamp if timeAssigner defined
          if (timeAssigner != null) {
            wel.setTimestamp(timeAssigner.extractTimestamp(wel.getElement()));
          }
          Iterable<Window> assigned = windowing.assignWindowsToElement(wel);
          for (Window wid : assigned) {
            Object el = wel.getElement();
            c.collect(new BatchElement<>(wid, wel.getTimestamp(), el));
          }
        })
        .returns(BatchElement.class)
        .name(operator.getName() + "::map-input")
            .setParallelism(inputParallelism);
    
    // ~ repartition and sort partitions
    DataSet<BatchElement> sorted = wAssigned
        .partitionCustom(new SortPartitionerWrapper<>(
            origOperator.getPartitioning().getPartitioner()),
            Utils.wrapQueryable(we -> udfKey.apply(we.getElement()), Integer.class))
        .setParallelism(operator.getParallelism())
        .sortPartition(Utils.wrapQueryable(
            (BatchElement we) -> Tuple2.of(we.getWindow(), udfSort.apply(we.getElement())), 
                (new TypeHint<Tuple2<Window, Comparable>>() {}).getTypeInfo()), 
            Order.ASCENDING)
        .name(operator.getName() + "::sort");
    
    return sorted;
  }
  
  public class SortPartitionerWrapper<T> 
      implements org.apache.flink.api.common.functions.Partitioner<T> {
    
    private final Partitioner<T> partitioner;
    
    public SortPartitionerWrapper(Partitioner<T> partitioner) {
      this.partitioner = partitioner;
    }
    
    @Override
    public int partition(T elem, int numPartitions) {
      int ret = partitioner.getPartition(elem);
      // already presume that the partitioner returns the number in the correct range
      Preconditions.checkArgument(
          ret >= 0 && ret < numPartitions, 
          "Unexpected partition number " + ret + " with number of partitions " + numPartitions);
      return ret;
    }
  }
}
