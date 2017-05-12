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
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.Sort;
import javax.annotation.Nullable;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

class SortTranslator implements SparkOperatorTranslator<Sort> {
  
  static boolean wantTranslate(Sort operator) {
    return (operator.getWindowing() == null
                || (!(operator.getWindowing() instanceof MergingWindowing)
                    && !operator.getWindowing().getTrigger().isStateful()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(Sort operator,
                              SparkExecutorContext context) {

    final JavaRDD<SparkElement> input = (JavaRDD) context.getSingleInput(operator);
    
    final UnaryFunction<Object, Integer> keyExtractor = operator.getKeyExtractor();
    final UnaryFunction<Object, Comparable> sortByFn = operator.getSortByExtractor();
    final Windowing windowing = operator.getWindowing() == null
            ? AttachedWindowing.INSTANCE
            : operator.getWindowing();

    // ~ extract key/value + timestamp from input elements and assign windows
    JavaPairRDD<Tuple3<Integer, Window, Comparable>, TimestampedElement> tuples = input.flatMapToPair(
            new CompositeKeyExtractor(keyExtractor, sortByFn, windowing));
    
    Partitioner partitioner = new PartitioningWrapper(operator.getPartitioning().getNumPartitions());
    Comparator comparator = new TripleComparator();

    JavaPairRDD<Tuple3<Integer, Window, Comparable>, TimestampedElement> sorted = 
        tuples.repartitionAndSortWithinPartitions(partitioner, comparator);
    
    return sorted.map(t -> {
      Tuple3<Integer, Window, Comparable> kw = t._1();
      TimestampedElement el = t._2();

      // ~ extract timestamp from element rather than from KeyedWindow
      // because in KeyedWindow there is the original timestamp from
      // pre-reduce age
      long timestamp = el.getTimestamp();

      return new SparkElement<>(kw._2(), timestamp, el.getElement());
    });
  }
  
  /**
   * Extracts {@link Window} from {@link SparkElement} and assigns timestamp 
   * according to (optional) eventTimeAssigner.
   * The result composite key tuple consists of [partitionId, window, sortByKey].
   */
  private static class CompositeKeyExtractor
      implements PairFlatMapFunction<SparkElement, Tuple3<Integer, Window, Comparable>, TimestampedElement> {

    private final UnaryFunction<Object, Integer> keyExtractor;
    private final UnaryFunction<Object, Comparable> sortByFn;
    private final Windowing windowing;

    public CompositeKeyExtractor(UnaryFunction<Object, Integer> keyExtractor,
                                 UnaryFunction<Object, Comparable> sortByFn,
                                 Windowing windowing) {
      this.keyExtractor = keyExtractor;
      this.sortByFn = sortByFn;
      this.windowing = windowing;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<Tuple3<Integer, Window, Comparable>, TimestampedElement>> call(SparkElement wel) 
        throws Exception {
      Iterable<Window> windows = windowing.assignWindowsToElement(wel);
      List<Tuple2<Tuple3<Integer, Window, Comparable>, TimestampedElement>> out = new ArrayList<>();
      for (Window wid : windows) {
        Object el = wel.getElement();
        long stamp = (wid instanceof TimedWindow)
                ? ((TimedWindow) wid).maxTimestamp()
                : wel.getTimestamp();
        out.add(new Tuple2<>(
            new Tuple3(keyExtractor.apply(el), wid, sortByFn.apply(el)), 
            new TimestampedElement(stamp, el)));
      }
      return out.iterator();
    }
  }
  
  /**
   * Adapter between Euphoria {@link Partitioning} and Spark {@link Partitioner}. Takes already computed
   * partitionId from the triple.
   */
  private static class PartitioningWrapper extends Partitioner {

    private final int numPartitions;

    public PartitioningWrapper(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int getPartition(Object el) {
      Tuple3<Integer, Window, Comparable> t = (Tuple3<Integer, Window, Comparable>) el;
      return t._1();
    }
  }
  
  private static class TripleComparator implements Comparator<Tuple3<Integer, Window, Comparable>>, Serializable {

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Tuple3<Integer, Window, Comparable> o1, Tuple3<Integer, Window, Comparable> o2) {
      int result = o1._1().compareTo(o2._1());
      if (result == 0) {
        result = o1._2().compareTo(o2._2());
      }
      if (result == 0) {
        result = o1._3().compareTo(o2._3());
      }
      return result;
    }
  }
}
