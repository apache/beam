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

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.partitioning.RangePartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Sorts the input dataset.<p>
 * 
 * The user is supposed to provide a function that extracts a comparable object
 * from the input object. The extracted object is than passed to a provided partitioner
 * to partition the result.<p>
 * 
 * To ensure that all elements from a specific range end up 
 * in the same partition - i.e. perform total sort ordering - it is recommended 
 * to use {@link RangePartitioning}, otherwise the user is responsible for his partitioning.<p>
 * 
 * If user does not provide custom {@link Partitioning} or {@link Partitioner} and number of input 
 * partitions differs from 1 (single partition), the program crashes, because runtime does not 
 * know how to partition the result. Input sampling is not supported for now.<p> 
 * 
 * Custom {@link Windowing} and {@link Partitioning} can be set, otherwise values from
 * input operator are used.<p>
 * 
 * Example:
 *
 * <pre>{@code
 *  Dataset<Pair<String, Double>> input = ...;
 *  Dataset<Pair<String, Double>> sorted =
 *         Sort.named("SORTED-BY-SCORE")
 *            .of(input)
 *            .by(Pair::getSecond)
 *            .partitioning(new RangePartitioning(0.2, 0.4, 0.6, 0.8)) // numPartitions = 5
 *            .output();
 * }</pre>
 * 
 * The above example sorts the paired input by the second field. The sorted elements can be
 * found in 5 partitions of corresponding intervals as follows:
 * <ul>
 *   <li>-Inf to 0.2</li>
 *   <li> 0.2 to 0.4</li>
 *   <li> 0.4 to 0.6</li>
 *   <li> 0.6 to 0.8</li>
 *   <li> 0.8 to Inf</li>
 * </ul>
 *
 */
@Derived(
    state = StateComplexity.LINEAR,
    repartitions = 1
)
public class Sort<IN, S extends Comparable<? super S>, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<IN, IN, IN, Integer, IN, W, Sort<IN, S, W>> {
  
  private static final class Sorted<V>
      implements State<V, V>, StateSupport.MergeFrom<Sorted<V>> {

    @SuppressWarnings("unchecked")
    static final ListStorageDescriptor SORT_STATE_DESCR =
        ListStorageDescriptor.of("sort", (Class) Object.class);

    final ListStorage<V> curr;
    final Comparator<V> cmp;
    
    @SuppressWarnings("unchecked")
    Sorted(StorageProvider storageProvider, Comparator<V> cmp) {
      this.curr = (ListStorage<V>) storageProvider.getListStorage(SORT_STATE_DESCR);
      this.cmp = cmp;
    }
    
    @Override
    public void add(V element) {
      curr.add(element);
    }
    
    @Override
    public void flush(Context<V> ctx) {
      List<V> toSort = Lists.newArrayList(curr.get());
      Collections.sort(toSort, cmp);
      toSort.forEach(ctx::collect);
    }

    @Override
    public void close() {
      curr.clear();
    }

    @Override
    public void mergeFrom(Sorted<V> other) {
      for (V v : other.curr.get()) {
        add(v);
      }
    }
  }

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> ByBuilder<IN> of(Dataset<IN> input) {
      return new ByBuilder<>(name, input);
    }
  }

  public static class ByBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    ByBuilder(String name, Dataset<IN> input) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
    }

    /**
     * The extractor function that defines the {@link Comparable} object which is used 
     * in sorting.
     * 
     * @param <S> type of comparable object
     * @param sortByFn the extractor function
     * @return the next builder to complete the setup of the {@link Sort} operator
     */
    public <S extends Comparable<? super S>> WindowByBuilder<IN, S> by(UnaryFunction<IN, S> sortByFn) {
      return new WindowByBuilder<>(name, input, requireNonNull(sortByFn));
    }
  }

  public static class WindowByBuilder<IN, S extends Comparable<? super S>>
      extends PartitioningBuilder<S, WindowByBuilder<IN, S>>
      implements Builders.WindowBy<IN>, Builders.Output<IN>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, S> sortByFn;

    WindowByBuilder(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, S> sortByFn)
    {
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.sortByFn = requireNonNull(sortByFn);
    }

    @Override
    public <W extends Window>
    OutputBuilder<IN, S, W>
    windowBy(Windowing<IN, W> windowing) {
      return new OutputBuilder<>(name, input, sortByFn, this, requireNonNull(windowing));
    }

    @Override
    public Dataset<IN> output() {
      return new OutputBuilder<>(name, input, sortByFn, this, null).output();
    }
  }

  public static class OutputBuilder<
      IN, S extends Comparable<? super S>, W extends Window>
      extends PartitioningBuilder<S, OutputBuilder<IN, S, W>>
      implements Builders.Output<IN>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, S> sortByFn;
    @Nullable
    private final Windowing<IN, W> windowing;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, S> sortByFn,
                  PartitioningBuilder<S, ?> partitioning,
                  @Nullable Windowing<IN, W> windowing) {

      super(partitioning);

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.sortByFn = requireNonNull(sortByFn);
      this.windowing = windowing;
    }

    @Override
    public Dataset<IN> output() {
      Preconditions.checkArgument(validPartitioning(getPartitioning()),
          "Non-single partitioning with default partitioner is not supported on Sort operator. "
          + "Set single partition or define custom partitioner, e.g. RangePartitioner.");
      Flow flow = input.getFlow();
      Sort<IN, S, W> top =
          new Sort<>(flow, name, input, sortByFn, getPartitioning(), windowing);
      flow.add(top);
      return top.output();
    }

    private static boolean validPartitioning(Partitioning<?> partitioning) {
      return !partitioning.hasDefaultPartitioner() || partitioning.getNumPartitions() == 1;
    }
  }

  /**
   * Starts building a nameless {@link Sort} operator to process
   * the given input dataset.
   *
   * @param <IN> the type of elements of the input dataset
   *
   * @param input the input data set to be processed
   *
   * @return a builder to complete the setup of the new operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <IN> ByBuilder<IN> of(Dataset<IN> input) {
    return new ByBuilder<>("Sort", input);
  }

  /**
   * Starts building a named {@link Sort} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  // ~ -----------------------------------------------------------------------------

  private final UnaryFunction<IN, S> sortByFn;

  Sort(Flow flow,
            String name,
            Dataset<IN> input,
            UnaryFunction<IN, S> sortByFn,
            Partitioning<S> partitioning,
            @Nullable Windowing<IN, W> windowing) {
    super(name, flow, input, 
        // Key is actually the number of the final partition - it ensures that all records
        // in one partition (and same window) get into the same state in ReduceStateByKey 
        // where they are later sorted.
        // At the same time the key (partition number) is simply used inside partitioner
        // to ensure that partitioning and states work together.
        new PartitionKeyExtractor<>(sortByFn, partitioning), 
        windowing,
        new HashPartitioning<>(partitioning.getNumPartitions()));
    
    this.sortByFn = sortByFn;
  }

  public UnaryFunction<IN, S> getSortByExtractor() {
    return sortByFn;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();
    
    final StateSupport.MergeFromStateMerger<IN, IN, Sorted<IN>> stateCombiner = 
        new StateSupport.MergeFromStateMerger<>();
    final SortByComparator<IN, S> comparator = new SortByComparator<>(sortByFn);
    ReduceStateByKey<IN, Integer, IN, IN, Sorted<IN>, W> reduce =
        new ReduceStateByKey<>(getName() + "::ReduceStateByKey", flow, input,
                keyExtractor,
                e -> e,
                windowing,
                (StateFactory<IN, IN, Sorted<IN>>)
                    (provider, ctx) -> new Sorted<>(provider, comparator),
                stateCombiner,
                partitioning);

    MapElements<Pair<Integer, IN>, IN> format = 
        new MapElements<>(getName() + "::MapElements", flow, reduce.output(),
            Pair::getSecond);

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }

  private static class SortByComparator<V, S extends Comparable<? super S>> 
      implements Comparator<V>, Serializable {

    private final UnaryFunction<V, S> sortByFn;
    
    public SortByComparator(UnaryFunction<V, S> sortByFn) {
      this.sortByFn = sortByFn;
    }

    @Override
    public int compare(V o1, V o2) {
      return sortByFn.apply(o1).compareTo(sortByFn.apply(o2));
    }
  }
  
  private static class PartitionKeyExtractor<IN, S extends Comparable<? super S>> 
      implements UnaryFunction<IN, Integer> {

    private final UnaryFunction<IN, S> sortByFn;
    private final Partitioner<S> partitioner;
    private final int numPartitions;
    
    public PartitionKeyExtractor(UnaryFunction<IN, S> sortByFn, Partitioning<S> partitioning) {
      this.sortByFn = sortByFn;
      this.partitioner = partitioning.getPartitioner();
      this.numPartitions = partitioning.getNumPartitions();
    }

    @Override
    public Integer apply(IN what) {
      int partitionId = partitioner.getPartition(sortByFn.apply(what));
      return (partitionId & Integer.MAX_VALUE) % numPartitions;
    }
  }
}
