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
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Operator for summing of long values extracted from elements. The sum is operated upon
 * defined key and window.
 * 
 * Example:
 * 
 * <pre>{@code
 *   Dataset<Pair<String, Long>> summed = SumByKey.of(elements)
 *       .keyBy(Pair::getFirst)
 *       .valueBy(Pair::getSecond)
 *       .output();
 * }</pre>
 */
@Derived(
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class SumByKey<IN, KEY, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, KEY, Pair<KEY, Long>, W, SumByKey<IN, KEY, W>> {
  
  public static class OfBuilder implements Builders.Of {
    
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> KeyByBuilder<IN> of(Dataset<IN> input) {
      return new KeyByBuilder<>(name, input);
    }
  }

  public static class KeyByBuilder<IN> implements Builders.KeyBy<IN> {
    private final String name;
    private final Dataset<IN> input;
    
    KeyByBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    
    @Override
    public <KEY> ByBuilder2<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new ByBuilder2<>(name, input, keyExtractor);
    }
  }
  public static class ByBuilder2<IN, KEY>
      extends PartitioningBuilder<KEY, ByBuilder2<IN, KEY>>
      implements Builders.WindowBy<IN>, Builders.Output<Pair<KEY, Long>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private UnaryFunction<IN, Long> valueExtractor = e -> 1L;
    
    ByBuilder2(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }
    
    public ByBuilder2<IN, KEY> valueBy(UnaryFunction<IN, Long> valueExtractor) {
      this.valueExtractor = valueExtractor;
      return this;
    }
    
    @Override
    public <W extends Window> OutputBuilder<IN, KEY, W>
    windowBy(Windowing<IN, W> windowing) {
      return windowBy(windowing, null);
    }
    
    @Override
    public <W extends Window> OutputBuilder<IN, KEY, W>
    windowBy(Windowing<IN, W> windowing, ExtractEventTime<IN> eventTimeAssigner) {
      return new OutputBuilder<>(name, input, keyExtractor, valueExtractor,
              windowing, eventTimeAssigner, this);
    }
    
    @Override
    public Dataset<Pair<KEY, Long>> output() {
      return new OutputBuilder<>(name, input,
          keyExtractor, valueExtractor, null, null, this)
          .output();
    }
  }
  public static class OutputBuilder<IN, KEY, W extends Window>
      extends PartitioningBuilder<KEY, OutputBuilder<IN, KEY, W>>
      implements Builders.Output<Pair<KEY, Long>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, Long> valueExtractor;
    @Nullable
    private final Windowing<IN, W> windowing;
    @Nullable
    private final ExtractEventTime<IN> eventTimeAssigner;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, KEY> keyExtractor,
                  UnaryFunction<IN, Long> valueExtractor,
                  @Nullable Windowing<IN, W> windowing,
                  @Nullable ExtractEventTime<IN> eventTimeAssigner,
                  PartitioningBuilder<KEY, ?> partitioning) {
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }
    @Override
    public Dataset<Pair<KEY, Long>> output() {
      Flow flow = input.getFlow();
      SumByKey<IN, KEY, W> sumByKey =
          new SumByKey<>(name, flow, input, keyExtractor, valueExtractor,
                  windowing, eventTimeAssigner, getPartitioning());
      flow.add(sumByKey);
      return sumByKey.output();
    }
  }

  /**
   * Starts building a nameless {@link SumByKey} operator to process
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
  public static <IN> KeyByBuilder<IN> of(Dataset<IN> input) {
    return new KeyByBuilder<>("SumByKey", input);
  }

  /**
   * Starts building a named {@link SumByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final UnaryFunction<IN, Long> valueExtractor;

  SumByKey(String name,
           Flow flow,
           Dataset<IN> input,
           UnaryFunction<IN, KEY> keyExtractor,
           UnaryFunction<IN, Long> valueExtractor,
           @Nullable Windowing<IN, W> windowing,
           @Nullable ExtractEventTime<IN> eventTimeAssigner,
           Partitioning<KEY> partitioning)
  {
    super(name, flow, input, keyExtractor, windowing, eventTimeAssigner, partitioning);
    this.valueExtractor = valueExtractor;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    ReduceByKey<IN, KEY, Long, Long, W> reduceByKey =
        new ReduceByKey<>(getName(), input.getFlow(), input,
        keyExtractor, valueExtractor, windowing, eventTimeAssigner,
                Sums.ofLongs(), getPartitioning());
    return DAG.of(reduceByKey);
  }
}
