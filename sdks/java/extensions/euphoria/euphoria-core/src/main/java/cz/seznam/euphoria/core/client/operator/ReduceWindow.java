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
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.util.Pair;

import javax.annotation.Nullable;

/**
 * Reduces all elements in a window. The operator corresponds to
 * {@link ReduceByKey} with the same key for all elements, so the actual key
 * is defined only by window.<p>
 * 
 * Custom {@link Windowing} and {@link Partitioning} can be set, otherwise
 * values from input operator are used.
 */
@Derived(
    state = StateComplexity.CONSTANT_IF_COMBINABLE,
    repartitions = 1
)
public class ReduceWindow<
    IN, VALUE, OUT, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, Byte, OUT, W,
            ReduceWindow<IN, VALUE, OUT, W>> {
  
  public static class OfBuilder implements Builders.Of {
    
    final String name;
    
    OfBuilder(String name) {
      this.name = name;
    }
    
    @Override
    public <T> ValueBuilder<T> of(Dataset<T> input) {
      return new ValueBuilder<>(name, input);
    }
  }

  public static class ValueBuilder<T> {
    final String name;
    final Dataset<T> input;
    ValueBuilder(String name, Dataset<T> input) {
      this.name = name;
      this.input = input;
    }
    public <VALUE> ReduceBuilder<T, VALUE> valueBy(
        UnaryFunction<T, VALUE> valueExtractor) {
      return new ReduceBuilder<>(name, input, valueExtractor);
    }
    public <OUT> OutputBuilder<T, T, OUT> reduceBy(
        ReduceFunction<T, OUT> reducer) {
      return new OutputBuilder<>(name, input, e -> e, reducer);
    }
    public OutputBuilder<T, T, T> combineBy(
        CombinableReduceFunction<T> reducer) {
      return new OutputBuilder<>(name, input, e -> e, reducer);
    }
  }
  
  public static class ReduceBuilder<T, VALUE> {
    final String name;
    final Dataset<T> input;
    final UnaryFunction<T, VALUE> valueExtractor;
    public ReduceBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor) {
      
      this.name = name;
      this.input = input;
      this.valueExtractor = valueExtractor;
    }
    public <OUT> OutputBuilder<T, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return reduceBy((Iterable<VALUE> in, Context<OUT> ctx) -> {
        ctx.collect(reducer.apply(in));
      });
    }
    public <OUT> OutputBuilder<T, VALUE, OUT> reduceBy(
        ReduceFunctor<VALUE, OUT> reducer) {
      return new OutputBuilder<>(name, input, valueExtractor, reducer);
    }
    public OutputBuilder<T, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new OutputBuilder<>(
          name, input, valueExtractor,
          ReduceByKey.toReduceFunctor(reducer));
    }
  }

  public static class OutputBuilder<T, VALUE, OUT>
      implements Builders.WindowBy<T>, OptionalMethodBuilder<OutputBuilder<T, VALUE, OUT>> {
    
    private final String name;
    private final Dataset<T> input;
    private final UnaryFunction<T, VALUE> valueExtractor;
    private final ReduceFunctor<VALUE, OUT> reducer;
    private int numPartitions = -1;
    private Windowing<T, ?> windowing;


    public OutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor,
        ReduceFunction<VALUE, OUT> reducer) {
      this(
          name, input, valueExtractor,
          (Iterable<VALUE> in, Context<OUT> ctx) -> {
            ctx.collect(reducer.apply(in));
          });
    }

    public OutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor,
        ReduceFunctor<VALUE, OUT> reducer) {
      
      this.name = name;
      this.input = input;
      this.valueExtractor = valueExtractor;
      this.reducer = reducer;
    }
    @SuppressWarnings("unchecked")
    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      ReduceWindow<T, VALUE, OUT, ?> operator = new ReduceWindow<>(
          name, flow, input, valueExtractor,
              (Windowing) windowing, reducer, numPartitions);
      flow.add(operator);
      return operator.output();
    }
    
    @Override
    public <W extends Window> OutputBuilder<T, VALUE, OUT>
    windowBy(Windowing<T, W> windowing) {
      this.windowing = windowing;
      return this;
    }
    
    public OutputBuilder<T, VALUE, OUT> setNumPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }
  }

  /**
   * Starts building a nameless {@link ReduceWindow} operator to process
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
  public static <IN> ValueBuilder<IN> of(Dataset<IN> input) {
    return new ValueBuilder<>("ReduceWindow", input);
  }

  /**
   * Starts building a named {@link ReduceWindow} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final ReduceFunctor<VALUE, OUT> reducer;
  final UnaryFunction<IN, VALUE> valueExtractor;

  static final Byte B_ZERO = (byte) 0;

  private ReduceWindow(
          String name,
          Flow flow,
          Dataset<IN> input,
          UnaryFunction<IN, VALUE> valueExtractor,
          @Nullable Windowing<IN, W> windowing,
          ReduceFunctor<VALUE, OUT> reducer,
          int numPartitions) {
    
    super(name, flow, input, e -> B_ZERO, windowing,
        new Partitioning<Byte>() {
          @Override
          public Partitioner<Byte> getPartitioner() {
            return e -> 0;
          }
          @Override
          public int getNumPartitions() {
            return numPartitions > 0 ? numPartitions : input.getNumPartitions();
          }
    });
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
  }

  public ReduceFunctor<VALUE, OUT> getReducer() {
    return reducer;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    // implement this operator via `ReduceByKey`
    ReduceByKey<IN, Byte, VALUE, OUT, W> reduceByKey;
    reduceByKey = new ReduceByKey<>(
        getName() + "::ReduceByKey", getFlow(), input,
        getKeyExtractor(), valueExtractor,
        windowing, reducer, partitioning);
    Dataset<Pair<Byte, OUT>> output = reduceByKey.output();

    MapElements<Pair<Byte, OUT>, OUT> format = new MapElements<>(
        getName() + "::MapElements", getFlow(), output, Pair::getSecond);

    return DAG.of(reduceByKey, format);
  }
}
