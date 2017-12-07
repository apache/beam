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

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.stream.Stream;

import javax.annotation.Nullable;

/**
 * Reduces all elements in a window. The operator corresponds to
 * {@link ReduceByKey} with the same key for all elements, so the actual key
 * is defined only by window.<p>
 *
 * Custom {@link Windowing} can be set, otherwise values from input operator are used.
 */
@Audience(Audience.Type.CLIENT)
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
    public <OUT> SortableOutputBuilder<T, T, OUT> reduceBy(
        ReduceFunction<T, OUT> reducer) {

      return new SortableOutputBuilder<>(
          name, input, e -> e, reducer, null);
    }
    public OutputBuilder<T, T, T> combineBy(
        CombinableReduceFunction<T> reducer) {
      return new OutputBuilder<>(name, input, e -> e, reducer, null, null);
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
    public <OUT> SortableOutputBuilder<T, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return reduceBy((Stream<VALUE> in, Collector<OUT> ctx) -> {
        ctx.collect(reducer.apply(in));
      });
    }
    public <OUT> SortableOutputBuilder<T, VALUE, OUT> reduceBy(
        ReduceFunctor<VALUE, OUT> reducer) {

      return new SortableOutputBuilder<>(
          name, input, valueExtractor, reducer, null);
    }
    public OutputBuilder<T, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new OutputBuilder<>(
          name, input, valueExtractor,
          ReduceByKey.toReduceFunctor(reducer),
          null, null);
    }
  }

  public static class OutputBuilder<T, VALUE, OUT>
      implements Builders.WindowBy<T, OutputBuilder<T, VALUE, OUT>> {

    final String name;
    final Dataset<T> input;
    final UnaryFunction<T, VALUE> valueExtractor;
    final ReduceFunctor<VALUE, OUT> reducer;
    final Windowing<T, ?> windowing;
    @Nullable
    final BinaryFunction<VALUE, VALUE, Integer> valueComparator;

    public OutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor,
        ReduceFunction<VALUE, OUT> reducer,
        @Nullable Windowing<T, ?> windowing,
        @Nullable BinaryFunction<VALUE, VALUE, Integer> valueComparator) {

      this(
          name, input, valueExtractor,
          (Stream<VALUE> in, Collector<OUT> ctx) -> {
            ctx.collect(reducer.apply(in));
          },
          windowing,
          valueComparator);
    }

    public OutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor,
        ReduceFunctor<VALUE, OUT> reducer,
        Windowing<T, ?> windowing,
        @Nullable BinaryFunction<VALUE, VALUE, Integer> valueComparator) {

      this.name = name;
      this.input = input;
      this.valueExtractor = valueExtractor;
      this.reducer = reducer;
      this.windowing = windowing;
      this.valueComparator = valueComparator;
    }

    @SuppressWarnings("unchecked")
    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      ReduceWindow<T, VALUE, OUT, ?> operator = new ReduceWindow<>(
          name, flow, input, valueExtractor,
              (Windowing) windowing, reducer, valueComparator);
      flow.add(operator);
      return operator.output();
    }

    @Override
    public <W extends Window> OutputBuilder<T, VALUE, OUT>
    windowBy(Windowing<T, W> windowing) {
      return new OutputBuilder<>(
          name, input, valueExtractor, reducer, windowing, valueComparator);
    }

  }

  public static class SortableOutputBuilder<T, VALUE, OUT>
      extends OutputBuilder<T, VALUE, OUT> {

    public SortableOutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor,
        ReduceFunction<VALUE, OUT> reducer,
        @Nullable Windowing<T, ?> windowing) {

      super(name, input, valueExtractor, reducer, windowing, null);
    }

    public SortableOutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor,
        ReduceFunctor<VALUE, OUT> reducer,
        @Nullable Windowing<T, ?> windowing) {

      super(name, input, valueExtractor, reducer, windowing, null);
    }


    /**
     * Sort values going to `reduceBy` function by given comparator.
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     *
     * @return next step builder
     */
    public OutputBuilder<T, VALUE, OUT> withSortedValues(
        BinaryFunction<VALUE, VALUE, Integer> comparator) {

      return new OutputBuilder<>(
          name, input, valueExtractor, reducer, windowing, comparator);
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

  private final ReduceFunctor<VALUE, OUT> reducer;
  final UnaryFunction<IN, VALUE> valueExtractor;
  final BinaryFunction<VALUE, VALUE, Integer> valueComparator;

  private static final Byte B_ZERO = (byte) 0;

  private ReduceWindow(
          String name,
          Flow flow,
          Dataset<IN> input,
          UnaryFunction<IN, VALUE> valueExtractor,
          @Nullable Windowing<IN, W> windowing,
          ReduceFunctor<VALUE, OUT> reducer,
          @Nullable BinaryFunction<VALUE, VALUE, Integer> valueComparator) {

    super(name, flow, input, e -> B_ZERO, windowing);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.valueComparator = valueComparator;
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
        windowing, reducer, valueComparator);
    Dataset<Pair<Byte, OUT>> output = reduceByKey.output();

    MapElements<Pair<Byte, OUT>, OUT> format = new MapElements<>(
        getName() + "::MapElements", getFlow(), output, Pair::getSecond);

    return DAG.of(reduceByKey, format);
  }
}
