/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.stream.Stream;

/**
 * Reduces all elements in a window. The operator corresponds to {@link ReduceByKey} with the same
 * key for all elements, so the actual key is defined only by window.
 *
 * <p>Custom {@link Windowing} can be set, otherwise values from input operator are used.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code [valueBy] ................} value extractor function (default: identity)
 *   <li>{@code (combineBy | reduceBy)....} {@link CombinableReduceFunction} or {@link
 *       ReduceFunction} for combinable or non-combinable function
 *   <li>{@code [withSortedValues] .......} use comparator for sorting values prior to being passed
 *       to {@link ReduceFunction} function (applicable only for non-combinable version)
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 *       attached windowing
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT_IF_COMBINABLE, repartitions = 1)
public class ReduceWindow<InputT, V, OutputT, W extends Window<W>>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, InputT, Byte, OutputT, W, ReduceWindow<InputT, V, OutputT, W>> {

  private static final Byte B_ZERO = (byte) 0;
  final UnaryFunction<InputT, V> valueExtractor;
  final BinaryFunction<V, V, Integer> valueComparator;
  private final ReduceFunctor<V, OutputT> reducer;

  private ReduceWindow(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, V> valueExtractor,
      @Nullable Windowing<InputT, W> windowing,
      ReduceFunctor<V, OutputT> reducer,
      @Nullable BinaryFunction<V, V, Integer> valueComparator) {

    super(name, flow, input, e -> B_ZERO, windowing, Collections.emptySet());
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.valueComparator = valueComparator;
  }

  /**
   * Starts building a nameless {@link ReduceWindow} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> ValueBuilder<InputT> of(Dataset<InputT> input) {
    return new ValueBuilder<>("ReduceWindow", input);
  }

  /**
   * Starts building a named {@link ReduceWindow} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public ReduceFunctor<V, OutputT> getReducer() {
    return reducer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    // implement this operator via `ReduceByKey`
    final ReduceByKey rbk;
    final DAG<Operator<?, ?>> dag = DAG.empty();
    if (windowing != null) {
      rbk =
          new ReduceByKey<>(
              getName() + "::ReduceByKey",
              getFlow(),
              input,
              getKeyExtractor(),
              valueExtractor,
              windowing,
              reducer,
              valueComparator,
              getHints());
      dag.add(rbk);
    } else {
      // otherwise we use attached windowing, therefore
      // we already know the window lables and can do group-by these
      // labels to increase parallelism
      FlatMap<InputT, Pair<Window<?>, InputT>> map =
          new FlatMap<>(
              getName() + "::window-to-key",
              getFlow(),
              input,
              (InputT in, Collector<Pair<Window<?>, InputT>> c) -> {
                c.collect(Pair.of(c.getWindow(), in));
              },
              null);
      rbk =
          new ReduceByKey<>(
              getName() + "::ReduceByKey::attached",
              getFlow(),
              map.output(),
              Pair::getFirst,
              p -> valueExtractor.apply(p.getSecond()),
              null,
              reducer,
              valueComparator,
              getHints());
      dag.add(map);
      dag.add(rbk);
    }

    MapElements<Pair<Object, OutputT>, OutputT> format =
        new MapElements<Pair<Object, OutputT>, OutputT>(
            getName() + "::MapElements", getFlow(), (Dataset) rbk.output(), Pair::getSecond);

    dag.add(format);
    return dag;
  }

  /** TODO: complete javadoc. */
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

  /** TODO: complete javadoc. */
  public static class ValueBuilder<T> {
    final String name;
    final Dataset<T> input;

    ValueBuilder(String name, Dataset<T> input) {
      this.name = name;
      this.input = input;
    }

    public <V> ReduceBuilder<T, V> valueBy(UnaryFunction<T, V> valueExtractor) {
      return new ReduceBuilder<>(name, input, valueExtractor);
    }

    public <OutputT> SortableOutputBuilder<T, T, OutputT> reduceBy(
        ReduceFunction<T, OutputT> reducer) {

      return new SortableOutputBuilder<>(
          name,
          input,
          e -> e,
          (Stream<T> s, Collector<OutputT> c) -> {
            c.collect(reducer.apply(s));
          },
          null);
    }

    public <OutputT> SortableOutputBuilder<T, T, OutputT> reduceBy(
        ReduceFunctor<T, OutputT> reducer) {

      return new SortableOutputBuilder<>(name, input, e -> e, reducer, null);
    }

    public OutputBuilder<T, T, T> combineBy(CombinableReduceFunction<T> reducer) {
      return new OutputBuilder<>(name, input, e -> e, reducer, null, null);
    }
  }

  /** TODO: complete javadoc. */
  public static class ReduceBuilder<T, V> {
    final String name;
    final Dataset<T> input;
    final UnaryFunction<T, V> valueExtractor;

    public ReduceBuilder(String name, Dataset<T> input, UnaryFunction<T, V> valueExtractor) {

      this.name = name;
      this.input = input;
      this.valueExtractor = valueExtractor;
    }

    public <OutputT> SortableOutputBuilder<T, V, OutputT> reduceBy(
        ReduceFunction<V, OutputT> reducer) {
      return reduceBy(
          (Stream<V> in, Collector<OutputT> ctx) -> {
            ctx.collect(reducer.apply(in));
          });
    }

    public <OutputT> SortableOutputBuilder<T, V, OutputT> reduceBy(
        ReduceFunctor<V, OutputT> reducer) {

      return new SortableOutputBuilder<>(name, input, valueExtractor, reducer, null);
    }

    public OutputBuilder<T, V, V> combineBy(CombinableReduceFunction<V> reducer) {
      return new OutputBuilder<>(
          name, input, valueExtractor, ReduceByKey.toReduceFunctor(reducer), null, null);
    }
  }

  /** TODO: complete javadoc. */
  public static class OutputBuilder<T, V, OutputT>
      implements Builders.WindowBy<T, OutputBuilder<T, V, OutputT>> {

    final String name;
    final Dataset<T> input;
    final UnaryFunction<T, V> valueExtractor;
    final ReduceFunctor<V, OutputT> reducer;
    final Windowing<T, ?> windowing;
    @Nullable final BinaryFunction<V, V, Integer> valueComparator;

    public OutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, V> valueExtractor,
        ReduceFunction<V, OutputT> reducer,
        @Nullable Windowing<T, ?> windowing,
        @Nullable BinaryFunction<V, V, Integer> valueComparator) {

      this(
          name,
          input,
          valueExtractor,
          (Stream<V> in, Collector<OutputT> ctx) -> {
            ctx.collect(reducer.apply(in));
          },
          windowing,
          valueComparator);
    }

    public OutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, V> valueExtractor,
        ReduceFunctor<V, OutputT> reducer,
        Windowing<T, ?> windowing,
        @Nullable BinaryFunction<V, V, Integer> valueComparator) {

      this.name = name;
      this.input = input;
      this.valueExtractor = valueExtractor;
      this.reducer = reducer;
      this.windowing = windowing;
      this.valueComparator = valueComparator;
    }

    @SuppressWarnings("unchecked")
    public Dataset<OutputT> output() {
      Flow flow = input.getFlow();
      ReduceWindow<T, V, OutputT, ?> operator =
          new ReduceWindow<>(
              name, flow, input, valueExtractor, (Windowing) windowing, reducer, valueComparator);
      flow.add(operator);
      return operator.output();
    }

    @Override
    public <W extends Window<W>> OutputBuilder<T, V, OutputT> windowBy(Windowing<T, W> windowing) {
      return new OutputBuilder<>(name, input, valueExtractor, reducer, windowing, valueComparator);
    }
  }

  /** TODO: complete javadoc. */
  public static class SortableOutputBuilder<T, V, OutputT> extends OutputBuilder<T, V, OutputT> {

    public SortableOutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, V> valueExtractor,
        ReduceFunctor<V, OutputT> reducer,
        @Nullable Windowing<T, ?> windowing) {

      super(name, input, valueExtractor, reducer, windowing, null);
    }

    /**
     * Sort values going to `reduceBy` function by given comparator.
     *
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     * @return next step builder
     */
    public OutputBuilder<T, V, OutputT> withSortedValues(BinaryFunction<V, V, Integer> comparator) {

      return new OutputBuilder<>(name, input, valueExtractor, reducer, windowing, comparator);
    }
  }
}
