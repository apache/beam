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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseSingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareUnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Emits top element for defined keys and windows. The elements are compared by comparable objects
 * extracted by user defined function applied on input elements.
 *
 * <p>Custom {@link Windowing} can be set, otherwise values from input operator are used.
 *
 * <p>Example:
 *
 * <pre>{@code
 * TopPerKey.of(elements)
 *      .keyBy(e -> (byte) 0)
 *      .valueBy(e -> e)
 *      .scoreBy(Pair::getSecond)
 *      .output();
 * }</pre>
 *
 * <p>The examples above finds global maximum of all elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 * <li>{@code [named] ..................} give name to the operator [optional]
 * <li>{@code of .......................} input dataset
 * <li>{@code keyBy ....................} key extractor function
 * <li>{@code valueBy ..................} value extractor function
 * <li>{@code scoreBy ..................} {@link UnaryFunction} transforming input elements to
 * {@link Comparable} scores
 * <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no windowing
 * <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 * <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 * <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT, repartitions = 1)
public class TopPerKey<InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, K, Triple<K, V, ScoreT>, W, TopPerKey<InputT, K, V, ScoreT, W>> {

  private final UnaryFunction<InputT, V> valueFn;
  private final UnaryFunction<InputT, ScoreT> scoreFn;

  TopPerKey(
      Flow flow,
      String name,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyFn,
      UnaryFunction<InputT, V> valueFn,
      UnaryFunction<InputT, ScoreT> scoreFn,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      Set<OutputHint> outputHints) {
    super(name, flow, input, keyFn, windowing, euphoriaWindowing, outputHints);

    this.valueFn = valueFn;
    this.scoreFn = scoreFn;
  }

  /**
   * Starts building a nameless {@link TopPerKey} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
    return new KeyByBuilder<>("TopPerKey", input);
  }

  /**
   * Starts building a named {@link TopPerKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(requireNonNull(name));
  }

  public UnaryFunction<InputT, V> getValueExtractor() {
    return valueFn;
  }

  public UnaryFunction<InputT, ScoreT> getScoreExtractor() {
    return scoreFn;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();

    StateSupport.MergeFromStateMerger<Pair<V, ScoreT>, Pair<V, ScoreT>, MaxScored<V, ScoreT>>
        stateCombiner = new StateSupport.MergeFromStateMerger<>();
    ReduceStateByKey<InputT, K, Pair<V, ScoreT>, Pair<V, ScoreT>, MaxScored<V, ScoreT>, W> reduce =
        new ReduceStateByKey<>(
            getName() + "::ReduceStateByKey",
            flow,
            input,
            keyExtractor,
            e -> Pair.of(valueFn.apply(e), scoreFn.apply(e)),
            windowing,
            euphoriaWindowing,
            (StateContext context, Collector<Pair<V, ScoreT>> collector) -> {
              return new MaxScored<>(context.getStorageProvider());
            },
            stateCombiner,
            Collections.emptySet());

    MapElements<Pair<K, Pair<V, ScoreT>>, Triple<K, V, ScoreT>> format =
        new MapElements<>(
            getName() + "::MapElements",
            flow,
            reduce.output(),
            e -> Triple.of(e.getFirst(), e.getSecond().getFirst(), e.getSecond().getSecond()),
            getHints());

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);

    return dag;
  }

  /**
   * Parameters of this operator used in builders.
   */
  private static final class BuiderParams<InputT, K, V, ScoreT extends Comparable<ScoreT>,
      W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, K> keyFn;
    UnaryFunction<InputT, V> valueFn;
    UnaryFunction<InputT, ScoreT> scoreFn;

    public BuiderParams(String name,
        Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }
  }

  /**
   * TODO: complete javadoc.
   */
  private static final class MaxScored<V, CompareT extends Comparable<CompareT>>
      implements State<Pair<V, CompareT>, Pair<V, CompareT>>,
      StateSupport.MergeFrom<MaxScored<V, CompareT>> {

    static final ValueStorageDescriptor<Pair> MAX_STATE_DESCR =
        ValueStorageDescriptor.of("max", Pair.class, Pair.of(null, null));

    final ValueStorage<Pair<V, CompareT>> curr;

    @SuppressWarnings("unchecked")
    MaxScored(StorageProvider storageProvider) {
      curr = (ValueStorage) storageProvider.getValueStorage(MAX_STATE_DESCR);
    }

    @Override
    public void add(Pair<V, CompareT> element) {
      Pair<V, CompareT> c = curr.get();
      if (c.getFirst() == null || element.getSecond().compareTo(c.getSecond()) > 0) {
        curr.set(element);
      }
    }

    @Override
    public void flush(Collector<Pair<V, CompareT>> context) {
      Pair<V, CompareT> c = curr.get();
      if (c.getFirst() != null) {
        context.collect(c);
      }
    }

    @Override
    public void close() {
      curr.clear();
    }

    @Override
    public void mergeFrom(MaxScored<V, CompareT> other) {
      Pair<V, CompareT> o = other.curr.get();
      if (o.getFirst() != null) {
        this.add(o);
      }
    }
  }

  // ~ -----------------------------------------------------------------------------

  /**
   * TODO: complete javadoc.
   */
  public static class OfBuilder implements Builders.Of {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
      return new KeyByBuilder<>(name, requireNonNull(input));
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {

    private final BuiderParams<InputT, ?, ?, ?, ?> params;

    KeyByBuilder(String name, Dataset<InputT> input) {
      params = new BuiderParams<>(name, input);
    }

    @Override
    public <K> ValueByBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyFn) {

      @SuppressWarnings("unchecked") BuiderParams<InputT, K, ?, ?, ?> paramsCasted =
          (BuiderParams<InputT, K, ?, ?, ?>) params;
      paramsCasted.keyFn = requireNonNull(keyFn);

      return new ValueByBuilder<>(paramsCasted);
    }

    @Override
    public <K> ValueByBuilder<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, TypeHint<K> typeHint) {
      return keyBy(TypeAwareUnaryFunction.of(keyExtractor, typeHint));
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class ValueByBuilder<InputT, K> {

    private final BuiderParams<InputT, K, ?, ?, ?> params;

    ValueByBuilder(BuiderParams<InputT, K, ?, ?, ?> params) {
      this.params = params;
    }

    public <V> ScoreByBuilder<InputT, K, V> valueBy(UnaryFunction<InputT, V> valueFn) {

      @SuppressWarnings("unchecked") BuiderParams<InputT, K, V, ?, ?> paramsCasted =
          (BuiderParams<InputT, K, V, ?, ?>) params;

      paramsCasted.valueFn = requireNonNull(valueFn);
      return new ScoreByBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class ScoreByBuilder<InputT, K, V> {

    private final BuiderParams<InputT, K, V, ?, ?> params;

    ScoreByBuilder(BuiderParams<InputT, K, V, ?, ?> params) {
      this.params = params;
    }

    public <ScoreT extends Comparable<ScoreT>> WindowByBuilder<InputT, K, V, ScoreT> scoreBy(
        UnaryFunction<InputT, ScoreT> scoreFn) {

      @SuppressWarnings("unchecked") BuiderParams<InputT, K, V, ScoreT, ?> paramsCasted =
          (BuiderParams<InputT, K, V, ScoreT, ?>) params;

      paramsCasted.scoreFn = requireNonNull(scoreFn);
      return new WindowByBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class WindowByBuilder<InputT, K, V, ScoreT extends Comparable<ScoreT>>
      implements Builders.WindowBy<TriggerByBuilder<InputT, K, V, ScoreT, ?>>,
      Builders.Output<Triple<K, V, ScoreT>>,
      OptionalMethodBuilder<WindowByBuilder<InputT, K, V, ScoreT>,
                OutputBuilder<InputT, K, V, ScoreT, ?>> {

    private final BuiderParams<InputT, K, V, ScoreT, ?> params;

    WindowByBuilder(BuiderParams<InputT, K, V, ScoreT, ?> params) {
      this.params = params;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, K, V, ScoreT, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked") BuiderParams<InputT, K, V, ScoreT, W> paramsCasted =
          (BuiderParams<InputT, K, V, ScoreT, W>) params;

      paramsCasted.windowFn = requireNonNull(windowing);
      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, V, ScoreT, ?> windowBy(
        Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public Dataset<Triple<K, V, ScoreT>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(params).output(outputHints);
    }

    @Override
    public OutputBuilder<InputT, K, V, ScoreT, ?> applyIf(boolean cond,
        UnaryFunction<WindowByBuilder<InputT, K, V, ScoreT>,
            OutputBuilder<InputT, K, V, ScoreT, ?>> applyWhenConditionHolds) {
      Objects.requireNonNull(applyWhenConditionHolds);

      if (cond) {
        return applyWhenConditionHolds.apply(this);
      }

      return new OutputBuilder<>(params);
    }
  }

  /**
   * Trigger defining operator builder.
   */
  public static class TriggerByBuilder<InputT, K, V, ScoreT extends Comparable<ScoreT>,
      W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, K, V, ScoreT, W>> {

    private final BuiderParams<InputT, K, V, ScoreT, W> params;

    TriggerByBuilder(BuiderParams<InputT, K, V, ScoreT, W> params) {
      this.params = params;
    }

    public AccumulatorModeBuilder<InputT, K, V, ScoreT, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }

  }

  /**
   * {@link WindowingStrategy.AccumulationMode} defining operator builder.
   */
  public static class AccumulatorModeBuilder<InputT, K, V, ScoreT extends Comparable<ScoreT>,
      W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, K, V, ScoreT, W>> {

    private final BuiderParams<InputT, K, V, ScoreT, W> params;

    AccumulatorModeBuilder(BuiderParams<InputT, K, V, ScoreT, W> params) {
      this.params = params;
    }

    public OutputBuilder<InputT, K, V, ScoreT, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {

      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }

  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<
      InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends BoundedWindow>
      implements Builders.Output<Triple<K, V, ScoreT>> {

    private final BuiderParams<InputT, K, V, ScoreT, W> params;

    OutputBuilder(BuiderParams<InputT, K, V, ScoreT, W> params) {
      this.params = params;
    }

    @Override
    public Dataset<Triple<K, V, ScoreT>> output(OutputHint... outputHints) {
      Flow flow = params.input.getFlow();
      TopPerKey<InputT, K, V, ScoreT, W> top =
          new TopPerKey<>(
              flow, params.name, params.input, params.keyFn, params.valueFn, params.scoreFn,
              params.getWindowing(), params.euphoriaWindowing, Sets.newHashSet(outputHints));
      flow.add(top);
      return top.output();
    }
  }
}
