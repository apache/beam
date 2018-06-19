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
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseSingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Emits top element for defined keys and windows. The elements are compared by comparable objects
 * extracted by user defined function applied on input elements.
 *
 * <p>Custom {@link Windowing} can be set.
 *
 * <p>Example:
 *
 * <pre>{@code
 * TopPerKey.of(elements)
 *      .keyBy(e -> (byte) 0)
 *      .valueBy(e -> e)
 *      .scoreBy(KV::getValue)
 *      .output();
 * }</pre>
 *
 * <p>The examples above finds global maximum of all elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code valueBy ..................} value extractor function
 *   <li>{@code scoreBy ..................} {@link UnaryFunction} transforming input elements to
 *       {@link Comparable} scores
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT, repartitions = 1)
public class TopPerKey<InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, K, Triple<K, V, ScoreT>, W, TopPerKey<InputT, K, V, ScoreT, W>>
    implements TypeAware.Value<V> {

  private final UnaryFunction<InputT, V> valueExtractor;
  private final TypeDescriptor<V> valueType;

  private final UnaryFunction<InputT, ScoreT> scoreCalculator;
  private final TypeDescriptor<ScoreT> scoreType;

  TopPerKey(
      Flow flow,
      String name,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      @Nullable TypeDescriptor<K> keyType,
      UnaryFunction<InputT, V> valueExtractor,
      TypeDescriptor<V> valueType,
      @Nullable UnaryFunction<InputT, ScoreT> scoreCalculator,
      TypeDescriptor<ScoreT> scoreType,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      @Nullable TypeDescriptor<Triple<K, V, ScoreT>> outputType,
      Set<OutputHint> outputHints) {
    super(
        name,
        flow,
        input,
        outputType,
        keyExtractor,
        keyType,
        windowing,
        euphoriaWindowing,
        outputHints);

    this.valueExtractor = valueExtractor;
    this.valueType = valueType;
    this.scoreCalculator = scoreCalculator;
    this.scoreType = scoreType;
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
    return valueExtractor;
  }

  public UnaryFunction<InputT, ScoreT> getScoreExtractor() {
    return scoreCalculator;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();

    // Firs we need to remap input elements to elements containing value and score
    // in order to make following ReduceByKey combinable
    MapElements<InputT, Triple<K, V, ScoreT>> inputMapperToScoredKvs =
        new MapElements<>(
            getName() + ":: ExtractKeyValueAndScore",
            flow,
            input,
            (InputT element) ->
                Triple.of(
                    keyExtractor.apply(element),
                    valueExtractor.apply(element),
                    scoreCalculator.apply(element)),
            outputType);

    ReduceByKey<Triple<K, V, ScoreT>, K, Triple<K, V, ScoreT>, Triple<K, V, ScoreT>, W> reduce =
        new ReduceByKey<>(
            getName() + ":: ReduceByKey",
            flow,
            inputMapperToScoredKvs.output(),
            Triple::getFirst,
            keyType,
            UnaryFunction.identity(),
            outputType,
            windowing,
            euphoriaWindowing,
            (CombinableReduceFunction<Triple<K, V, ScoreT>>)
                (triplets) ->
                    triplets
                        .reduce((a, b) -> a.getThird().compareTo(b.getThird()) > 0 ? a : b)
                        .orElseThrow(IllegalStateException::new),
            getHints(),
            TypeUtils.keyValues(keyType, outputType));

    MapElements<KV<K, Triple<K, V, ScoreT>>, Triple<K, V, ScoreT>> format =
        new MapElements<>(
            getName() + "::MapToOutputFormat",
            flow,
            reduce.output(),
            KV::getValue,
            getHints(),
            outputType);

    DAG<Operator<?, ?>> dag = DAG.of(inputMapperToScoredKvs);
    dag.add(reduce, inputMapperToScoredKvs);
    dag.add(format, reduce);

    return dag;
  }

  @Override
  public TypeDescriptor<V> getValueType() {
    return valueType;
  }

  public TypeDescriptor<ScoreT> getScoreType() {
    return scoreType;
  }

  /** Parameters of this operator used in builders. */
  private static final class BuiderParams<
          InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, K> keyFn;
    TypeDescriptor<K> keyType;
    UnaryFunction<InputT, V> valueFn;
    TypeDescriptor<V> valueType;
    UnaryFunction<InputT, ScoreT> scoreFn;
    TypeDescriptor<ScoreT> scoreType;

    BuiderParams(String name, Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }
  }

  /** Star of builders chain. */
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

  /** Key extractor defining builder. */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {

    private final BuiderParams<InputT, ?, ?, ?, ?> params;

    KeyByBuilder(String name, Dataset<InputT> input) {
      params = new BuiderParams<>(name, input);
    }

    @Override
    public <K> ValueByBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyExtractor) {
      return keyBy(keyExtractor, null);
    }

    @Override
    public <K> ValueByBuilder<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, TypeDescriptor<K> keyType) {
      @SuppressWarnings("unchecked")
      BuiderParams<InputT, K, ?, ?, ?> paramsCasted = (BuiderParams<InputT, K, ?, ?, ?>) params;

      paramsCasted.keyFn = requireNonNull(keyExtractor);
      paramsCasted.keyType = keyType;

      return new ValueByBuilder<>(paramsCasted);
    }
  }

  /** Value extractor defining builder. */
  public static class ValueByBuilder<InputT, K> {

    private final BuiderParams<InputT, K, ?, ?, ?> params;

    ValueByBuilder(BuiderParams<InputT, K, ?, ?, ?> params) {
      this.params = params;
    }

    public <V> ScoreByBuilder<InputT, K, V> valueBy(UnaryFunction<InputT, V> valueExtractor) {
      return valueBy(valueExtractor, null);
    }

    public <V> ScoreByBuilder<InputT, K, V> valueBy(
        UnaryFunction<InputT, V> valueExtractor, TypeDescriptor<V> valueType) {
      @SuppressWarnings("unchecked")
      BuiderParams<InputT, K, V, ?, ?> paramsCasted = (BuiderParams<InputT, K, V, ?, ?>) params;

      paramsCasted.valueFn = requireNonNull(valueExtractor);
      paramsCasted.valueType = valueType;
      return new ScoreByBuilder<>(paramsCasted);
    }
  }

  /** Score calculator defining builder. */
  public static class ScoreByBuilder<InputT, K, V> {

    private final BuiderParams<InputT, K, V, ?, ?> params;

    ScoreByBuilder(BuiderParams<InputT, K, V, ?, ?> params) {
      this.params = params;
    }

    public <ScoreT extends Comparable<ScoreT>> WindowByBuilder<InputT, K, V, ScoreT> scoreBy(
        UnaryFunction<InputT, ScoreT> scoreFn) {
      return scoreBy(scoreFn, null);
    }

    public <ScoreT extends Comparable<ScoreT>> WindowByBuilder<InputT, K, V, ScoreT> scoreBy(
        UnaryFunction<InputT, ScoreT> scoreFn, TypeDescriptor<ScoreT> scoreType) {

      @SuppressWarnings("unchecked")
      BuiderParams<InputT, K, V, ScoreT, ?> paramsCasted =
          (BuiderParams<InputT, K, V, ScoreT, ?>) params;

      paramsCasted.scoreFn = requireNonNull(scoreFn);
      paramsCasted.scoreType = scoreType;
      return new WindowByBuilder<>(paramsCasted);
    }
  }

  /** First of windowing defining builders. */
  public static class WindowByBuilder<InputT, K, V, ScoreT extends Comparable<ScoreT>>
      implements Builders.WindowBy<TriggerByBuilder<InputT, K, V, ScoreT, ?>>,
          Builders.Output<Triple<K, V, ScoreT>>,
          OptionalMethodBuilder<
              WindowByBuilder<InputT, K, V, ScoreT>, OutputBuilder<InputT, K, V, ScoreT, ?>> {

    private final BuiderParams<InputT, K, V, ScoreT, ?> params;

    WindowByBuilder(BuiderParams<InputT, K, V, ScoreT, ?> params) {
      this.params = params;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, K, V, ScoreT, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked")
      BuiderParams<InputT, K, V, ScoreT, W> paramsCasted =
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
    public OutputBuilder<InputT, K, V, ScoreT, ?> applyIf(
        boolean cond,
        UnaryFunction<WindowByBuilder<InputT, K, V, ScoreT>, OutputBuilder<InputT, K, V, ScoreT, ?>>
            applyWhenConditionHolds) {
      Objects.requireNonNull(applyWhenConditionHolds);

      if (cond) {
        return applyWhenConditionHolds.apply(this);
      }

      return new OutputBuilder<>(params);
    }
  }

  /** Trigger defining operator builder. */
  public static class TriggerByBuilder<
          InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, K, V, ScoreT, W>> {

    private final BuiderParams<InputT, K, V, ScoreT, W> params;

    TriggerByBuilder(BuiderParams<InputT, K, V, ScoreT, W> params) {
      this.params = params;
    }

    @Override
    public AccumulatorModeBuilder<InputT, K, V, ScoreT, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }
  }

  /** {@link WindowingStrategy.AccumulationMode} defining operator builder. */
  public static class AccumulatorModeBuilder<
          InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, K, V, ScoreT, W>> {

    private final BuiderParams<InputT, K, V, ScoreT, W> params;

    AccumulatorModeBuilder(BuiderParams<InputT, K, V, ScoreT, W> params) {
      this.params = params;
    }

    @Override
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
              flow,
              params.name,
              params.input,
              params.keyFn,
              params.keyType,
              params.valueFn,
              params.valueType,
              params.scoreFn,
              params.scoreType,
              params.getWindowing(),
              params.euphoriaWindowing,
              TypeUtils.triplets(params.keyType, params.valueType, params.scoreType),
              Sets.newHashSet(outputHints));
      flow.add(top);
      return top.output();
    }
  }
}
