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

import com.google.common.collect.Sets;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Operator counting elements with same key.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 * <li>{@code [named] ..................} give name to the operator [optional]
 * <li>{@code of .......................} input dataset
 * <li>{@code keyBy ....................} key extractor function
 * <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 * attached windowing
 * <li>{@code output ...................} build output dataset
 * </ol>
 */ //TODO update JavaDoc
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT, repartitions = 1)
public class CountByKey<InputT, K, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
    InputT, InputT, InputT, K, Pair<K, Long>, W, CountByKey<InputT, K, W>> {

  CountByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> extractor,
      @Nullable WindowingDesc<Object, W> windowing,
      Set<OutputHint> outputHints) {

    super(name, flow, input, extractor, windowing, outputHints);
  }

  /**
   * Starts building a nameless {@link CountByKey} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
    return new KeyByBuilder<>("CountByKey", Objects.requireNonNull(input));
  }

  /**
   * Starts building a named {@link CountByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    SumByKey<InputT, K, ?> sum =
        new SumByKey<>(
            getName(), input.getFlow(), input, keyExtractor, e -> 1L, windowing, getHints());
    return DAG.of(sum);
  }

  private static class BuilderParams<InputT, K, W extends BoundedWindow> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, K> keyExtractor;
    WindowFn<Object, W> windowFn;
    Trigger trigger;
    WindowingStrategy.AccumulationMode accumulationMode;

    public BuilderParams(String name,
        Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }

    @Nullable
    private WindowingDesc<Object, W> getWindowing() {
      if (windowFn == null || trigger == null || accumulationMode == null) {
        return null;
      }

      return new WindowingDesc<>(windowFn, trigger, accumulationMode);
    }
  }

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
      return new KeyByBuilder<>(Objects.requireNonNull(name), Objects.requireNonNull(input));
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {

    private final BuilderParams<InputT, ?, ?> params;

    KeyByBuilder(String name, Dataset<InputT> input) {
      this.params = new BuilderParams<>(name, input);
    }

    @Override
    public <K> WindowingBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyExtractor) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, K, ?> paramsCasted =
          (BuilderParams<InputT, K, ?>) params;
      paramsCasted.keyExtractor = Objects.requireNonNull(keyExtractor);
      return new WindowingBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class WindowingBuilder<InputT, K>
      implements Builders.WindowBy<TriggerByBuilder<InputT, K, ?>>,
      Builders.Output<Pair<K, Long>> {

    private final BuilderParams<InputT, K, ?> params;

    WindowingBuilder(BuilderParams<InputT, K, ?> params) {
      this.params = params;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, K, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, K, W> paramsCasted =
          (BuilderParams<InputT, K, W>) params;

      paramsCasted.windowFn = Objects.requireNonNull(windowing);
      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(params).output(outputHints);
    }
  }

  public static class TriggerByBuilder<InputT, K, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, K, W>> {

    private final BuilderParams<InputT, K, W> params;

    TriggerByBuilder(BuilderParams<InputT, K, W> params) {
      this.params = params;
    }

    public AccumulatorModeBuilder<InputT, K, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }

  }

  public static class AccumulatorModeBuilder<InputT, K, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, K, W>> {

    private final BuilderParams<InputT, K, W> params;

    AccumulatorModeBuilder(BuilderParams<InputT, K, W> params) {
      this.params = params;
    }

    public OutputBuilder<InputT, K, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {

      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }

  }

  /**
   * TODO: complete javadoc.
   */
  public static class OutputBuilder<InputT, K, W extends BoundedWindow>
      implements Builders.Output<Pair<K, Long>> {

    private final BuilderParams<InputT, K, W> params;

    OutputBuilder(BuilderParams<InputT, K, W> params) {
      this.params = params;
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {
      Flow flow = params.input.getFlow();
      CountByKey<InputT, K, W> count =
          new CountByKey<>(
              params.name, flow, params.input, params.keyExtractor,
              params.getWindowing(), Sets.newHashSet(outputHints));
      flow.add(count);
      return count.output();
    }
  }
}
