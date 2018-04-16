/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * Operator counting elements with same key.
 *
 * <h3>Builders:</h3>
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default attached windowing
 *   <li>{@code output ...................} build output dataset
 * </ol>
 *
 */
@Audience(Audience.Type.CLIENT)
@Derived(
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class CountByKey<IN, KEY, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
    IN, IN, IN, KEY, Pair<KEY, Long>, W, CountByKey<IN, KEY, W>> {

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
    public <KEY> WindowingBuilder<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new WindowingBuilder<>(name, input, keyExtractor);
    }
  }

  public static class WindowingBuilder<IN, KEY>
      implements Builders.WindowBy<IN, WindowingBuilder<IN, KEY>>,
      Builders.Output<Pair<KEY, Long>> {

    final String name;
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;

    WindowingBuilder(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    @Override
    public <W extends Window> OutputBuilder<IN, KEY, W>
    windowBy(Windowing<IN, W> windowing) {
      return new OutputBuilder<>(name, input, keyExtractor, windowing);
    }

    @Override
    public Dataset<Pair<KEY, Long>> output(OutputHint... outputHints) {
      return windowBy(null).output(outputHints);
    }
  }

  public static class OutputBuilder<IN, KEY, W extends Window>
      extends WindowingBuilder<IN, KEY>
      implements Builders.Output<Pair<KEY, Long>> {

    @Nullable
    private final Windowing<IN, W> windowing;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, KEY> keyExtractor,
                  @Nullable Windowing<IN, W> windowing) {

      super(name, input, keyExtractor);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<KEY, Long>> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      CountByKey<IN, KEY, W> count = new CountByKey<>(
          name, flow, input, keyExtractor, windowing, Sets.newHashSet(outputHints));
      flow.add(count);
      return count.output();
    }
  }

  /**
   * Starts building a nameless {@link CountByKey} operator to process
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
    return new KeyByBuilder<>("CountByKey", input);
  }

  /**
   * Starts building a named {@link CountByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  CountByKey(String name,
             Flow flow,
             Dataset<IN> input,
             UnaryFunction<IN, KEY> extractor,
             @Nullable Windowing<IN, W> windowing,
             Set<OutputHint> outputHints) {

    super(name, flow, input, extractor, windowing, outputHints);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    SumByKey<IN, KEY, W> sum = new SumByKey<>(
        getName(),
        input.getFlow(),
        input,
        keyExtractor,
        e -> 1L,
        windowing,
        getHints());
    return DAG.of(sum);
  }
}
