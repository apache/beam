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
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Operator for summing of long values extracted from elements. The sum is operated upon defined key
 * and window.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Dataset<Pair<String, Long>> summed = SumByKey.of(elements)
 *     .keyBy(Pair::getFirst)
 *     .valueBy(Pair::getSecond)
 *     .output();
 * }</pre>
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code [valueBy] ................} {@link UnaryFunction} transforming from input element to
 *       long (default: {@code e -> 1L})
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 *       attached windowing
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT, repartitions = 1)
public class SumByKey<InputT, K, W extends Window<W>>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, InputT, K, Pair<K, Long>, W, SumByKey<InputT, K, W>> {

  private final UnaryFunction<InputT, Long> valueExtractor;

  SumByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, Long> valueExtractor,
      @Nullable Windowing<InputT, W> windowing) {
    this(name, flow, input, keyExtractor, valueExtractor, windowing, Collections.emptySet());
  }

  SumByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, Long> valueExtractor,
      @Nullable Windowing<InputT, W> windowing,
      Set<OutputHint> outputHints) {
    super(name, flow, input, keyExtractor, windowing, outputHints);
    this.valueExtractor = valueExtractor;
  }

  /**
   * Starts building a nameless {@link SumByKey} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
    return new KeyByBuilder<>("SumByKey", input);
  }

  /**
   * Starts building a named {@link SumByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    ReduceByKey<InputT, K, Long, Long, W> reduceByKey =
        new ReduceByKey<>(
            getName(),
            input.getFlow(),
            input,
            keyExtractor,
            valueExtractor,
            windowing,
            Sums.ofLongs(),
            getHints());
    return DAG.of(reduceByKey);
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder implements Builders.Of {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
      return new KeyByBuilder<>(name, input);
    }
  }

  /** TODO: complete javadoc. */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {
    private final String name;
    private final Dataset<InputT> input;

    KeyByBuilder(String name, Dataset<InputT> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    @Override
    public <K> ValueByBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyExtractor) {
      return new ValueByBuilder<>(name, input, keyExtractor);
    }
  }

  /** TODO: complete javadoc. */
  public static class ValueByBuilder<InputT, K>
      implements Builders.WindowBy<InputT, ByBuilder2<InputT, K>>,
          Builders.Output<Pair<K, Long>>,
          Builders.OutputValues<K, Long> {

    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunction<InputT, K> keyExtractor;

    ValueByBuilder(String name, Dataset<InputT> input, UnaryFunction<InputT, K> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    public ByBuilder2<InputT, K> valueBy(UnaryFunction<InputT, Long> valueExtractor) {
      return new ByBuilder2<>(name, input, keyExtractor, valueExtractor);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, W> windowBy(
        Windowing<InputT, W> windowing) {
      return new OutputBuilder<>(name, input, keyExtractor, e -> 1L, windowing);
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(name, input, keyExtractor, e -> 1L, null).output(outputHints);
    }
  }

  /** TODO: complete javadoc. */
  public static class ByBuilder2<InputT, K>
      implements Builders.WindowBy<InputT, ByBuilder2<InputT, K>>,
          Builders.Output<Pair<K, Long>>,
          Builders.OutputValues<K, Long> {

    final String name;
    final Dataset<InputT> input;
    final UnaryFunction<InputT, K> keyExtractor;
    final UnaryFunction<InputT, Long> valueExtractor;

    ByBuilder2(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, Long> valueExtractor) {

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, W> windowBy(
        Windowing<InputT, W> windowing) {
      return new OutputBuilder<>(name, input, keyExtractor, valueExtractor, windowing);
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(name, input, keyExtractor, valueExtractor, null).output();
    }
  }

  /** TODO: complete javadoc. */
  public static class OutputBuilder<InputT, K, W extends Window<W>> extends ByBuilder2<InputT, K> {

    @Nullable private final Windowing<InputT, W> windowing;

    OutputBuilder(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, Long> valueExtractor,
        @Nullable Windowing<InputT, W> windowing) {

      super(name, input, keyExtractor, valueExtractor);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      SumByKey<InputT, K, W> sumByKey =
          new SumByKey<>(
              name,
              flow,
              input,
              keyExtractor,
              valueExtractor,
              windowing,
              Sets.newHashSet(outputHints));
      flow.add(sumByKey);
      return sumByKey.output();
    }
  }
}
