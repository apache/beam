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

import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctionEnv;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.translate.Translation;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Simple one-to-one transformation of input elements. It is a special case of {@link FlatMap} with
 * exactly one output element for every one input element. No context is provided inside the map
 * function.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code using ....................} apply {@link UnaryFunction} or {@link UnaryFunctionEnv}
 *       to input elements
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.ZERO, repartitions = 0)
public class MapElements<InputT, OutputT> extends Operator<OutputT>
    implements CompositeOperator<InputT, OutputT>, TypeAware.Output<OutputT> {

  private final UnaryFunctionEnv<InputT, OutputT> mapper;

  private MapElements(
      String name,
      UnaryFunctionEnv<InputT, OutputT> mapper,
      @Nullable TypeDescriptor<OutputT> outputType) {
    super(name, outputType);
    this.mapper = mapper;
  }

  /**
   * Starts building a nameless {@link MapElements} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> UsingBuilder<InputT> of(Dataset<InputT> input) {
    return new UsingBuilder<>("MapElements", input);
  }

  /**
   * Starts building a named {@link MapElements} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder implements Builders.Of {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> UsingBuilder<InputT> of(Dataset<InputT> input) {
      return new UsingBuilder<>(name, input);
    }
  }

  /** TODO: complete javadoc. */
  public static class UsingBuilder<InputT> {

    private final String name;
    private final Dataset<InputT> input;

    UsingBuilder(String name, Dataset<InputT> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    /**
     * The mapping function that takes input element and outputs the OutputT type element. If you
     * want use aggregators use rather {@link #using(UnaryFunctionEnv)}.
     *
     * @param <OutputT> type of output elements
     * @param mapper the mapping function
     * @return the next builder to complete the setup of the {@link MapElements} operator
     */
    public <OutputT> OutputBuilder<InputT, OutputT> using(UnaryFunction<InputT, OutputT> mapper) {
      return new OutputBuilder<>(name, input, ((el, ctx) -> mapper.apply(el)));
    }

    public <OutputT> OutputBuilder<InputT, OutputT> using(
        UnaryFunction<InputT, OutputT> mapper, TypeDescriptor<OutputT> outputType) {
      return new OutputBuilder<>(name, input, (el, ctx) -> mapper.apply(el), outputType);
    }

    /**
     * The mapping function that takes input element and outputs the OutputT type element.
     *
     * @param <OutputT> type of output elements
     * @param mapper the mapping function
     * @return the next builder to complete the setup of the {@link MapElements} operator
     */
    public <OutputT> OutputBuilder<InputT, OutputT> using(
        UnaryFunctionEnv<InputT, OutputT> mapper) {
      return using(mapper, null);
    }

    public <OutputT> OutputBuilder<InputT, OutputT> using(
        UnaryFunctionEnv<InputT, OutputT> mapper, TypeDescriptor<OutputT> outputType) {
      return new OutputBuilder<>(name, input, mapper, outputType);
    }
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<InputT, OutputT> implements Builders.Output<OutputT> {

    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunctionEnv<InputT, OutputT> mapper;
    @Nullable private final TypeDescriptor<OutputT> outputType;

    OutputBuilder(
        String name,
        Dataset<InputT> input,
        UnaryFunctionEnv<InputT, OutputT> mapper,
        @Nullable TypeDescriptor<OutputT> outputType) {
      this.name = name;
      this.input = input;
      this.mapper = mapper;
      this.outputType = outputType;
    }

    OutputBuilder(String name, Dataset<InputT> input, UnaryFunctionEnv<InputT, OutputT> mapper) {
      this.name = name;
      this.input = input;
      this.mapper = mapper;
      this.outputType = null;
    }

    @Override
    public Dataset<OutputT> output(OutputHint... outputHints) {
      final MapElements<InputT, OutputT> operator = new MapElements<>(name, mapper, outputType);
      return Translation.apply(operator, Collections.singletonList(input));
    }
  }

  @Override
  public Dataset<OutputT> expand(List<Dataset<InputT>> inputs) {
    return FlatMap.named(getName())
        .of(Iterables.getOnlyElement(inputs))
        .using(
            (InputT elem, Collector<OutputT> coll) ->
                coll.collect(getMapper().apply(elem, coll.asContext())),
            getOutputType().orElse(null))
        .output();
  }

  public UnaryFunctionEnv<InputT, OutputT> getMapper() {
    return mapper;
  }
}
