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
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctionEnv;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

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
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.ZERO, repartitions = 0)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class MapElements<InputT, OutputT> extends Operator<OutputT>
    implements CompositeOperator<InputT, OutputT>, TypeAware.Output<OutputT> {

  /**
   * Starts building a nameless {@link MapElements} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> UsingBuilder<InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link MapElements} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder<>(name);
  }

  /** Builder for the 'of' step. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> UsingBuilder<InputT> of(PCollection<InputT> input);
  }

  /** MapElements builder which adds mapper to operator under build. */
  public interface UsingBuilder<InputT> {

    /**
     * The mapping function that takes input element and outputs the OutputT type element. If you
     * want use aggregators use rather {@link #using(UnaryFunctionEnv)}.
     *
     * @param <OutputT> type of output elements
     * @param mapper the mapping function
     * @return the next builder to complete the setup of the {@link MapElements} operator
     */
    default <OutputT> Builders.Output<OutputT> using(UnaryFunction<InputT, OutputT> mapper) {
      return using(mapper, null);
    }

    default <OutputT> Builders.Output<OutputT> using(
        UnaryFunction<InputT, OutputT> mapper, @Nullable TypeDescriptor<OutputT> outputType) {
      return using((el, ctx) -> mapper.apply(el), outputType);
    }

    /**
     * The mapping function that takes input element and outputs the OutputT type element.
     *
     * @param <OutputT> type of output elements
     * @param mapper the mapping function
     * @return the next builder to complete the setup of the {@link MapElements} operator
     */
    default <OutputT> Builders.Output<OutputT> using(UnaryFunctionEnv<InputT, OutputT> mapper) {
      return using(mapper, null);
    }

    <OutputT> Builders.Output<OutputT> using(
        UnaryFunctionEnv<InputT, OutputT> mapper, @Nullable TypeDescriptor<OutputT> outputType);
  }

  private static class Builder<InputT, OutputT>
      implements OfBuilder, UsingBuilder<InputT>, Builders.Output<OutputT> {

    private final @Nullable String name;
    private PCollection<InputT> input;
    private UnaryFunctionEnv<InputT, OutputT> mapper;
    private @Nullable TypeDescriptor<OutputT> outputType;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> UsingBuilder<T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, ?> cast = (Builder) this;
      cast.input = input;
      return cast;
    }

    @Override
    public <T> Builders.Output<T> using(
        UnaryFunctionEnv<InputT, T> mapper, @Nullable TypeDescriptor<T> outputType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T> cast = (Builder) this;
      cast.mapper = mapper;
      cast.outputType = outputType;
      return cast;
    }

    @Override
    public PCollection<OutputT> output() {
      final MapElements<InputT, OutputT> operator = new MapElements<>(name, mapper, outputType);
      return OperatorTransform.apply(operator, PCollectionList.of(input));
    }
  }

  private final UnaryFunctionEnv<InputT, OutputT> mapper;

  private MapElements(
      @Nullable String name,
      UnaryFunctionEnv<InputT, OutputT> mapper,
      @Nullable TypeDescriptor<OutputT> outputType) {
    super(name, outputType);
    this.mapper = mapper;
  }

  @Override
  public PCollection<OutputT> expand(PCollectionList<InputT> inputs) {
    return FlatMap.named(getName().orElse(null))
        .of(PCollectionLists.getOnlyElement(inputs))
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
