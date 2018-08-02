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
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Basic;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ExtractEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ElementWiseOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A transformation of a dataset from one type into another allowing user code to generate zero,
 * one, or many output elements for a given input element.
 *
 * <p>The user supplied map function is supposed to be stateless. It is fed items from the input in
 * no specified order and the results of the map function are "flattened" to the output (equally in
 * no specified order.)
 *
 * <p>Example:
 *
 * <pre>{@code
 * Dataset<String> strings = ...;
 * Dataset<Integer> ints =
 *        FlatMap.named("TO-INT")
 *           .of(strings)
 *           .using((String s, Context<String> c) -> {
 *             try {
 *               int i = Integer.parseInt(s);
 *               c.collect(i);
 *             } catch (NumberFormatException e) {
 *               // ~ ignore the input if we failed to parse it
 *             }
 *           })
 *           .output();
 * }</pre>
 *
 * <p>The above example tries to parse incoming strings as integers, silently skipping those which
 * cannot be successfully converted. While {@link Collector#collect(Object)} has been used only once
 * here, a {@link FlatMap} operator is free to invoke it multiple times or not at all to generate
 * that many elements to the output dataset.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code using ....................} apply {@link UnaryFunctor} to input elements
 *   <li>{@code [eventTimeBy] ............} change event time characteristic of output elements
 *       using {@link ExtractEventTime}
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Basic(state = StateComplexity.ZERO, repartitions = 0)
public class FlatMap<InputT, OutputT> extends ElementWiseOperator<InputT, OutputT> {

  private final UnaryFunctor<InputT, OutputT> functor;
  private final ExtractEventTime<InputT> eventTimeFn;

  FlatMap(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunctor<InputT, OutputT> functor,
      @Nullable ExtractEventTime<InputT> evtTimeFn,
      Set<OutputHint> outputHints,
      TypeDescriptor<OutputT> outputTypeDescriptor) {
    super(name, flow, input, outputHints, outputTypeDescriptor);
    this.functor = functor;
    this.eventTimeFn = evtTimeFn;
  }

  FlatMap(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunctor<InputT, OutputT> functor,
      @Nullable ExtractEventTime<InputT> evtTimeFn,
      TypeDescriptor<OutputT> outputTypeDescriptor) {
    this(name, flow, input, functor, evtTimeFn, Collections.emptySet(), outputTypeDescriptor);
  }

  /**
   * Starts building a nameless {@link FlatMap} operator to transform the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be transformed
   * @return a builder to complete the setup of the new {@link FlatMap} operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> UsingBuilder<InputT> of(Dataset<InputT> input) {
    return new Builder<>("FlatMap", input);
  }

  /**
   * Starts building a named {@link FlatMap} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new {@link FlatMap} operator
   */
  public static OfBuilder named(String name) {
    return new Builder<>(name);
  }

  /**
   * Retrieves the user defined map function to be applied to this operator's input elements.
   *
   * @return the user defined map function; never {@code null}
   */
  public UnaryFunctor<InputT, OutputT> getFunctor() {
    return functor;
  }

  /**
   * Retrieves the optional user defined event time assigner.
   *
   * @return the user defined event time assigner or {@code null} if none is specified
   */
  @Nullable
  public ExtractEventTime<InputT> getEventTimeExtractor() {
    return eventTimeFn;
  }

  // ------------- Builders chain

  /** Builder exposing {@link #of(Dataset)} method. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> UsingBuilder<InputT> of(Dataset<InputT> input);
  }

  /**
   * A builder which allows user to determine {@link FlatMap FlatMap's} {@link UnaryFunctor
   * functor}.
   *
   * @param <InputT> Input elements type parameter.
   */
  public interface UsingBuilder<InputT> {

    /**
     * Specifies the user defined map function by which to transform the final operator's input
     * dataset.
     *
     * @param <OutputT> the type of elements the user defined map function will produce to the
     *     output dataset
     * @param functor the user defined map function
     * @return the next builder to complete the setup of the {@link FlatMap} operator
     */
    <OutputT> EventTimeBuilder<InputT, OutputT> using(UnaryFunctor<InputT, OutputT> functor);

    <OutputT> EventTimeBuilder<InputT, OutputT> using(
        UnaryFunctor<InputT, OutputT> functor, TypeDescriptor<OutputT> outputTypeDescriptor);
  }

  /**
   * Builder allowing user to specify how event time is associated with input elements.
   *
   * @param <InputT>
   * @param <OutputT>
   */
  public interface EventTimeBuilder<InputT, OutputT> extends Builders.Output<OutputT> {

    /**
     * Specifies a function to derive the input element's event time. Processing of the input stream
     * continues then to proceed with this event time.
     *
     * @param eventTimeFn the event time extraction function
     * @return the next builder to complete the setup of the {@link FlatMap} operator
     */
    OutputBuilder<OutputT> eventTimeBy(ExtractEventTime<InputT> eventTimeFn);
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public interface OutputBuilder<OutputT> extends Builders.Output<OutputT> {

    @Override
    Dataset<OutputT> output(OutputHint... outputHints);
  }

  /** Builder of {@link FlatMap}. */
  public static class Builder<InputT, OutputT>
      implements OfBuilder,
          UsingBuilder<InputT>,
          EventTimeBuilder<InputT, OutputT>,
          OutputBuilder<OutputT> {

    private final String name;
    private Dataset<InputT> input;
    private UnaryFunctor<InputT, OutputT> functor;
    @Nullable private TypeDescriptor<OutputT> outputType;
    @Nullable private ExtractEventTime<InputT> evtTimeFn;

    Builder(String name) {
      this.name = Objects.requireNonNull(name);
    }

    public Builder(String name, Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }

    @Override
    public <InputLocalT> UsingBuilder<InputLocalT> of(Dataset<InputLocalT> input) {
      Objects.requireNonNull(input);

      @SuppressWarnings("unchecked")
      Builder<InputLocalT, ?> casted = (Builder<InputLocalT, ?>) this;
      casted.input = input;
      return casted;
    }

    @Override
    public <OutputLocalT> EventTimeBuilder<InputT, OutputLocalT> using(
        UnaryFunctor<InputT, OutputLocalT> functor) {
      Objects.requireNonNull(functor);

      @SuppressWarnings("unchecked")
      Builder<InputT, OutputLocalT> casted = (Builder<InputT, OutputLocalT>) this;
      casted.functor = functor;
      return casted;
    }

    @Override
    public <OutputLocalT> EventTimeBuilder<InputT, OutputLocalT> using(
        UnaryFunctor<InputT, OutputLocalT> functor, TypeDescriptor<OutputLocalT> outputType) {
      Objects.requireNonNull(functor);
      Objects.requireNonNull(outputType);

      @SuppressWarnings("unchecked")
      Builder<InputT, OutputLocalT> casted = (Builder<InputT, OutputLocalT>) this;
      casted.functor = functor;
      casted.outputType = outputType;
      return casted;
    }

    @Override
    public OutputBuilder<OutputT> eventTimeBy(ExtractEventTime<InputT> eventTimeFn) {
      this.evtTimeFn = Objects.requireNonNull(eventTimeFn);
      return this;
    }

    @Override
    public Dataset<OutputT> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      FlatMap<InputT, OutputT> map =
          new FlatMap<>(
              name, flow, input, functor, evtTimeFn, Sets.newHashSet(outputHints), outputType);
      flow.add(map);
      return map.output();
    }
  }
}
