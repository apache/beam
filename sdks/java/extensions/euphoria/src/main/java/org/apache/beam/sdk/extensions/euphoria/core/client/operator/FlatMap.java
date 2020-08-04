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

import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Basic;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ExtractEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

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
 * PCollection<String> strings = ...;
 * PCollection<Integer> ints =
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
public class FlatMap<InputT, OutputT> extends Operator<OutputT>
    implements TypeAware.Output<OutputT> {

  /**
   * Starts building a nameless {@link FlatMap} operator to transform the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be transformed
   * @return a builder to complete the setup of the new {@link FlatMap} operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> UsingBuilder<InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link FlatMap} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new {@link FlatMap} operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder<>(name);
  }

  // ------------- Builders chain

  /** Builder exposing {@link #of(PCollection)} method. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> UsingBuilder<InputT> of(PCollection<InputT> input);
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
   * @param <InputT> input elements type
   * @param <OutputT> output elements type
   */
  public interface EventTimeBuilder<InputT, OutputT> extends Builders.Output<OutputT> {

    /**
     * Specifies a function to derive the input element's event time. Processing of the input stream
     * continues then to proceed with this event time.
     *
     * @param eventTimeFn the event time extraction function
     * @return the next builder to complete the setup of the {@link FlatMap} operator
     */
    default Builders.Output<OutputT> eventTimeBy(ExtractEventTime<InputT> eventTimeFn) {
      // allowed timestamp shifts to infitive past
      return eventTimeBy(eventTimeFn, null);
    }

    /**
     * Specifies a function to derive the input element's event time. Processing of the input stream
     * continues then to proceed with this event time.
     *
     * @param eventTimeFn the event time extraction function
     * @param timestampSkew allowed skew in milliseconds of already assigned timestamps and the
     *     newly assigned (see {@link DoFn#getAllowedTimestampSkew}
     * @return the next builder to complete the setup of the {@link FlatMap} operator
     */
    Builders.Output<OutputT> eventTimeBy(
        ExtractEventTime<InputT> eventTimeFn, @Nullable Duration timestampSkew);
  }

  /** Builder of {@link FlatMap}. */
  public static class Builder<InputT, OutputT>
      implements OfBuilder,
          UsingBuilder<InputT>,
          EventTimeBuilder<InputT, OutputT>,
          Builders.Output<OutputT> {

    private final @Nullable String name;
    private PCollection<InputT> input;
    private UnaryFunctor<InputT, OutputT> functor;
    private @Nullable TypeDescriptor<OutputT> outputType;
    private @Nullable ExtractEventTime<InputT> evtTimeFn;
    private Duration allowedTimestampSkew = Duration.millis(Long.MAX_VALUE);

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <InputLocalT> UsingBuilder<InputLocalT> of(PCollection<InputLocalT> input) {
      @SuppressWarnings("unchecked")
      Builder<InputLocalT, ?> cast = (Builder) this;
      cast.input = requireNonNull(input);
      return cast;
    }

    @Override
    public <OutputLocalT> EventTimeBuilder<InputT, OutputLocalT> using(
        UnaryFunctor<InputT, OutputLocalT> functor) {
      return using(functor, null);
    }

    @Override
    public <OutputLocalT> EventTimeBuilder<InputT, OutputLocalT> using(
        UnaryFunctor<InputT, OutputLocalT> functor, TypeDescriptor<OutputLocalT> outputType) {
      @SuppressWarnings("unchecked")
      Builder<InputT, OutputLocalT> cast = (Builder) this;
      cast.functor = requireNonNull(functor);
      cast.outputType = outputType;
      return cast;
    }

    @Override
    public Builders.Output<OutputT> eventTimeBy(
        ExtractEventTime<InputT> eventTimeFn, @Nullable Duration timestampSkew) {
      this.evtTimeFn = requireNonNull(eventTimeFn);
      this.allowedTimestampSkew =
          MoreObjects.firstNonNull(timestampSkew, Duration.millis(Long.MAX_VALUE));
      return this;
    }

    @Override
    public PCollection<OutputT> output() {
      return OperatorTransform.apply(
          new FlatMap<>(name, functor, outputType, evtTimeFn, allowedTimestampSkew),
          PCollectionList.of(input));
    }
  }

  private final UnaryFunctor<InputT, OutputT> functor;
  private final @Nullable ExtractEventTime<InputT> eventTimeFn;
  private final Duration allowedTimestampSkew;

  private FlatMap(
      @Nullable String name,
      UnaryFunctor<InputT, OutputT> functor,
      @Nullable TypeDescriptor<OutputT> outputType,
      @Nullable ExtractEventTime<InputT> evtTimeFn,
      Duration allowedTimestampSkew) {

    super(name, outputType);
    this.functor = functor;
    this.eventTimeFn = evtTimeFn;
    this.allowedTimestampSkew = requireNonNull(allowedTimestampSkew);
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
   * @return the user defined event time assigner if specified
   */
  public Optional<ExtractEventTime<InputT>> getEventTimeExtractor() {
    return Optional.ofNullable(eventTimeFn);
  }

  /**
   * Retrieves maximal allowed timestamp skew.
   *
   * @return the user supplied maximal allowed timestamp skew
   */
  public Duration getAllowedTimestampSkew() {
    return allowedTimestampSkew;
  }
}
