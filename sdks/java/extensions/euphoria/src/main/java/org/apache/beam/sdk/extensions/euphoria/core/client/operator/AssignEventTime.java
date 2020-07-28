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
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ExtractEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A convenient alias for assignment of event time.
 *
 * <p>Can be rewritten as:
 *
 * <pre>{@code
 * PCollection<T> input = ...;
 * PCollection<T> withStamps = FlatMap.of(input)
 *    .using(t -> t)
 *    .eventTimeBy(evt-time-fn)
 *    .output();
 * }</pre>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.ZERO, repartitions = 0)
public class AssignEventTime<InputT> extends Operator<InputT>
    implements CompositeOperator<InputT, InputT> {

  /**
   * Starts building a named {@link AssignEventTime} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new {@link AssignEventTime} operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder(name);
  }

  /**
   * Starts building a nameless {@link AssignEventTime} operator to (re-)assign event time the given
   * input dataset's elements.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new {@link AssignEventTime} operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> UsingBuilder<InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /** Builder for the 'of' step from the builder chain. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> UsingBuilder<InputT> of(PCollection<InputT> input);
  }

  /** Builder for the 'using' step from the builder chain. */
  public interface UsingBuilder<InputT> {

    /**
     * @param fn the event time extraction function
     * @return the next builder to complete the setup
     * @see FlatMap.EventTimeBuilder#eventTimeBy(ExtractEventTime)
     */
    Builders.Output<InputT> using(ExtractEventTime<InputT> fn);

    /**
     * @param fn the event time extraction function
     * @param allowedTimestampSkew allowed timestamp skew when assigning timestamps back in time
     *     {@link DoFn#getAllowedTimestampSkew}.
     * @return the next builder to complete the setup
     * @see FlatMap.EventTimeBuilder#eventTimeBy(ExtractEventTime)
     */
    Builders.Output<InputT> using(ExtractEventTime<InputT> fn, Duration allowedTimestampSkew);
  }

  /** Last builder in a chain. It concludes this operators creation by calling {@link #output()}. */
  public static class Builder<InputT>
      implements OfBuilder, UsingBuilder<InputT>, Builders.Output<InputT> {

    private final @Nullable String name;
    private PCollection<InputT> input;
    private ExtractEventTime<InputT> eventTimeExtractor;
    private @Nullable Duration allowedTimestampSkew = null;

    private Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> UsingBuilder<T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T> cast = (Builder<T>) this;
      cast.input = input;
      return cast;
    }

    @Override
    public Builders.Output<InputT> using(ExtractEventTime<InputT> eventTimeExtractor) {
      this.eventTimeExtractor = eventTimeExtractor;
      return this;
    }

    @Override
    public Builders.Output<InputT> using(
        ExtractEventTime<InputT> eventTimeExtractor, Duration allowedTimestampSkew) {
      this.allowedTimestampSkew = allowedTimestampSkew;
      return using(eventTimeExtractor);
    }

    @Override
    public PCollection<InputT> output() {
      return OperatorTransform.apply(
          new AssignEventTime<>(
              name, eventTimeExtractor, allowedTimestampSkew, input.getTypeDescriptor()),
          PCollectionList.of(input));
    }
  }

  private final ExtractEventTime<InputT> eventTimeExtractor;
  private final @Nullable Duration allowedTimestampSkew;

  private AssignEventTime(
      @Nullable String name,
      ExtractEventTime<InputT> eventTimeExtractor,
      @Nullable Duration allowedTimestampSkew,
      @Nullable TypeDescriptor<InputT> outputType) {
    super(name, outputType);
    this.eventTimeExtractor = eventTimeExtractor;
    this.allowedTimestampSkew = allowedTimestampSkew;
  }

  /**
   * @return the user defined event time assigner
   * @see FlatMap#getEventTimeExtractor()
   */
  public ExtractEventTime<InputT> getEventTimeExtractor() {
    return eventTimeExtractor;
  }

  @Override
  public PCollection<InputT> expand(PCollectionList<InputT> inputs) {
    final PCollection<InputT> input = PCollectionLists.getOnlyElement(inputs);
    return FlatMap.named(getName().orElse(null))
        .of(input)
        .using(
            (InputT element, Collector<InputT> coll) -> coll.collect(element),
            input.getTypeDescriptor())
        .eventTimeBy(getEventTimeExtractor(), allowedTimestampSkew)
        .output();
  }
}
