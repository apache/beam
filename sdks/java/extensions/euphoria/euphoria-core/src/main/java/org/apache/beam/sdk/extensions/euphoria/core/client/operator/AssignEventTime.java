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
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ExtractEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ElementWiseOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A convenient alias for assignment of event time.
 *
 * <p>Can be rewritten as:
 *
 * <pre>{@code
 * Dataset<T> input = ...;
 * Dataset<T> withStamps = FlatMap.of(input)
 *    .using(t -> t)
 *    .eventTimeBy(evt-time-fn)
 *    .output();
 * }</pre>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.ZERO, repartitions = 0)
public class AssignEventTime<InputT> extends ElementWiseOperator<InputT, InputT> {

  private final ExtractEventTime<InputT> eventTimeFn;

  AssignEventTime(
      String name,
      Flow flow,
      Dataset<InputT> input,
      ExtractEventTime<InputT> eventTimeFn,
      Set<OutputHint> outputHints,
      TypeDescriptor<InputT> outputType) {
    super(name, flow, input, outputHints, outputType);
    this.eventTimeFn = eventTimeFn;
  }

  /**
   * Starts building a named {@link AssignEventTime} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new {@link AssignEventTime} operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(Objects.requireNonNull(name));
  }

  /**
   * Starts building a nameless {@link AssignEventTime} operator to (re-)assign event time the given
   * input dataset's elements.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new {@link AssignEventTime} operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> UsingBuilder<InputT> of(Dataset<InputT> input) {
    return new UsingBuilder<>("AssignEventTime", input);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(
        new FlatMap<>(
            getName(),
            getFlow(),
            input,
            (i, c) -> c.collect(i),
            eventTimeFn,
            getHints(),
            outputType));
  }

  /**
   * @return the user defined event time assigner
   * @see FlatMap#getEventTimeExtractor()
   */
  public ExtractEventTime<InputT> getEventTimeExtractor() {
    return eventTimeFn;
  }

  /** AssignEventTime builder which adds input {@link Dataset} to operator under build. */
  public static class OfBuilder implements Builders.Of {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> UsingBuilder<InputT> of(Dataset<InputT> input) {
      return new UsingBuilder<>(name, Objects.requireNonNull(input));
    }
  }

  /** AssignEventTime builder which adds event time extractor to operator under build. */
  public static class UsingBuilder<InputT> {

    private final String name;
    private final Dataset<InputT> input;

    UsingBuilder(String name, Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }

    /**
     * @param fn the event time extraction function
     * @return the next builder to complete the setup
     * @see FlatMap.EventTimeBuilder#eventTimeBy(ExtractEventTime)
     */
    public OutputBuilder<InputT> using(ExtractEventTime<InputT> fn) {
      return new OutputBuilder<>(name, input, Objects.requireNonNull(fn));
    }
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<InputT> implements Builders.Output<InputT> {

    private final String name;
    private final Dataset<InputT> input;
    private final ExtractEventTime<InputT> eventTimeFn;

    OutputBuilder(String name, Dataset<InputT> input, ExtractEventTime<InputT> eventTimeFn) {
      this.name = name;
      this.input = input;
      this.eventTimeFn = eventTimeFn;
    }

    @Override
    public Dataset<InputT> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();

      AssignEventTime<InputT> op =
          new AssignEventTime<>(
              name,
              flow,
              input,
              eventTimeFn,
              Sets.newHashSet(outputHints),
              TypeUtils.getDatasetElementType(input));

      flow.add(op);
      return op.output();
    }
  }
}
