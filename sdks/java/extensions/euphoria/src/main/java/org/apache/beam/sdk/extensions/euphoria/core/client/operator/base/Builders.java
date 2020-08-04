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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator.base;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

/**
 * Common methods used in operator builders.
 *
 * <p>They serve several purposes:
 *
 * <ul>
 *   <li>Defines united API among all the {@link Operator Operators}.
 *   <li>Enables to share related javadoc.
 *   <li>Allows for mandatory chaining of some builders. See {@link WindowBy}.
 * </ul>
 *
 * <p>For internal usage only.
 */
@Audience(Audience.Type.INTERNAL)
public class Builders {

  /**
   * Usually the first builder in a chain. It defines an {@link Operator Operator's} input {@link
   * PCollection}.
   */
  public interface Of {

    /**
     * Specifies the input dataset of the operator.
     *
     * @param <InputT> the type of elements in the input dataset
     * @param input the input dataset to recuce
     * @return the next builder to complete the setup of the operator
     */
    <InputT> Object of(PCollection<InputT> input);
  }

  /**
   * Builder which adds a key extractor to the {@link Operator} in focus.
   *
   * @param <InputT> type of input elements
   */
  public interface KeyBy<InputT> {

    /**
     * Specifies the function to derive the keys from the operator's input elements.
     *
     * @param <K> the type of the extracted key
     * @param keyExtractor a user defined function to extract keys from the processed input
     *     dataset's elements
     * @param keyType {@link TypeDescriptor} of key type {@code <K>}
     * @return the next builder to complete the setup of the operator
     */
    <K> Object keyBy(UnaryFunction<InputT, K> keyExtractor, TypeDescriptor<K> keyType);

    default <K> Object keyBy(UnaryFunction<InputT, K> keyExtractor) {
      return keyBy(keyExtractor, null);
    }
  }

  /**
   * First windowing builder which starts builders chain defining Beam windowing.
   *
   * <p>It consumes {@link WindowFn} and it is followed by {@link TriggeredBy} and {@link
   * AccumulationMode} builders.
   *
   * @param <OutTriggerBuilderT> type of following {@link TriggeredBy} builder.
   */
  public interface WindowBy<OutTriggerBuilderT extends TriggeredBy> {

    /**
     * Specifies the windowing strategy to be applied to the input dataset. Unless the operator is
     * already preceded by an event time assignment, it will process the input elements in ingestion
     * time.
     *
     * @param <W> the type of the windowing, subclass of {@link BoundedWindow}
     * @param windowing {@link BoundedWindow} subclass used to represent the windows used by given
     *     {@link WindowFn}.It represents windowing strategy to apply to the input elements.
     * @return the next builder to complete the setup of the operator
     */
    <W extends BoundedWindow> OutTriggerBuilderT windowBy(WindowFn<Object, W> windowing);
  }

  /**
   * Second builder in windowing builders chain. It introduces a {@link Trigger}.
   *
   * @param <AccumulationModeBuilderT> following {@link AccumulationMode} builder type
   */
  public interface TriggeredBy<AccumulationModeBuilderT extends AccumulationMode> {

    AccumulationModeBuilderT triggeredBy(Trigger trigger);
  }

  /**
   * Third and last builder in windowing chain introducing {@link WindowingStrategy.AccumulationMode
   * accumulation mode}.
   *
   * @param <WindowedOutputBuilderT> output builder type
   */
  public interface AccumulationMode<WindowedOutputBuilderT> {

    WindowedOutputBuilderT accumulationMode(WindowingStrategy.AccumulationMode accumulationMode);

    default WindowedOutputBuilderT discardingFiredPanes() {
      return accumulationMode(WindowingStrategy.AccumulationMode.DISCARDING_FIRED_PANES);
    }

    default WindowedOutputBuilderT accumulatingFiredPanes() {
      return accumulationMode(WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES);
    }
  }

  /**
   * Builder for window optional parameters.
   *
   * @param <T> output builder type
   */
  public interface WindowedOutput<T extends WindowedOutput<T>> {

    /**
     * {@link Window#withAllowedLateness(Duration)}.
     *
     * @param allowedLateness allowed lateness of elements
     * @return next windowing builder in chain
     */
    T withAllowedLateness(Duration allowedLateness);

    /**
     * {@link Window#withAllowedLateness(Duration, Window.ClosingBehavior)}.
     *
     * @param allowedLateness allowed lateness of elements
     * @param closingBehavior a window closing behavior
     * @return next windowing builder in chain
     */
    T withAllowedLateness(Duration allowedLateness, Window.ClosingBehavior closingBehavior);

    /**
     * {@link Window#withTimestampCombiner(TimestampCombiner)}.
     *
     * @param timestampCombiner timestamp combiner
     * @return next windowing builder in chain
     */
    T withTimestampCombiner(TimestampCombiner timestampCombiner);

    /**
     * {@link Window#withOnTimeBehavior(Window.OnTimeBehavior)}.
     *
     * @param behavior window on time behavior
     * @return next windowing builder in chain
     */
    T withOnTimeBehavior(Window.OnTimeBehavior behavior);
  }

  /** Output builder, usually last building step. */
  public interface Output<T> {

    /**
     * Finalizes the operator and retrieves its output dataset.
     *
     * @param outputHint output dataset description
     * @param outputHints other output dataset descriptions
     * @return the dataset representing the new operator's output
     * @deprecated Use {@link #output()} instead.
     */
    @Deprecated
    default PCollection<T> output(OutputHint outputHint, OutputHint... outputHints) {
      LoggerFactory.getLogger(Output.class)
          .warn(
              "OutputHints are deprecated and will be removed in next release. Use Output#output() instead.");
      return output();
    }

    /**
     * Finalizes the operator and retrieves its output dataset.
     *
     * @return the dataset representing the new operator's output
     */
    PCollection<T> output();
  }

  /** Similar to {@link Output}, but it adds method which extracts values from {@link KV}. */
  public interface OutputValues<K, V> extends Output<KV<K, V>> {

    /**
     * Finalizes the operator and retrieves its output dataset. Using this output new operator
     * {@link MapElements} is added to the flow to extract values from pairs.
     *
     * @param outputHint output dataset description
     * @param outputHints other output dataset descriptions
     * @return the dataset representing the new operator's output
     * @deprecated Use {@link #output()} instead.
     */
    @Deprecated
    default PCollection<V> outputValues(OutputHint outputHint, OutputHint... outputHints) {
      LoggerFactory.getLogger(OutputValues.class)
          .warn(
              "OutputHints are deprecated and will be removed in next release. Use OutputValues#outputValues() instead.");
      return outputValues();
    }

    /**
     * Finalizes the operator and retrieves its output dataset. Using this output new operator
     * {@link MapElements} is added to the flow to extract values from pairs.
     *
     * @return the dataset representing the new operator's output
     */
    PCollection<V> outputValues();
  }
}
