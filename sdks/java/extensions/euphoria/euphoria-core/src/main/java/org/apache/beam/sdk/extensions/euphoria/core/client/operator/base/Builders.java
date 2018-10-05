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
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;

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
   * Dataset}.
   */
  public interface Of {

    /**
     * Specifies the input dataset of the operator.
     *
     * @param <InputT> the type of elements in the input dataset
     * @param input the input dataset to recuce
     * @return the next builder to complete the setup of the operator
     */
    <InputT> Object of(Dataset<InputT> input);
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
   * AccumulatorMode} builders.
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
     * @return the next builder to complete the setup of the {@link ReduceByKey} operator
     */
    <W extends BoundedWindow> OutTriggerBuilderT windowBy(WindowFn<Object, W> windowing);

    /**
     * Specifies the windowing strategy to be applied to the input dataset. Unless the operator is
     * already preceded by an event time assignment, it will process the input elements in ingestion
     * time.
     *
     * @param <W> the type of the windowing
     * @param windowing the windowing strategy to apply to the input dataset
     * @return the next builder to complete the setup of the {@link ReduceByKey} operator
     * @deprecated This method is deprecated and will be removed in future. It is left here for
     *     backward compatibility.
     */
    @Deprecated
    <W extends Window<W>> Object windowBy(Windowing<?, W> windowing);
  }

  /**
   * Second builder in windowing builders chain. It introduces a {@link Trigger}.
   *
   * @param <OutAccBuilderT> following {@link AccumulatorMode} builder type
   */
  public interface TriggeredBy<OutAccBuilderT extends AccumulatorMode> {

    OutAccBuilderT triggeredBy(Trigger trigger);
  }

  /**
   * Third and last builder in windowing chain introducing {@link WindowingStrategy.AccumulationMode
   * accumulation mode}.
   *
   * @param <OutBuilderT> output builder type
   */
  public interface AccumulatorMode<OutBuilderT> {

    OutBuilderT accumulationMode(WindowingStrategy.AccumulationMode accumulationMode);

    default OutBuilderT discardingFiredPanes() {
      return accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES);
    }

    default OutBuilderT accumulatingFiredPanes() {
      return accumulationMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    }
  }

  /** TODO: complete javadoc. */
  public interface Output<T> {

    /**
     * Finalizes the operator and retrieves its output dataset.
     *
     * @param outputHints output dataset description
     * @return the dataset representing the new operator's output
     */
    Dataset<T> output(OutputHint... outputHints);
  }

  /** TODO: complete javadoc. */
  public interface OutputValues<K, V> extends Output<Pair<K, V>> {

    /**
     * Finalizes the operator and retrieves its output dataset. Using this output new operator
     * {@link MapElements} is added to the flow to extract values from pairs.
     *
     * @param outputHints output dataset description
     * @return the dataset representing the new operator's output
     */
    default Dataset<V> outputValues(OutputHint... outputHints) {
      return MapElements.named("extract-values")
          .of(output())
          .using(Pair::getSecond)
          .output(outputHints);
    }
  }
}
