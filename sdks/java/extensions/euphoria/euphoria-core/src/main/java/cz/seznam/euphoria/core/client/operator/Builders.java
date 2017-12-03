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
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.type.TypeAwareUnaryFunction;
import cz.seznam.euphoria.core.client.type.TypeHint;

/**
 * Common methods used in operator builders to share related javadoc
 * descriptions.<p>
 *
 * For internal usage only.
 */
@Audience(Audience.Type.INTERNAL)
public class Builders {

  interface Of {

    /**
     * Specifies the input dataset of the operator.
     *
     * @param <IN> the type of elements in the input dataset
     *
     * @param input the input dataset to recuce
     *
     * @return the next builder to complete the setup of the operator
     */
    <IN> Object of(Dataset<IN> input);
  }

  interface KeyBy<IN> {

    /**
     * Specifies the function to derive the keys from the operator's input
     * elements.
     *
     * @param <KEY> the type of the extracted key
     *
     * @param keyExtractor a user defined function to extract keys from the
     *                      processed input dataset's elements
     *
     * @return the next builder to complete the setup of the operator
     */
    <KEY> Object keyBy(UnaryFunction<IN, KEY> keyExtractor);

    default <KEY> Object keyBy(UnaryFunction<IN, KEY> keyExtractor, TypeHint<KEY> typeHint) {
      return keyBy(TypeAwareUnaryFunction.of(keyExtractor, typeHint));
    }
  }

  /**
   * Interface for builders of windowing.
   * @param <IN> data type of the input elements
   * @param <BUILDER> the builder
   */
  interface WindowBy<IN, BUILDER extends WindowBy<IN, BUILDER>>
      extends OptionalMethodBuilder<BUILDER> {

    /**
     * Specifies the windowing strategy to be applied to the input dataset.
     * Unless the operator is already preceded by an event time assignment,
     * it will process the input elements in ingestion time.
     *
     * @param <W> the type of the windowing
     *
     * @param windowing the windowing strategy to apply to the input dataset
     *
     * @return the next builder to complete the setup of the
     *          {@link ReduceByKey} operator
     */
    <W extends Window<W>> Object windowBy(Windowing<IN, W> windowing);
  }

  public interface Output<T> {

    /**
     * Finalizes the operator and retrieves its output dataset.
     *
     * @param outputHints output dataset description
     * @return the dataset representing the new operator's output
     */
    Dataset<T> output(OutputHint... outputHints);
  }

  public interface OutputValues<K, V> extends Output<Pair<K, V>> {

    /**
     * Finalizes the operator and retrieves its output dataset.
     * Using this output new operator {@link MapElements} is added
     * to the flow to extract values from pairs.
     *
     * @param outputHints output dataset description
     * @return the dataset representing the new operator's output
     */
    default Dataset<V> outputValues(OutputHint... outputHints) {
      return MapElements
          .named("extract-values")
          .of(output())
          .using(Pair::getSecond)
          .output(outputHints);
    }
  }

}
