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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * A functor to translate an operator into a beam execution.
 *
 * @param <InputT> the type of input elements
 * @param <OutputT> the type of output elements
 * @param <OperatorT> the type of the euphoria operator
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
@Deprecated
public interface OperatorTranslator<InputT, OutputT, OperatorT extends Operator> {

  /**
   * Translates the given a operator it into a concrete beam transformation.
   *
   * @param operator the operator to translate
   * @param inputs list of inputs
   * @return a beam transformation
   */
  PCollection<OutputT> translate(OperatorT operator, PCollectionList<InputT> inputs);

  /**
   * Returns true when implementing {@link OperatorTranslator} is able to translate given instance
   * of an operator, false otherwise.
   *
   * <p>This method allow us to have more {@link OperatorTranslator} implementations for one {@link
   * Operator} in case when some specialized translators are needed.
   *
   * @param operator operator to check
   * @return true if operator can be translated
   */
  default boolean canTranslate(OperatorT operator) {
    return true;
  }
}
