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

import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * Expand operator to a beam {@link PTransform}.
 *
 * @param <InputT> type of input elements
 * @param <OutputT> type of output elements
 * @param <OperatorT> type of operator to expand
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class OperatorTransform<InputT, OutputT, OperatorT extends Operator<OutputT>>
    extends PTransform<PCollectionList<InputT>, PCollection<OutputT>> {

  public static <InputT, OutputT, OperatorT extends Operator<OutputT>> PCollection<OutputT> apply(
      OperatorT operator, PCollectionList<InputT> inputs) {

    final Optional<OperatorTranslator<InputT, OutputT, OperatorT>> maybeTranslator =
        TranslatorProvider.of(inputs.getPipeline()).findTranslator(operator);

    if (maybeTranslator.isPresent()) {
      final PCollection<OutputT> output =
          inputs.apply(
              operator.getName().orElseGet(() -> operator.getClass().getName()),
              new OperatorTransform<>(operator, maybeTranslator.orElse(null)));
      Preconditions.checkState(
          output.getTypeDescriptor() != null,
          "Translator should always return a typed PCollection.");
      return output;
    }

    throw new IllegalStateException(
        "Unable to find translator for basic operator ["
            + operator.getClass()
            + "] with name ["
            + operator.getName().orElse(null)
            + "]");
  }

  private final OperatorT operator;
  private final OperatorTranslator<InputT, OutputT, OperatorT> translator;

  private OperatorTransform(
      OperatorT operator, OperatorTranslator<InputT, OutputT, OperatorT> translator) {
    this.operator = operator;
    this.translator = translator;
  }

  @Override
  public PCollection<OutputT> expand(PCollectionList<InputT> inputs) {
    return translator.translate(operator, inputs);
  }

  public OperatorT getOperator() {
    return operator;
  }
}
