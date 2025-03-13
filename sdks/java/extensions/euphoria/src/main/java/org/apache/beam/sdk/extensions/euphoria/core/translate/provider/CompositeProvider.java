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
package org.apache.beam.sdk.extensions.euphoria.core.translate.provider;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.TranslatorProvider;

/**
 * An implementation of {@link TranslatorProvider} which allows to stack other {@link
 * TranslatorProvider TranslatorProviders} in order given on construction time.
 */
/** @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release. */
@Deprecated
public class CompositeProvider implements TranslatorProvider {

  private final List<TranslatorProvider> orderedTranslatorsChain;

  public static CompositeProvider of(List<TranslatorProvider> orderedTranslatorsChain) {
    return new CompositeProvider(orderedTranslatorsChain);
  }

  public static CompositeProvider of(TranslatorProvider... orderedTranslatorsChain) {
    requireNonNull(orderedTranslatorsChain);
    return new CompositeProvider(Arrays.asList(orderedTranslatorsChain));
  }

  private CompositeProvider(List<TranslatorProvider> orderedTranslatorsChain) {
    requireNonNull(orderedTranslatorsChain);
    this.orderedTranslatorsChain = Collections.unmodifiableList(orderedTranslatorsChain);
  }

  /**
   * Returns first {@code Optional<OperatorTranslator<InputT, OutputT, OperatorT>>} which {@link
   * OperatorTranslator#canTranslate(Operator) can translate} given operator.
   *
   * <p>Translators are acquired by calling {@link TranslatorProvider#findTranslator(Operator)} on
   * {@link TranslatorProvider TranslatorProviders} from list given at construction time in given
   * order.
   *
   * @param operator operator to translate
   * @param <InputT> the type of input elements
   * @param <OutputT> the type of output elements
   * @param <OperatorT> the type of the euphoria operator
   * @return first {@code Optional<OperatorTranslator<InputT, OutputT, OperatorT>>} which {@link
   *     OperatorTranslator#canTranslate(Operator) can translate} given operator.
   */
  @Override
  public <InputT, OutputT, OperatorT extends Operator<OutputT>>
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(OperatorT operator) {

    for (TranslatorProvider provider : orderedTranslatorsChain) {
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> maybeTranslator =
          provider.findTranslator(operator);

      if (maybeTranslator.isPresent() && maybeTranslator.get().canTranslate(operator)) {
        return maybeTranslator;
      }
    }

    return Optional.empty();
  }
}
