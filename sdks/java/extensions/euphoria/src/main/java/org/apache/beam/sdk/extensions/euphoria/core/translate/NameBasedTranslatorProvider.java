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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translation provider that selects name matching translation (operator name starts same as added
 * {@link Builder#addShortNameTranslation(Class, OperatorTranslator, String)} translation). If no
 * match, then its used {@link Builder#setDefaultTranslationProvider(TranslatorProvider)}.
 */
public class NameBasedTranslatorProvider implements TranslatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(NameBasedTranslatorProvider.class);

  private final Multimap<Class<? extends Operator>, TranslationCandidate> translators;
  private final TranslatorProvider defaultTranslationProvider;

  private NameBasedTranslatorProvider(
      Multimap<Class<? extends Operator>, TranslationCandidate> translators,
      TranslatorProvider defaultTranslationProvider) {
    this.translators = translators;
    this.defaultTranslationProvider = defaultTranslationProvider;
  }

  /**
   * Filters to all translator that can translate operator. Then choose first match if translator
   * starts with shortName in shortTranslatorNameMap or if it operator contains whole name of
   * translator. If no match in operator's name it choose first translator it can translate.
   */
  @Override
  public <InputT, OutputT, OperatorT extends Operator<OutputT>>
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(OperatorT operator) {

    Collection<TranslationCandidate> candidates = translators.get(operator.getClass());

    Optional<OperatorTranslator<InputT, OutputT, OperatorT>> chosenTranslator =
        candidates
            .stream()
            .filter(candidate -> candidate.getTranslator().canTranslate(operator))
            .filter(candidate -> isOperatorNameInTranslator(candidate, operator))
            .map(TranslationCandidate::<InputT, OutputT, OperatorT>getTranslator)
            .findFirst();

    if (chosenTranslator.isPresent()) {
      LOG.info("For operator {} was chosen translator {}", operator, chosenTranslator.get());
      return chosenTranslator;
    }

    return defaultTranslationProvider.findTranslator(operator);
  }

  /**
   * Create a new builder for provider.
   *
   * @return builder
   */
  public static NameBasedTranslatorProvider.Builder newBuilder() {
    return new NameBasedTranslatorProvider.Builder();
  }

  public <OutputT, OperatorT extends Operator<OutputT>> boolean isOperatorNameInTranslator(
      TranslationCandidate translationCandidate, OperatorT operator) {
    if (!operator.getName().isPresent()) {
      return false;
    }

    String translationName = translationCandidate.getTranslationName().toLowerCase();
    return operator.getName().get().toLowerCase().startsWith(translationName);
  }

  /** {@link NameBasedTranslatorProvider} builder. */
  public static class Builder {

    private final Multimap<Class<? extends Operator>, TranslationCandidate> translators =
        ArrayListMultimap.create();
    private TranslatorProvider defaultTranslationProvider;

    private Builder() {}

    public NameBasedTranslatorProvider.Builder setDefaultTranslationProvider(
        TranslatorProvider defaultTranslationProvider) {
      this.defaultTranslationProvider = defaultTranslationProvider;
      return this;
    }

    /**
     * If more translators can translate operator choose the one which starts with
     * operator.getName() == shortName (case insensitive).
     *
     * @param operator to translate
     * @param translatorClass class of translation
     * @param shortName on which should operator name start
     * @return builder
     */
    public NameBasedTranslatorProvider.Builder addShortNameTranslation(
        Class<? extends Operator> operator,
        OperatorTranslator<?, ?, ?> translatorClass,
        String shortName) {
      translators.put(
          Objects.requireNonNull(operator),
          new TranslationCandidate(
              Objects.requireNonNull(shortName), Objects.requireNonNull(translatorClass)));
      return this;
    }

    public NameBasedTranslatorProvider build() {
      return new NameBasedTranslatorProvider(
          translators, Objects.requireNonNull(defaultTranslationProvider));
    }
  }

  static class TranslationCandidate {

    final String translationName;
    final OperatorTranslator<?, ?, ?> translator;

    TranslationCandidate(String translationName, OperatorTranslator<?, ?, ?> operatorTranslator) {
      this.translationName = translationName;
      this.translator = operatorTranslator;
    }

    String getTranslationName() {
      return translationName;
    }

    @SuppressWarnings("unchecked")
    <InputT, OutputT, OperatorT extends Operator>
        OperatorTranslator<InputT, OutputT, OperatorT> getTranslator() {
      return (OperatorTranslator<InputT, OutputT, OperatorT>) translator;
    }
  }
}
