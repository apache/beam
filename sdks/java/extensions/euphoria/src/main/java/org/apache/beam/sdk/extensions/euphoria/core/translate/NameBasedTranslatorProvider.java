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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translation provider that selects name matching translation ( operator name starts same as added
 * {@link Builder#addShortNameTranslation(Class, String)} translation or first matching translation
 * for the registered operator.
 */
public class NameBasedTranslatorProvider implements TranslatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(NameBasedTranslatorProvider.class);
  private final Multimap<Class<? extends Operator>, OperatorTranslator<?, ?, ?>> translators;
  private final Map<Class<? extends OperatorTranslator>, String> shortTranslatorNameMap;

  private NameBasedTranslatorProvider(
      Multimap<Class<? extends Operator>, OperatorTranslator<?, ?, ?>> translators,
      Map<Class<? extends OperatorTranslator>, String> shortTranslatorNameMap) {
    this.translators = translators;
    this.shortTranslatorNameMap = shortTranslatorNameMap;
  }

  /**
   * Create a new builder for provider.
   *
   * @return builder
   */
  public static NameBasedTranslatorProvider.Builder newBuilder() {
    return new NameBasedTranslatorProvider.Builder();
  }

  /**
   * Filters to all translator that can translate operator. Then choose first match if translator
   * starts with shortName in shortTranslatorNameMap or if it operator contains whole name of
   * translator. If no match in operator's name it choose first translator it can translate.
   */
  @Override
  public <InputT, OutputT, OperatorT extends Operator<OutputT>>
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(OperatorT operator) {
    @SuppressWarnings("unchecked")
    final Collection<OperatorTranslator<InputT, OutputT, OperatorT>> candidates =
        (Collection) translators.get(operator.getClass());
    if (!candidates.isEmpty()) {
      List<OperatorTranslator<InputT, OutputT, OperatorT>> possibleTranslators =
          candidates
              .stream()
              .filter(candidate -> candidate.canTranslate(operator))
              .collect(Collectors.toList());

      Stream<OperatorTranslator<InputT, OutputT, OperatorT>> translatorStream =
          possibleTranslators.stream();
      if (possibleTranslators.size() > 1) {
        translatorStream =
            translatorStream.filter(candidate -> isOperatorNameInTranslator(candidate, operator));
      }

      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> chosenTranslator =
          translatorStream.findFirst();
      if (!chosenTranslator.isPresent()) { //name wasnt filled fallback to first
        chosenTranslator = candidates.stream().findFirst();
      }
      LOG.info("For operator {} was chosen translator {}", operator, chosenTranslator.orElse(null));
      return chosenTranslator;
    }
    return Optional.empty();
  }

  public <InputT, OutputT, OperatorT extends Operator<OutputT>> boolean isOperatorNameInTranslator(
      OperatorTranslator<InputT, OutputT, OperatorT> candidate, OperatorT operator) {
    if (!operator.getName().isPresent()) {
      return false;
    }
    Class<? extends OperatorTranslator> candidateClass = candidate.getClass();
    String operatorName = operator.getName().get().toLowerCase();
    if (shortTranslatorNameMap.containsKey(candidateClass)) {
      return shortTranslatorNameMap.get(candidateClass).startsWith(operatorName);
    }

    return operatorName.contains(candidateClass.getSimpleName().toLowerCase());
  }

  /** {@link NameBasedTranslatorProvider} builder. */
  public static class Builder {

    private final Multimap<Class<? extends Operator>, OperatorTranslator<?, ?, ?>> translators =
        ArrayListMultimap.create();
    private final Map<Class<? extends OperatorTranslator>, String> shortTranslatorNameMap =
        new HashMap<>();

    private Builder() {}

    public NameBasedTranslatorProvider.Builder registerTranslator(
        Class<? extends Operator> clazz, OperatorTranslator<?, ?, ?> operatorTranslator) {
      translators.put(clazz, operatorTranslator);
      return this;
    }

    /**
     * If more translators can translate operator choose the one which starts with
     * operator.getName() == shortName (case insensitive).
     *
     * @param translatorClass class of translation
     * @param shortName on which should operator name start
     * @return builder
     */
    public NameBasedTranslatorProvider.Builder addShortNameTranslation(
        Class<? extends OperatorTranslator> translatorClass, String shortName) {
      shortTranslatorNameMap.put(translatorClass, shortName);
      return this;
    }

    public NameBasedTranslatorProvider build() {
      return new NameBasedTranslatorProvider(translators, shortTranslatorNameMap);
    }
  }
}
