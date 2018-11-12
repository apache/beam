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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;

/**
 * Default translation provider that selects first matching translation for the registered operator.
 */
public class SimpleTranslatorProvider implements TranslatorProvider {

  /**
   * Create a new builder for provider.
   *
   * @return builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** {@link SimpleTranslatorProvider} builder. */
  public static class Builder {

    private final Multimap<Class<? extends Operator>, OperatorTranslator<?, ?, ?>> translators =
        ArrayListMultimap.create();

    private Builder() {}

    public Builder registerTranslator(
        Class<? extends Operator> clazz, OperatorTranslator<?, ?, ?> operatorTranslator) {
      translators.put(clazz, operatorTranslator);
      return this;
    }

    public SimpleTranslatorProvider build() {
      return new SimpleTranslatorProvider(translators);
    }
  }

  private final Multimap<Class<? extends Operator>, OperatorTranslator<?, ?, ?>> translators;

  private SimpleTranslatorProvider(
      Multimap<Class<? extends Operator>, OperatorTranslator<?, ?, ?>> translators) {
    this.translators = translators;
  }

  @Override
  public <InputT, OutputT, OperatorT extends Operator<OutputT>>
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(OperatorT operator) {
    @SuppressWarnings("unchecked")
    final List<OperatorTranslator<InputT, OutputT, OperatorT>> candidates =
        new ArrayList<>((Collection) translators.get(operator.getClass()));
    if (!candidates.isEmpty()) {
      for (OperatorTranslator<InputT, OutputT, OperatorT> candidate : candidates) {
        if (candidate.canTranslate(operator)) {
          return Optional.of(candidate);
        }
      }
    }
    // try to fallback to composite translator
    final OperatorTranslator<InputT, OutputT, OperatorT> fallbackTranslator =
        new CompositeOperatorTranslator<>();
    if (fallbackTranslator.canTranslate(operator)) {
      return Optional.of(fallbackTranslator);
    }
    return Optional.empty();
  }
}
