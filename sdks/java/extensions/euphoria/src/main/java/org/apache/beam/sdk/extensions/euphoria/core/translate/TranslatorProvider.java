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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;

/**
 * Simple interface that allows user to define translation of his own.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Deprecated
public interface TranslatorProvider {

  static TranslatorProvider of(Pipeline pipeline) {
    return pipeline.getOptions().as(EuphoriaOptions.class).getTranslatorProvider();
  }

  /**
   * Find translation for the given operator. It is possible that no translation exists for the
   * operator. You can provide your own provider using {@link EuphoriaOptions}.
   *
   * @param operator operator to translate
   * @param <InputT> input type
   * @param <OutputT> output type
   * @param <OperatorT> operator type
   * @return translation if available
   */
  <InputT, OutputT, OperatorT extends Operator<OutputT>>
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(OperatorT operator);
}
