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
package org.apache.beam.sdk.util.construction;

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A registrar of {@link Coder} URNs to the associated {@link CoderTranslator}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public interface CoderTranslatorRegistrar {
  /**
   * Returns a mapping of coder classes to the URN representing that coder.
   *
   * <p>URNs must map to only one coder.
   */
  Map<Class<? extends Coder>, String> getCoderURNs();

  /** Returns a mapping of URN to {@link CoderTranslator}. */
  Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> getCoderTranslators();

  /**
   * Returns whether the given Coder is known to this CoderTranslatorRegistrar. If the Coder is
   * known, then getCoderTranslator() will return a non-null CoderTranslator.
   */
  default boolean isKnownCoder(Coder<?> coder, PipelineOptions options) {
    return getCoderURNs().containsKey(coder.getClass());
  }

  /** Returns the CoderTranslator to use for this Coder, or null if the Coder is not known. */
  default @Nullable CoderTranslator<? extends Coder> getCoderTranslator(
      Class<? extends Coder> coderClass) {
    return getCoderTranslators().get(coderClass);
  }

  /** Returns the Coder to use for the given Urn, or null if the Urn is for an unknown Coder. */
  default @Nullable Class<? extends Coder> getCoderForUrn(String coderUrn) {
    for (Map.Entry<Class<? extends Coder>, String> coderUrnEntry : getCoderURNs().entrySet()) {
      if (coderUrnEntry.getValue().equals(coderUrn)) {
        return coderUrnEntry.getKey();
      }
    }
    return null;
  }
}
