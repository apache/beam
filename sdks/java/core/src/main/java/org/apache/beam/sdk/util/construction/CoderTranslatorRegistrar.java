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
}
