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
package org.apache.beam.runners.jstorm.translation.translator;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry of JStorm {@link TransformTranslator TransformTranslators}.
 */
public class TranslatorRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TranslatorRegistry.class);

  private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS =
      new HashMap<>();

  static {
    // TODO: add translators.
  }

  /**
   * Returns the {@link TransformTranslator} for the given {@link PTransform}.
   */
  public static TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
    TransformTranslator<?> translator = TRANSLATORS.get(transform.getClass());
    if (translator == null) {
      LOG.warn("Unsupported operator={}", transform.getClass().getName());
    }
    return translator;
  }
}
