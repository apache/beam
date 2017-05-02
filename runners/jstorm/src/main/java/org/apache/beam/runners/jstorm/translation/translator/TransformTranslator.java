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

import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * The class that translates {@link PTransform PTransforms} into JStorm components.
 */
public abstract class TransformTranslator<TransformT extends PTransform> {

  /**
   * Translates the {@link TransformT} with the given {@link TranslationContext}.
   */
  public abstract void translateNode(TransformT transform, TranslationContext context);

  /**
   * Returns true if the given {@link TransformT} can be translated.
   */
  public boolean canTranslate(TransformT transform, TranslationContext context) {
    return true;
  }
}
