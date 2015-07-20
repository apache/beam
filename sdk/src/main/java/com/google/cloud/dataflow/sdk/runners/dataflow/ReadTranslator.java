/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.dataflow;

import static com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TransformTranslator;
import static com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TranslationContext;

import com.google.cloud.dataflow.sdk.io.Read;

/**
 * Translator for the {@code Read} {@code PTransform} for the Dataflow back-end.
 */
public class ReadTranslator implements TransformTranslator<Read.Bounded<?>> {
  @Override
  public void translate(Read.Bounded<?> transform, TranslationContext context) {
    BasicSerializableSourceFormat.translateReadHelper(transform.getSource(), transform, context);
  }
}
