/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.io.ReadSource;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;

/**
 * Translator for the {@code ReadSource} {@code PTransform} for the Dataflow back-end.
 */
public class ReadSourceTranslator
    implements DataflowPipelineTranslator.TransformTranslator<ReadSource.Bound> {
  @Override
  public void translate(
      ReadSource.Bound transform, DataflowPipelineTranslator.TranslationContext context) {
    BasicSerializableSourceFormat.translateReadHelper(transform, context);
  }
}
