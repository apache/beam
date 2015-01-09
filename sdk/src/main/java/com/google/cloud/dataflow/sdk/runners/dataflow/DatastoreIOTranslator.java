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

import com.google.cloud.dataflow.sdk.io.DatastoreIO;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TransformTranslator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TranslationContext;

/**
 * Datastore transform support code for the Dataflow backend.
 */
public class DatastoreIOTranslator {

  /**
   * Implements DatastoreIO Write translation for the Dataflow backend.
   */
  public static class WriteTranslator implements TransformTranslator<DatastoreIO.Sink> {
    @Override
    public void translate(
        DatastoreIO.Sink transform,
        TranslationContext context) {
      // TODO: Not implemented yet.
      // translateWriteHelper(transform, context);
      throw new UnsupportedOperationException("Write only supports direct mode now.");
    }
  }
}
