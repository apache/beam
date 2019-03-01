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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.WindowingHelpers;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Dataset;

class WindowAssignTranslatorBatch<T>
    implements TransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<T>, PCollection<T>> transform, TranslationContext context) {

    Window.Assign<T> assignTransform = (Window.Assign<T>) transform;
    @SuppressWarnings("unchecked")
    final PCollection<T> input = (PCollection<T>) context.getInput();
    @SuppressWarnings("unchecked")
    final PCollection<T> output = (PCollection<T>) context.getOutput();

    Dataset<WindowedValue<T>> inputDataset = context.getDataset(input);
    if (WindowingHelpers.skipAssignWindows(assignTransform, context)) {
      context.putDataset(output, inputDataset);
    } else {
      Dataset<WindowedValue<T>> outputDataset = inputDataset
          .map(WindowingHelpers.assignWindowsMapFunction(assignTransform.getWindowFn()),
              EncoderHelpers.windowedValueEncoder());
      context.putDataset(output, outputDataset);
    }
  }
}
