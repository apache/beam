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

import java.util.Collections;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Dataset;

public class ImpulseTranslatorBatch
    implements TransformTranslator<PTransform<PBegin, PCollection<byte[]>>> {

  @Override
  public void translateTransform(
      PTransform<PBegin, PCollection<byte[]>> transform, TranslationContext context) {
    Coder<WindowedValue<byte[]>> windowedValueCoder =
        WindowedValue.FullWindowedValueCoder.of(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE);
    Dataset<WindowedValue<byte[]>> dataset =
        context
            .getSparkSession()
            .createDataset(
                Collections.singletonList(WindowedValue.valueInGlobalWindow(new byte[0])),
                EncoderHelpers.fromBeamCoder(windowedValueCoder));
    context.putDataset(context.getOutput(), dataset);
  }
}
