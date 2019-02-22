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

import java.io.IOException;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.WindowingHelpers;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class ReadSourceTranslatorBatch<T>
    implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {

  private static String sourceProviderClass = DatasetSourceBatch.class.getCanonicalName();

  @SuppressWarnings("unchecked")
  @Override
  public void translateTransform(
      PTransform<PBegin, PCollection<T>> transform, TranslationContext context) {
    AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> rootTransform =
        (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
            context.getCurrentTransform();

    BoundedSource<T> source;
    try {
      source = ReadTranslation.boundedSourceFromTransform(rootTransform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SparkSession sparkSession = context.getSparkSession();

    String serializedSource = Base64Serializer.serializeUnchecked(source);
    Dataset<Row> rowDataset =
        sparkSession
            .read()
            .format(sourceProviderClass)
            .option(DatasetSourceBatch.BEAM_SOURCE_OPTION, serializedSource)
            .option(
                DatasetSourceBatch.DEFAULT_PARALLELISM,
                String.valueOf(context.getSparkSession().sparkContext().defaultParallelism()))
            .option(
                DatasetSourceBatch.PIPELINE_OPTIONS, context.getSerializableOptions().toString())
            .load();

    // extract windowedValue from Row
    Dataset<WindowedValue<T>> dataset =
        rowDataset.map(
            WindowingHelpers.rowToWindowedValueMapFunction(source.getOutputCoder()),
            EncoderHelpers.windowedValueEncoder());

    PCollection<T> output = (PCollection<T>) context.getOutput();
    context.putDataset(output, dataset);
  }
}
