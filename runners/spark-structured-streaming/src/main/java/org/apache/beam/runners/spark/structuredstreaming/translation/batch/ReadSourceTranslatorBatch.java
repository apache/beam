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
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.DatasetStreamingSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

class ReadSourceTranslatorBatch<T>
    implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {

  private String SOURCE_PROVIDER_CLASS = DatasetSourceBatch.class.getCanonicalName();

  @SuppressWarnings("unchecked")
  @Override
  public void translateTransform(
      PTransform<PBegin, PCollection<T>> transform, TranslationContext context) {
    AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> rootTransform =
        (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
            context.getCurrentTransform();

        String providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"));
        BoundedSource<T> source;
    try {
      source = ReadTranslation.boundedSourceFromTransform(rootTransform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SparkSession sparkSession = context.getSparkSession();

    Dataset<Row> rowDataset = sparkSession.read().format(providerClassName).load();

    //TODO initialize source : how, to get a reference to the DatasetStreamingSource instance that spark
    // instantiates to be able to call DatasetStreamingSource.initialize(). How to pass in a DatasetCatalog?
    MapFunction<Row, WindowedValue<T>> func = new MapFunction<Row, WindowedValue<T>>() {
      @Override public WindowedValue<T> call(Row value) throws Exception {
        //there is only one value put in each Row by the InputPartitionReader
        return value.<WindowedValue<T>>getAs(0);
      }
    };
    //TODO fix encoder: how to get an Encoder<WindowedValue<T>>
    Dataset<WindowedValue<T>> dataset = rowDataset.map(func, null);

    PCollection<T> output = (PCollection<T>) context.getOutput();
    context.putDataset(output, dataset);
  }
}
