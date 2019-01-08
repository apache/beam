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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.mocks;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Mock translator that generates a source of 0 to 999 and prints it.
 * @param <T>
 */
public class ReadSourceTranslatorMockBatch<T>
    implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {

  private static String sourceProviderClass = DatasetSourceMockBatch.class.getCanonicalName();

  @SuppressWarnings("unchecked")
  @Override
  public void translateTransform(
      PTransform<PBegin, PCollection<T>> transform, TranslationContext context) {
    SparkSession sparkSession = context.getSparkSession();

    Dataset<Row> rowDataset = sparkSession.read().format(sourceProviderClass).load();

    MapFunction<Row, WindowedValue> func = new MapFunction<Row, WindowedValue>() {
      @Override public WindowedValue call(Row value) throws Exception {
        //there is only one value put in each Row by the InputPartitionReader
        return value.<WindowedValue>getAs(0);
      }
    };
    //TODO: is there a better way than using the raw WindowedValue? Can an Encoder<WindowedVAlue<T>>
    // be created ?
    Dataset<WindowedValue> dataset = rowDataset.map(func, Encoders.kryo(WindowedValue.class));

    PCollection<T> output = (PCollection<T>) context.getOutput();
    context.putDatasetRaw(output, dataset);
  }
}
