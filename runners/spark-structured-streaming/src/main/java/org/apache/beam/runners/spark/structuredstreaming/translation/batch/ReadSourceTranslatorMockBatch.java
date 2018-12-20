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
import org.apache.beam.runners.spark.structuredstreaming.translation.io.DatasetSourceMock;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;


/**
 * Mock translator that generates a source of 0 to 999 and prints it.
 * @param <T>
 */
class ReadSourceTranslatorMockBatch<T>
    implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {

  private String SOURCE_PROVIDER_CLASS = DatasetSourceMock.class.getCanonicalName();

  @SuppressWarnings("unchecked")
  @Override
  public void translateTransform(
      PTransform<PBegin, PCollection<T>> transform, TranslationContext context) {
    SparkSession sparkSession = context.getSparkSession();
    DataStreamReader dataStreamReader = sparkSession.readStream().format(SOURCE_PROVIDER_CLASS);

    Dataset<Row> rowDataset = dataStreamReader.load();

    MapFunction<Row, WindowedValue<Integer>> func = new MapFunction<Row, WindowedValue<Integer>>() {
      @Override public WindowedValue<Integer> call(Row value) throws Exception {
        //there is only one value put in each Row by the InputPartitionReader
        return value.<WindowedValue<Integer>>getAs(0);
      }
    };
    Dataset<WindowedValue<Integer>> dataset = rowDataset.map(func, new Encoder<WindowedValue<Integer>>() {

      @Override public StructType schema() {
        return null;
      }

      @Override public ClassTag<WindowedValue<Integer>> clsTag() {
        return scala.reflect.ClassTag$.MODULE$.<WindowedValue<Integer>>apply(WindowedValue.class);
      }
    });

    PCollection<T> output = (PCollection<T>) context.getOutput();
    context.putDataset(output, dataset);
    dataset.show();
  }
}
