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

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.internal.SQLConf;

class ReshuffleTranslatorBatch<K, V>
    extends TransformTranslator<PCollection<KV<K, V>>, PCollection<KV<K, V>>, Reshuffle<K, V>> {

  ReshuffleTranslatorBatch() {
    super(0.1f);
  }

  @Override
  protected void translate(Reshuffle<K, V> transform, Context cxt) throws IOException {
    Dataset<WindowedValue<KV<K, V>>> input = cxt.getDataset(cxt.getInput());
    cxt.putDataset(cxt.getOutput(), input.repartition(col("value.key")));
  }

  static class ViaRandomKey<V>
      extends TransformTranslator<PCollection<V>, PCollection<V>, Reshuffle.ViaRandomKey<V>> {

    ViaRandomKey() {
      super(0.1f);
    }

    @Override
    protected void translate(Reshuffle.ViaRandomKey<V> transform, Context cxt) throws IOException {
      Dataset<WindowedValue<V>> input = cxt.getDataset(cxt.getInput());
      // Reshuffle randomly
      cxt.putDataset(cxt.getOutput(), input.repartition(SQLConf.get().numShufflePartitions()));
    }
  }
}
