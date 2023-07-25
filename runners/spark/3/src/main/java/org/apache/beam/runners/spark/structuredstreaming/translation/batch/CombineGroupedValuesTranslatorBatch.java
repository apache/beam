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
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.Fun1;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * Translator for {@link Combine.GroupedValues} if the {@link CombineFn} doesn't require context /
 * side-inputs.
 *
 * <p>This doesn't require a Spark {@link Aggregator}. Instead it can directly use the respective
 * {@link CombineFn} to reduce each iterable of values into an aggregated output value.
 */
class CombineGroupedValuesTranslatorBatch<K, InT, AccT, OutT>
    extends TransformTranslator<
        PCollection<? extends KV<K, ? extends Iterable<InT>>>,
        PCollection<KV<K, OutT>>,
        Combine.GroupedValues<K, InT, OutT>> {

  CombineGroupedValuesTranslatorBatch() {
    super(0.2f);
  }

  @Override
  protected void translate(Combine.GroupedValues<K, InT, OutT> transform, Context cxt)
      throws IOException {
    CombineFn<InT, AccT, OutT> combineFn = (CombineFn<InT, AccT, OutT>) transform.getFn();

    Encoder<WindowedValue<KV<K, OutT>>> enc = cxt.windowedEncoder(cxt.getOutput().getCoder());
    Dataset<WindowedValue<KV<K, Iterable<InT>>>> inputDs = (Dataset) cxt.getDataset(cxt.getInput());

    cxt.putDataset(cxt.getOutput(), inputDs.map(reduce(combineFn), enc));
  }

  @Override
  public boolean canTranslate(Combine.GroupedValues<K, InT, OutT> transform) {
    return !(transform.getFn() instanceof CombineWithContext);
  }

  private static <K, InT, AccT, OutT>
      Fun1<WindowedValue<KV<K, Iterable<InT>>>, WindowedValue<KV<K, OutT>>> reduce(
          CombineFn<InT, AccT, OutT> fn) {
    return wv -> {
      KV<K, ? extends Iterable<InT>> kv = wv.getValue();
      AccT acc = null;
      for (InT in : kv.getValue()) {
        acc = fn.addInput(acc != null ? acc : fn.createAccumulator(), in);
      }
      OutT res = acc != null ? fn.extractOutput(acc) : fn.defaultValue();
      return wv.withValue(KV.of(kv.getKey(), res));
    };
  }
}
