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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import java.io.ByteArrayInputStream;
import org.apache.beam.runners.spark.structuredstreaming.translation.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.AggregatorCombiner;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class CombinePerKeyTranslatorBatch<K, InputT, AccumT, OutputT> implements
    TransformTranslator<PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {

  @Override public void translateTransform(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      TranslationContext context) {


    Combine.PerKey combineTransform = (Combine.PerKey) transform;
    @SuppressWarnings("unchecked")
    final PCollection<KV<K, InputT>> input = (PCollection<KV<K, InputT>>)context.getInput();
    @SuppressWarnings("unchecked")
    final PCollection<KV<K, OutputT>> output = (PCollection<KV<K, OutputT>>)context.getOutput();
    @SuppressWarnings("unchecked")
    final KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) input.getCoder();
    @SuppressWarnings("unchecked")
    final KvCoder<K, OutputT> outputCoder = (KvCoder<K, OutputT>) output.getCoder();
    @SuppressWarnings("unchecked")
    final Combine.CombineFn<InputT, AccumT, OutputT> combineFn = (Combine.CombineFn<InputT, AccumT, OutputT>)combineTransform.getFn();

    final Coder<AccumT> accumulatorCoder;
    try {
      accumulatorCoder =
          combineFn.getAccumulatorCoder(
              input.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Could not determine coder for accumulator", e);
    }

    Dataset<WindowedValue<KV<K, InputT>>> inputDataset = context.getDataset(context.getInput());

    Dataset<KV<K, InputT>> keyedDataset = inputDataset
        .map((MapFunction<WindowedValue<KV<K, InputT>>, KV<K, InputT>>) WindowedValue::getValue,
            EncoderHelpers.kvEncoder());
    //TODO do the aggregation per key
    Dataset<Row> rowDataset = keyedDataset.select(
        new AggregatorCombiner<>(combineFn, accumulatorCoder, outputCoder.getValueCoder())
            .toColumn());


    // extract KV from Row
    MapFunction<Row, KV<K, OutputT>> func =
        new MapFunction<Row, KV<K, OutputT>>() {
          @Override
          public KV<K, OutputT> call(Row value) throws Exception {
            //TODO replace with outputCoder
            byte[] bytes = (byte[]) value.get(0);
            // it was serialized by kryo
            Kryo kryo = new Kryo();
            Input kryoInput = new Input(new ByteArrayInputStream(bytes));
            @SuppressWarnings("unchecked")
            KV<K, OutputT> kv= (KV<K, OutputT>)kryo.readClassAndObject(kryoInput);
            kryoInput.close();
            return kv;
          }
        };
    Dataset< KV<K, OutputT>> dataset = rowDataset.map(func, EncoderHelpers.kvEncoder());


    // Window the result into global window.
    //TODO generify and make a util method for use with Pardo/GBK and combine
    Dataset<WindowedValue<KV<K, OutputT>>> outputDataset =
        dataset.map(
            (MapFunction<KV<K, OutputT>, WindowedValue<KV<K, OutputT>>>)
                WindowedValue::valueInGlobalWindow,
            EncoderHelpers.windowedValueEncoder());
    context.putDataset(output, outputDataset);

  }
}
