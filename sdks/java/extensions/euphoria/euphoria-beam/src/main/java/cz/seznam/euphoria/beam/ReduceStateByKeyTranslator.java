/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.beam.window.BeamWindowFn;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ReduceStateByKeyTranslator implements OperatorTranslator<ReduceStateByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(ReduceStateByKey operator, BeamExecutorContext context) {
    PCollection<Object> input = context.getInput(operator);
    final UnaryFunction<Object, Object> keyExtractor = operator.getKeyExtractor();
    final UnaryFunction<Object, Object> valueExtractor = operator.getValueExtractor();

    final PCollection<KV<Object, Object>> extracted = input.apply(
        MapElements
            .into(kvDescriptor())
            .via(e -> KV.of(keyExtractor.apply(e), valueExtractor.apply(e))))
        .setCoder(KvCoder.of(new KryoCoder<>(), new KryoCoder<>()));

//    final Windowing<Object, Window> windowing = operator.getWindowing();
//
//    if (windowing != null) {
//      BeamWindowFn wrap = BeamWindowFn.wrap(operator.getWindowing());
//      org.apache.beam.sdk.transforms.windowing.Window<Object> into =
//          org.apache.beam.sdk.transforms.windowing.Window.into(wrap);
//    }

//    if (operator.isCombinable()) {
//      // combine
//      kvs = kvs.apply(Combine.perKey(asSerializableCombinableFunction(
//          (ReduceFunctor) operator.getReducer())));
    // remap from KVs to Pairs
//      col = (PCollection<Object, Object>>) col.apply(
//          MapElements
//              .into(pairDescriptor())
//              .via((KV kv) -> Pair.of(kv.getKey(), kv.getValue())));
//      kvs.setCoder(new KryoCoder<>());
//    } else {
    // reduce
    return (PCollection<?>) extracted
        .apply(GroupByKey.create())
//        .apply(ParDo.of(ReduceByKeyTranslator.asReduceParDo(operator.get())))
        .setCoder(new KryoCoder<>());
  }

  private static TypeDescriptor<KV<Object, Object>> kvDescriptor() {
    return new TypeDescriptor<KV<Object, Object>>() {

    };
  }
}
