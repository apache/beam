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

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.tuple;
import static org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING;
import static org.apache.beam.sdk.transforms.windowing.TimestampCombiner.END_OF_WINDOW;

import java.util.Collection;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.Fun1;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;
import scala.collection.TraversableOnce;

abstract class GroupingTranslator<
        KeyT,
        InT,
        OutT,
        TransformT extends
            PTransform<? extends PCollection<KV<KeyT, InT>>, PCollection<KV<KeyT, OutT>>>>
    extends TransformTranslator<
        PCollection<KV<KeyT, InT>>, PCollection<KV<KeyT, OutT>>, TransformT> {

  static boolean eligibleForGroupByWindow(
      WindowingStrategy<?, ?> windowing, boolean endOfWindowOnly) {
    return !windowing.needsMerge()
        && (!endOfWindowOnly || windowing.getTimestampCombiner() == END_OF_WINDOW)
        && windowing.getWindowFn().windowCoder().consistentWithEquals();
  }

  static boolean eligibleForGlobalGroupBy(
      WindowingStrategy<?, ?> windowing, boolean endOfWindowOnly) {
    return windowing.getWindowFn() instanceof GlobalWindows
        && (!endOfWindowOnly || windowing.getTimestampCombiner() == END_OF_WINDOW);
  }

  /** Encoder for composite key of window and key. */
  protected Encoder<Tuple2<BoundedWindow, KeyT>> windowedKeyEnc(Encoder<KeyT> keyEnc, Context cxt) {
    return cxt.tupleEncoder(cxt.windowEncoder(), keyEnc);
  }

  static <K, V, T>
      Fun1<WindowedValue<KV<K, V>>, TraversableOnce<Tuple2<Tuple2<BoundedWindow, K>, T>>>
          explodeWindowedKey(Fun1<WindowedValue<KV<K, V>>, T> valueFn) {
    return v -> {
      T value = valueFn.apply(v);
      K key = v.getValue().getKey();
      Collection<BoundedWindow> windows = (Collection<BoundedWindow>) v.getWindows();
      return ScalaInterop.scalaIterator(windows).map(w -> tuple(tuple(w, key), value));
    };
  }

  static <K, V> Fun1<Tuple2<Tuple2<BoundedWindow, K>, V>, WindowedValue<KV<K, V>>> windowedKV() {
    return t -> windowedKV(t._1, t._2);
  }

  static <K, V> WindowedValue<KV<K, V>> windowedKV(Tuple2<BoundedWindow, K> key, V value) {
    return WindowedValue.of(KV.of(key._2, value), key._1.maxTimestamp(), key._1, NO_FIRING);
  }

  static <V> Fun1<WindowedValue<V>, V> value() {
    return v -> v.getValue();
  }

  static <K, V> Fun1<WindowedValue<KV<K, V>>, V> valueValue() {
    return v -> v.getValue().getValue();
  }

  static <K, V> Fun1<WindowedValue<KV<K, V>>, K> valueKey() {
    return v -> v.getValue().getKey();
  }
}
