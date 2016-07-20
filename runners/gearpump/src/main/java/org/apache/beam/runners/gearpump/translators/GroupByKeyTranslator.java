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

package org.apache.beam.runners.gearpump.translators;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import com.google.common.collect.Iterables;

import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.javaapi.dsl.functions.FlatMapFunction;
import org.apache.gearpump.streaming.javaapi.dsl.functions.GroupByFunction;
import org.apache.gearpump.streaming.javaapi.dsl.functions.MapFunction;
import org.apache.gearpump.streaming.javaapi.dsl.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * {@link GroupByKey} is translated to Gearpump groupBy function.
 */
public class GroupByKeyTranslator<K, V> implements TransformTranslator<GroupByKey<K, V>> {
  @Override
  public void translate(GroupByKey<K, V> transform, TranslationContext context) {
    JavaStream<WindowedValue<KV<K, V>>> inputStream =
        context.getInputStream(context.getInput(transform));
    int parallelism = context.getPipelineOptions().getParallelism();
    JavaStream<WindowedValue<KV<K, Iterable<V>>>> outputStream = inputStream
        .flatMap(new KeyedByKeyAndWindow<K, V>(), "keyed_by_Key_and_Window")
        .groupBy(new GroupByKeyAndWindow<K, V>(), parallelism, "group_by_Key_and_Window")
        .map(new ExtractKeyValue<K, V>(), "extract_Key_and_Value")
        .reduce(new MergeValue<K, V>(), "merge_value");

    context.setOutputStream(context.getOutput(transform), outputStream);
  }

  private static class KeyedByKeyAndWindow<K, V> implements
      FlatMapFunction<WindowedValue<KV<K, V>>, WindowedValue<KV<KV<K, BoundedWindow>, V>>> {

    @Override
    public Iterator<WindowedValue<KV<KV<K, BoundedWindow>, V>>> apply(WindowedValue<KV<K, V>> wv) {
      List<WindowedValue<KV<KV<K, BoundedWindow>, V>>> ret = new ArrayList<>(wv.getWindows().size
          ());
      for (BoundedWindow window : wv.getWindows()) {
        KV<K, BoundedWindow> keyWin = KV.of(wv.getValue().getKey(), window);
        ret.add(WindowedValue.of(KV.of(keyWin, wv.getValue().getValue()),
            wv.getTimestamp(), window, wv.getPane()));
      }
      return ret.iterator();
    }
  }

  private static class GroupByKeyAndWindow<K, V> implements
      GroupByFunction<WindowedValue<KV<KV<K, BoundedWindow>, V>>, KV<K, BoundedWindow>> {

    @Override
    public KV<K, BoundedWindow> apply(WindowedValue<KV<KV<K, BoundedWindow>, V>> wv) {
      return wv.getValue().getKey();
    }
  }

  private static class ExtractKeyValue<K, V> implements
      MapFunction<WindowedValue<KV<KV<K, BoundedWindow>, V>>,
          WindowedValue<KV<K, Iterable<V>>>> {
    @Override
    public WindowedValue<KV<K, Iterable<V>>> apply(WindowedValue<KV<KV<K, BoundedWindow>, V>> wv) {
      return WindowedValue.of(KV.of(wv.getValue().getKey().getKey(),
              (Iterable<V>) Collections.singletonList(wv.getValue().getValue())),
          wv.getTimestamp(), wv.getWindows(), wv.getPane());
    }
  }

  private static class MergeValue<K, V> implements
      ReduceFunction<WindowedValue<KV<K, Iterable<V>>>> {
    @Override
    public WindowedValue<KV<K, Iterable<V>>> apply(WindowedValue<KV<K, Iterable<V>>> wv1,
        WindowedValue<KV<K, Iterable<V>>> wv2) {
      return WindowedValue.of(KV.of(wv1.getValue().getKey(),
              Iterables.concat(wv1.getValue().getValue(), wv2.getValue().getValue())),
          wv1.getTimestamp(), wv1.getWindows(), wv1.getPane());
    }
  }
}
