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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import org.apache.beam.sdk.values.PCollection;
import org.apache.gearpump.streaming.dsl.api.functions.MapFunction;
import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.functions.GroupByFunction;
import org.apache.gearpump.streaming.dsl.window.api.Discarding$;
import org.apache.gearpump.streaming.dsl.window.api.EventTimeTrigger$;
import org.apache.gearpump.streaming.dsl.window.api.WindowFunction;
import org.apache.gearpump.streaming.dsl.window.api.Windows;
import org.apache.gearpump.streaming.dsl.window.impl.Window;

/**
 * {@link GroupByKey} is translated to Gearpump groupBy function.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByKeyTranslator<K, V> implements TransformTranslator<GroupByKey<K, V>> {
  @Override
  public void translate(GroupByKey<K, V> transform, TranslationContext context) {
    PCollection<KV<K, V>> input = context.getInput(transform);
    Coder<K> inputKeyCoder = ((KvCoder<K, V>) input.getCoder()).getKeyCoder();
    JavaStream<WindowedValue<KV<K, V>>> inputStream =
        context.getInputStream(input);
    int parallelism = context.getPipelineOptions().getParallelism();
    OutputTimeFn<? super BoundedWindow> outputTimeFn = (OutputTimeFn<? super BoundedWindow>)
        input.getWindowingStrategy().getOutputTimeFn();
    WindowFn<KV<K, V>, BoundedWindow> windowFn = (WindowFn<KV<K, V>, BoundedWindow>)
        input.getWindowingStrategy().getWindowFn();
    JavaStream<WindowedValue<KV<K, Iterable<V>>>> outputStream = inputStream
        .window(Windows.apply(
            new GearpumpWindowFn(windowFn.isNonMerging()),
            EventTimeTrigger$.MODULE$, Discarding$.MODULE$), "assign_window")
        .groupBy(new GroupByFn<K, V>(inputKeyCoder), parallelism, "group_by_Key_and_Window")
        .map(new ValueToIterable<K, V>(), "map_value_to_iterable")
        .map(new KeyedByTimestamp<K, V>((OutputTimeFn<? super BoundedWindow>)
            input.getWindowingStrategy().getOutputTimeFn()), "keyed_by_timestamp")
        .reduce(new Merge<>(windowFn, outputTimeFn), "merge")
        .map(new Values<K, V>(), "values");

    context.setOutputStream(context.getOutput(transform), outputStream);
  }

  private static class GearpumpWindowFn<T, W extends BoundedWindow>
      implements WindowFunction<WindowedValue<T>>, Serializable {

    private final boolean isNonMerging;

    public GearpumpWindowFn(boolean isNonMerging) {
      this.isNonMerging = isNonMerging;
    }

    @Override
    public Window[] apply(Context<WindowedValue<T>> context) {
      try {
        return toGearpumpWindows(context.element().getWindows().toArray(new BoundedWindow[0]));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isNonMerging() {
      return isNonMerging;
    }

    private Window[] toGearpumpWindows(BoundedWindow[] windows) {
      Window[] gwins = new Window[windows.length];
      for (int i = 0; i < windows.length; i++) {
        gwins[i] = TranslatorUtils.boundedWindowToGearpumpWindow(windows[i]);
      }
      return gwins;
    }
  }

  private static class GroupByFn<K, V> extends
      GroupByFunction<WindowedValue<KV<K, V>>, ByteBuffer> {

    private final Coder<K> keyCoder;

    GroupByFn(Coder<K> keyCoder) {
      this.keyCoder = keyCoder;
    }

    @Override
    public ByteBuffer apply(WindowedValue<KV<K, V>> wv) {
      try {
        return ByteBuffer.wrap(CoderUtils.encodeToByteArray(keyCoder, wv.getValue().getKey()));
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class ValueToIterable<K, V>
      extends MapFunction<WindowedValue<KV<K, V>>, WindowedValue<KV<K, Iterable<V>>>> {

    @Override
    public WindowedValue<KV<K, Iterable<V>>> apply(WindowedValue<KV<K, V>> wv) {
      Iterable<V> values = Lists.newArrayList(wv.getValue().getValue());
      return wv.withValue(KV.of(wv.getValue().getKey(), values));
    }
  }

  private static class KeyedByTimestamp<K, V>
      extends MapFunction<WindowedValue<KV<K, Iterable<V>>>,
      KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>>> {

    private final OutputTimeFn<? super BoundedWindow> outputTimeFn;

    public KeyedByTimestamp(OutputTimeFn<? super BoundedWindow> outputTimeFn) {
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> apply(
        WindowedValue<KV<K, Iterable<V>>> wv) {
      org.joda.time.Instant timestamp = outputTimeFn.assignOutputTime(wv.getTimestamp(),
          Iterables.getOnlyElement(wv.getWindows()));
      return KV.of(timestamp, wv);
    }
  }

  private static class Merge<K, V> extends
      ReduceFunction<KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>>> {

    private final WindowFn<KV<K, V>, BoundedWindow> windowFn;
    private final OutputTimeFn<? super BoundedWindow> outputTimeFn;

    Merge(WindowFn<KV<K, V>, BoundedWindow> windowFn,
        OutputTimeFn<? super BoundedWindow> outputTimeFn) {
      this.windowFn = windowFn;
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> apply(
        KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> kv1,
        KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> kv2) {
      org.joda.time.Instant t1 = kv1.getKey();
      org.joda.time.Instant t2 = kv2.getKey();

      final WindowedValue<KV<K, Iterable<V>>> wv1 = kv1.getValue();
      final WindowedValue<KV<K, Iterable<V>>> wv2 = kv2.getValue();

      final List<BoundedWindow> mergedWindows = new ArrayList<>();
      if (!windowFn.isNonMerging()) {
        try {
          windowFn.mergeWindows(windowFn.new MergeContext() {

            @Override
            public Collection<BoundedWindow> windows() {
              ArrayList<BoundedWindow> windows = new ArrayList<>();
              windows.addAll(wv1.getWindows());
              windows.addAll(wv2.getWindows());
              return windows;
            }

            @Override
            public void merge(Collection<BoundedWindow> toBeMerged,
                BoundedWindow mergeResult) throws Exception {
              mergedWindows.add(mergeResult);
            }
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        mergedWindows.addAll(wv1.getWindows());
      }

      org.joda.time.Instant timestamp = outputTimeFn.combine(t1, t2);
      return KV.of(timestamp,
          WindowedValue.of(KV.of(wv1.getValue().getKey(),
              Iterables.concat(wv1.getValue().getValue(), wv2.getValue().getValue())), timestamp,
              mergedWindows, wv1.getPane()));
    }
  }

  private static class Values<K, V> extends
      MapFunction<KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>>,
          WindowedValue<KV<K, Iterable<V>>>> {

    @Override
    public WindowedValue<KV<K, Iterable<V>>> apply(KV<org.joda.time.Instant,
        WindowedValue<KV<K, Iterable<V>>>> kv) {
      org.joda.time.Instant timestamp = kv.getKey();
      WindowedValue<KV<K, Iterable<V>>> wv = kv.getValue();
      return WindowedValue.of(wv.getValue(), timestamp, wv.getWindows(), wv.getPane());
    }
  }
}
