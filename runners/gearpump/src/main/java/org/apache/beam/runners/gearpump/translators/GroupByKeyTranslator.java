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

import io.gearpump.streaming.dsl.api.functions.FoldFunction;
import io.gearpump.streaming.dsl.api.functions.MapFunction;
import io.gearpump.streaming.dsl.javaapi.JavaStream;
import io.gearpump.streaming.dsl.javaapi.functions.GroupByFunction;
import io.gearpump.streaming.dsl.window.api.Discarding$;
import io.gearpump.streaming.dsl.window.api.EventTimeTrigger$;
import io.gearpump.streaming.dsl.window.api.WindowFunction;
import io.gearpump.streaming.dsl.window.api.Windows;
import io.gearpump.streaming.dsl.window.impl.Window;
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
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;

/** {@link GroupByKey} is translated to Gearpump groupBy function. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByKeyTranslator<K, V> implements TransformTranslator<GroupByKey<K, V>> {

  private static final long serialVersionUID = -8742202583992787659L;

  @Override
  public void translate(GroupByKey<K, V> transform, TranslationContext context) {
    PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) context.getInput();
    Coder<K> inputKeyCoder = ((KvCoder<K, V>) input.getCoder()).getKeyCoder();
    JavaStream<WindowedValue<KV<K, V>>> inputStream = context.getInputStream(input);
    int parallelism = context.getPipelineOptions().getParallelism();
    TimestampCombiner timestampCombiner = input.getWindowingStrategy().getTimestampCombiner();
    WindowFn<KV<K, V>, BoundedWindow> windowFn =
        (WindowFn<KV<K, V>, BoundedWindow>) input.getWindowingStrategy().getWindowFn();
    JavaStream<WindowedValue<KV<K, List<V>>>> outputStream =
        inputStream
            .window(
                Windows.apply(
                    new GearpumpWindowFn(windowFn.isNonMerging()),
                    EventTimeTrigger$.MODULE$,
                    Discarding$.MODULE$,
                    windowFn.toString()))
            .groupBy(new GroupByFn<>(inputKeyCoder), parallelism, "group_by_Key_and_Window")
            .map(new KeyedByTimestamp<>(windowFn, timestampCombiner), "keyed_by_timestamp")
            .fold(new Merge<>(windowFn, timestampCombiner), "merge")
            .map(new Values<>(), "values");

    context.setOutputStream(context.getOutput(), outputStream);
  }

  /** A transform used internally to translate Beam's Window to Gearpump's Window. */
  protected static class GearpumpWindowFn<T, W extends BoundedWindow>
      implements WindowFunction, Serializable {

    private final boolean isNonMerging;

    public GearpumpWindowFn(boolean isNonMerging) {
      this.isNonMerging = isNonMerging;
    }

    @Override
    public <T2> Window[] apply(Context<T2> context) {
      try {
        Object element = context.element();
        if (element instanceof TranslatorUtils.RawUnionValue) {
          element = ((TranslatorUtils.RawUnionValue) element).getValue();
        }
        return toGearpumpWindows(
            ((WindowedValue<T>) element).getWindows().toArray(new BoundedWindow[0]));
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

  /** A transform used internally to group KV message by its key. */
  protected static class GroupByFn<K, V>
      extends GroupByFunction<WindowedValue<KV<K, V>>, ByteBuffer> {

    private static final long serialVersionUID = -807905402490735530L;
    private final Coder<K> keyCoder;

    GroupByFn(Coder<K> keyCoder) {
      this.keyCoder = keyCoder;
    }

    @Override
    public ByteBuffer groupBy(WindowedValue<KV<K, V>> wv) {
      try {
        return ByteBuffer.wrap(CoderUtils.encodeToByteArray(keyCoder, wv.getValue().getKey()));
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** A transform used internally to transform WindowedValue to KV. */
  protected static class KeyedByTimestamp<K, V>
      extends MapFunction<WindowedValue<KV<K, V>>, KV<Instant, WindowedValue<KV<K, V>>>> {

    private final WindowFn<KV<K, V>, BoundedWindow> windowFn;
    private final TimestampCombiner timestampCombiner;

    public KeyedByTimestamp(
        WindowFn<KV<K, V>, BoundedWindow> windowFn, TimestampCombiner timestampCombiner) {
      this.windowFn = windowFn;
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public KV<Instant, WindowedValue<KV<K, V>>> map(WindowedValue<KV<K, V>> wv) {
      BoundedWindow window = Iterables.getOnlyElement(wv.getWindows());
      Instant timestamp =
          timestampCombiner.assign(window, windowFn.getOutputTime(wv.getTimestamp(), window));
      return KV.of(timestamp, wv);
    }
  }

  /** A transform used internally by Gearpump which encapsulates the merge logic. */
  protected static class Merge<K, V>
      extends FoldFunction<
          KV<Instant, WindowedValue<KV<K, V>>>, KV<Instant, WindowedValue<KV<K, List<V>>>>> {

    private final WindowFn<KV<K, V>, BoundedWindow> windowFn;
    private final TimestampCombiner timestampCombiner;

    Merge(WindowFn<KV<K, V>, BoundedWindow> windowFn, TimestampCombiner timestampCombiner) {
      this.windowFn = windowFn;
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public KV<Instant, WindowedValue<KV<K, List<V>>>> init() {
      return KV.of(null, null);
    }

    @Override
    public KV<Instant, WindowedValue<KV<K, List<V>>>> fold(
        KV<Instant, WindowedValue<KV<K, List<V>>>> accum,
        KV<Instant, WindowedValue<KV<K, V>>> iter) {
      if (accum.getKey() == null) {
        WindowedValue<KV<K, V>> wv = iter.getValue();
        KV<K, V> kv = wv.getValue();
        V v = kv.getValue();
        List<V> nv = Lists.newArrayList(v);
        return KV.of(iter.getKey(), wv.withValue(KV.of(kv.getKey(), nv)));
      }

      Instant t1 = accum.getKey();
      Instant t2 = iter.getKey();

      final WindowedValue<KV<K, List<V>>> wv1 = accum.getValue();
      final WindowedValue<KV<K, V>> wv2 = iter.getValue();
      wv1.getValue().getValue().add(wv2.getValue().getValue());

      final List<BoundedWindow> mergedWindows = new ArrayList<>();
      if (!windowFn.isNonMerging()) {
        try {
          windowFn.mergeWindows(
              windowFn.new MergeContext() {

                @Override
                public Collection<BoundedWindow> windows() {
                  ArrayList<BoundedWindow> windows = new ArrayList<>();
                  windows.addAll(wv1.getWindows());
                  windows.addAll(wv2.getWindows());
                  return windows;
                }

                @Override
                public void merge(Collection<BoundedWindow> toBeMerged, BoundedWindow mergeResult)
                    throws Exception {
                  mergedWindows.add(mergeResult);
                }
              });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        mergedWindows.addAll(wv1.getWindows());
      }

      Instant timestamp = timestampCombiner.combine(t1, t2);
      return KV.of(
          timestamp, WindowedValue.of(wv1.getValue(), timestamp, mergedWindows, wv1.getPane()));
    }
  }

  private static class Values<K, V>
      extends MapFunction<
          KV<Instant, WindowedValue<KV<K, List<V>>>>, WindowedValue<KV<K, List<V>>>> {

    @Override
    public WindowedValue<KV<K, List<V>>> map(KV<Instant, WindowedValue<KV<K, List<V>>>> kv) {
      Instant timestamp = kv.getKey();
      WindowedValue<KV<K, List<V>>> wv = kv.getValue();
      return WindowedValue.of(wv.getValue(), timestamp, wv.getWindows(), wv.getPane());
    }
  }
}
