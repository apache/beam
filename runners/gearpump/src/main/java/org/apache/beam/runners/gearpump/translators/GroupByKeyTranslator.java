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
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
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
import org.apache.gearpump.streaming.dsl.window.api.Window;
import org.apache.gearpump.streaming.dsl.window.api.WindowFn;
import org.apache.gearpump.streaming.dsl.window.impl.Bucket;
import scala.collection.JavaConversions;


/**
 * {@link GroupByKey} is translated to Gearpump groupBy function.
 */
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
    JavaStream<WindowedValue<KV<K, Iterable<V>>>> outputStream = inputStream
        .window(Window.apply(new GearpumpWindowFn(input.getWindowingStrategy().getWindowFn()),
            EventTimeTrigger$.MODULE$, Discarding$.MODULE$), "assign_window")
        .groupBy(new GroupByFn<K, V>(inputKeyCoder), parallelism, "group_by_Key_and_Window")
        .map(new ValueToIterable<K, V>(), "map_value_to_iterable")
        .map(new KeyedByTimestamp<K, V>(), "keyed_by_timestamp")
        .reduce(new Merge<K, V>(outputTimeFn), "merge")
        .map(new Values<K, V>(), "values");

    context.setOutputStream(context.getOutput(transform), outputStream);
  }

  private static class GearpumpWindowFn<T, W extends BoundedWindow> implements WindowFn,
      Serializable {

    private org.apache.beam.sdk.transforms.windowing.WindowFn<T, W> windowFn;

    GearpumpWindowFn(org.apache.beam.sdk.transforms.windowing.WindowFn<T, W> windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    public scala.collection.immutable.List<Bucket> apply(final Instant timestamp) {
      try {
        Collection<W> windows = windowFn.assignWindows(windowFn.new AssignContext() {
          @Override
          public T element() {
            throw new UnsupportedOperationException();
          }

          @Override
          public org.joda.time.Instant timestamp() {
            return TranslatorUtils.java8TimeToJodaTime(timestamp);
          }

          @Override
          public W window() {
            throw new UnsupportedOperationException();
          }
        });

        List<Bucket> buckets = new LinkedList<>();
        for (BoundedWindow window : windows) {
          buckets.add(getBucket(window));
        }
        return JavaConversions.asScalaBuffer(buckets).toList();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private Bucket getBucket(BoundedWindow window) {
      if (window instanceof IntervalWindow) {
        IntervalWindow intervalWindow = (IntervalWindow) window;
        Instant start = TranslatorUtils.jodaTimeToJava8Time(intervalWindow.start());
        Instant end = TranslatorUtils.jodaTimeToJava8Time(intervalWindow.end());
        return new Bucket(start, end);
      } else if (window instanceof GlobalWindow) {
        Instant end = TranslatorUtils.jodaTimeToJava8Time(window.maxTimestamp());
        return new Bucket(Instant.MIN, end);
      } else {
        throw new RuntimeException("unknown window " + window.getClass().getName());
      }
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

    @Override
    public KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> apply(
        WindowedValue<KV<K, Iterable<V>>> wv) {
      return KV.of(wv.getTimestamp(), wv);
    }
  }

  private static class Merge<K, V> extends
      ReduceFunction<KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>>> {

    private final OutputTimeFn<? super BoundedWindow> outputTimeFn;

    Merge(OutputTimeFn<? super BoundedWindow> outputTimeFn) {
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> apply(
        KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> kv1,
        KV<org.joda.time.Instant, WindowedValue<KV<K, Iterable<V>>>> kv2) {
      org.joda.time.Instant t1 = kv1.getKey();
      org.joda.time.Instant t2 = kv2.getKey();

      WindowedValue<KV<K, Iterable<V>>> wv1 = kv1.getValue();
      WindowedValue<KV<K, Iterable<V>>> wv2 = kv2.getValue();

      return KV.of(outputTimeFn.combine(t1, t2),
          WindowedValue.of(KV.of(wv1.getValue().getKey(),
              Iterables.concat(wv1.getValue().getValue(), wv2.getValue().getValue())),
              wv1.getTimestamp(), wv1.getWindows(), wv1.getPane()));
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
