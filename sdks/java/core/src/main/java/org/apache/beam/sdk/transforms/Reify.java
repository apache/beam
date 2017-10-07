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

package org.apache.beam.sdk.transforms;

import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * {@link PTransform PTransforms} for reifying the timestamp, window and pane of values.
 */
public class Reify {
  /**
   * Private implementation of {@link #windows()}.
   */
  private static class Window<T> extends PTransform<PCollection<T>,
      PCollection<ValueInSingleWindow<T>>> {
    private static class Do<T> extends DoFn<T, ValueInSingleWindow<T>> {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) {
        c.outputWithTimestamp(ValueInSingleWindow.of(c.element(), c.timestamp(), window, c.pane()),
            c.timestamp());
      }
    }

    @Override
    public PCollection<ValueInSingleWindow<T>> expand(PCollection<T> input) {
      return input
          .apply(ParDo.of(new Do<T>()))
          .setCoder(ValueInSingleWindow.Coder.of(input.getCoder(),
              input.getWindowingStrategy().getWindowFn().windowCoder()));
    }
  }

  private static class Timestamp<T> extends PTransform<PCollection<T>,
      PCollection<TimestampedValue<T>>> {
    private static class Do<T> extends DoFn<T, TimestampedValue<T>> {
      @ProcessElement
      public void processElement(ProcessContext context) {
        context.output(TimestampedValue.of(context.element(), context.timestamp()));
      }
    }

    @Override
    public PCollection<TimestampedValue<T>> expand(PCollection<T> input) {
      return input.apply(ParDo.of(new Do<T>()));
    }

  }

  private static class WindowInValue<K, V> extends PTransform<PCollection<KV<K, V>>,
      PCollection<KV<K, ValueInSingleWindow<V>>>> {
    private static class Do<K, V> extends DoFn<KV<K, V>, KV<K, ValueInSingleWindow<V>>> {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) {
        c.output(KV.of(c.element().getKey(), ValueInSingleWindow.of(c.element()
            .getValue(), c.timestamp(), window, c.pane())));
      }
    }

    @Override
    public PCollection<KV<K, ValueInSingleWindow<V>>> expand(PCollection<KV<K, V>>
        input) {
      List<? extends Coder<?>> coderArguments = input.getCoder().getCoderArguments();
      Coder<K> keyCoder = (Coder<K>) coderArguments.get(0);
      Coder<V> valueCoder = (Coder<V>) coderArguments.get(1);
      return input
          .apply(ParDo.of(new Do<K, V>()))
          .setCoder(KvCoder.of(keyCoder, ValueInSingleWindow.Coder.of(valueCoder,
              input.getWindowingStrategy().getWindowFn().windowCoder())));
    }
  }


  private static class TimestampInValue<K, V> extends PTransform<PCollection<KV<K, V>>,
      PCollection<KV<K, TimestampedValue<V>>>> {
    private static class Do<K, V> extends DoFn<KV<K, V>, KV<K, TimestampedValue<V>>> {
      @ProcessElement
      public void processElement(ProcessContext context) {
        context.output(KV.of(context.element().getKey(),
            TimestampedValue.of(context.element().getValue(), context.timestamp())));
      }
    }

    @Override
    public PCollection<KV<K, TimestampedValue<V>>> expand(PCollection<KV<K, V>> input) {
      return input.apply(ParDo.of(new Do<K, V>()));
    }
  }

  Reify() {
  }

  /**
   * Create a {@link PTransform} that will output all inputs wrapped in a {@link TimestampedValue}.
   */
  public static <T>
  PTransform<PCollection<T>, PCollection<TimestampedValue<T>>>
  timestamps() {
    return new Timestamp<>();
  }

  /**
   * Create a {@link PTransform} that will output all input {@link KV KVs} with the timestamp inside
   * the value.
   */
  public static <K, V>
  PTransform<PCollection<KV<K, V>>, PCollection<KV<K, TimestampedValue<V>>>>
  timestampsInValue() {
    return new TimestampInValue<>();
  }

  /**
   * Create a {@link PTransform} that will reify information from the processing context into
   * instances of {@link ValueInSingleWindow}.
   *
   * @param <T> element type
   */
  public static <T> PTransform<PCollection<T>, PCollection<ValueInSingleWindow<T>>>
  windows() {
    return new Window<>();
  }

  /**
   * Create a {@link PTransform} that will output all input {@link KV KVs} with the window pane info
   * inside the value.
   */
  public static <K, V>
  PTransform<PCollection<KV<K, V>>, PCollection<KV<K, ValueInSingleWindow<V>>>>
  windowsInValue() {
    return new WindowInValue<>();
  }

}
