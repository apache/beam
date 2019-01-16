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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link PTransform PTransforms} for converting between explicit and implicit form of various Beam
 * values.
 */
public class Reify {
  private static class ReifyView<K, V> extends PTransform<PCollection<K>, PCollection<KV<K, V>>> {
    private final PCollectionView<V> view;
    private final Coder<V> coder;

    private ReifyView(PCollectionView<V> view, Coder<V> coder) {
      this.view = view;
      this.coder = coder;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<K> input) {
      return input
          .apply(
              ParDo.of(
                      new DoFn<K, KV<K, V>>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                          c.output(KV.of(c.element(), c.sideInput(view)));
                        }
                      })
                  .withSideInputs(view))
          .setCoder(KvCoder.of(input.getCoder(), coder));
    }
  }

  private static class ReifyViewInGlobalWindow<V> extends PTransform<PBegin, PCollection<V>> {
    private final PCollectionView<V> view;
    private final Coder<V> coder;

    private ReifyViewInGlobalWindow(PCollectionView<V> view, Coder<V> coder) {
      this.view = view;
      this.coder = coder;
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      return input
          .apply(Create.of((Void) null).withCoder(VoidCoder.of()))
          .apply(Reify.viewAsValues(view, coder))
          .apply(Values.create());
    }
  }

  /** Private implementation of {@link #windows()}. */
  private static class Window<T>
      extends PTransform<PCollection<T>, PCollection<ValueInSingleWindow<T>>> {
    @Override
    public PCollection<ValueInSingleWindow<T>> expand(PCollection<T> input) {
      return input
          .apply(
              ParDo.of(
                  new DoFn<T, ValueInSingleWindow<T>>() {
                    @ProcessElement
                    public void processElement(
                        @Element T element,
                        @Timestamp Instant timestamp,
                        BoundedWindow window,
                        PaneInfo pane,
                        OutputReceiver<ValueInSingleWindow<T>> r) {
                      r.outputWithTimestamp(
                          ValueInSingleWindow.of(element, timestamp, window, pane), timestamp);
                    }
                  }))
          .setCoder(
              ValueInSingleWindow.Coder.of(
                  input.getCoder(), input.getWindowingStrategy().getWindowFn().windowCoder()));
    }
  }

  private static class Timestamp<T>
      extends PTransform<PCollection<T>, PCollection<TimestampedValue<T>>> {
    @Override
    public PCollection<TimestampedValue<T>> expand(PCollection<T> input) {
      return input
          .apply(
              ParDo.of(
                  new DoFn<T, TimestampedValue<T>>() {
                    @ProcessElement
                    public void processElement(
                        @Element T element,
                        @Timestamp Instant timestamp,
                        OutputReceiver<TimestampedValue<T>> r) {
                      r.output(TimestampedValue.of(element, timestamp));
                    }
                  }))
          .setCoder(TimestampedValueCoder.of(input.getCoder()));
    }
  }

  private static class WindowInValue<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, ValueInSingleWindow<V>>>> {
    @Override
    public PCollection<KV<K, ValueInSingleWindow<V>>> expand(PCollection<KV<K, V>> input) {
      KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
      return input
          .apply(
              ParDo.of(
                  new DoFn<KV<K, V>, KV<K, ValueInSingleWindow<V>>>() {
                    @ProcessElement
                    public void processElement(
                        @Element KV<K, V> element,
                        @Timestamp Instant timestamp,
                        BoundedWindow window,
                        PaneInfo pane,
                        OutputReceiver<KV<K, ValueInSingleWindow<V>>> r) {
                      r.output(
                          KV.of(
                              element.getKey(),
                              ValueInSingleWindow.of(element.getValue(), timestamp, window, pane)));
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  coder.getKeyCoder(),
                  ValueInSingleWindow.Coder.of(
                      coder.getValueCoder(),
                      input.getWindowingStrategy().getWindowFn().windowCoder())));
    }
  }

  private static class TimestampInValue<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, TimestampedValue<V>>>> {
    @Override
    public PCollection<KV<K, TimestampedValue<V>>> expand(PCollection<KV<K, V>> input) {
      KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
      return input
          .apply(
              ParDo.of(
                  new DoFn<KV<K, V>, KV<K, TimestampedValue<V>>>() {
                    @ProcessElement
                    public void processElement(
                        @Element KV<K, V> element,
                        @Timestamp Instant timestamp,
                        OutputReceiver<KV<K, TimestampedValue<V>>> r) {
                      r.output(
                          KV.of(
                              element.getKey(),
                              TimestampedValue.of(element.getValue(), timestamp)));
                    }
                  }))
          .setCoder(
              KvCoder.of(coder.getKeyCoder(), TimestampedValueCoder.of(coder.getValueCoder())));
    }
  }

  private static class ExtractTimestampsFromValues<K, V>
      extends PTransform<PCollection<KV<K, TimestampedValue<V>>>, PCollection<KV<K, V>>> {
    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, TimestampedValue<V>>> input) {
      KvCoder<K, TimestampedValue<V>> kvCoder = (KvCoder<K, TimestampedValue<V>>) input.getCoder();
      TimestampedValueCoder<V> tvCoder = (TimestampedValueCoder<V>) kvCoder.getValueCoder();
      return input
          .apply(
              ParDo.of(
                  new DoFn<KV<K, TimestampedValue<V>>, KV<K, V>>() {
                    @Override
                    public Duration getAllowedTimestampSkew() {
                      return Duration.millis(Long.MAX_VALUE);
                    }

                    @ProcessElement
                    public void processElement(
                        @Element KV<K, TimestampedValue<V>> kv, OutputReceiver<KV<K, V>> r) {
                      r.outputWithTimestamp(
                          KV.of(kv.getKey(), kv.getValue().getValue()),
                          kv.getValue().getTimestamp());
                    }
                  }))
          .setCoder(KvCoder.of(kvCoder.getKeyCoder(), tvCoder.getValueCoder()));
    }
  }

  private Reify() {}

  /**
   * Create a {@link PTransform} that will output all inputs wrapped in a {@link TimestampedValue}.
   */
  public static <T> PTransform<PCollection<T>, PCollection<TimestampedValue<T>>> timestamps() {
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
  public static <T> PTransform<PCollection<T>, PCollection<ValueInSingleWindow<T>>> windows() {
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

  /** Extracts the timestamps from each value in a {@link KV}. */
  public static <K, V>
      PTransform<PCollection<KV<K, TimestampedValue<V>>>, PCollection<KV<K, V>>>
          extractTimestampsFromValues() {
    return new ExtractTimestampsFromValues<>();
  }

  /**
   * Pairs each element in a collection with the value of a side input associated with the element's
   * window.
   */
  public static <K, V> PTransform<PCollection<K>, PCollection<KV<K, V>>> viewAsValues(
      PCollectionView<V> view, Coder<V> coder) {
    return new ReifyView<>(view, coder);
  }

  /**
   * Returns a {@link PCollection} consisting of a single element, containing the value of the given
   * view in the global window.
   */
  public static <K, V> PTransform<PBegin, PCollection<V>> viewInGlobalWindow(
      PCollectionView<V> view, Coder<V> coder) {
    return new ReifyViewInGlobalWindow<>(view, coder);
  }
}
