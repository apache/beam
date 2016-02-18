/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Collection;

/**
 * A {@link PTransform} that returns a {@link PCollection} equivalent to its input but operationally
 * provides some of the side effects of a {@link GroupByKey}, in particular preventing fusion of
 * the surrounding transforms, checkpointing and deduplication by id (see
 * {@link ValueWithRecordId}).
 *
 * <p>Performs a {@link GroupByKey} so that the data is key-partitioned. Configures the
 * {@link WindowingStrategy} so that no data is dropped, but doesn't affect the need for
 * the user to specify allowed lateness and accumulation mode before a user-inserted GroupByKey.
 *
 * @param <K> The type of key being reshuffled on.
 * @param <V> The type of value being reshuffled.
 */
public class Reshuffle<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

  private Reshuffle() {
  }

  public static <K, V> Reshuffle<K, V> of() {
    return new Reshuffle<K, V>();
  }

  @Override
  public PCollection<KV<K, V>> apply(PCollection<KV<K, V>> input) {
    WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
    // If the input has already had its windows merged, then the GBK that performed the merge
    // will have set originalStrategy.getWindowFn() to InvalidWindows, causing the GBK contained
    // here to fail. Instead, we install a valid WindowFn that leaves all windows unchanged.
    Window.Bound<KV<K, V>> rewindow = Window
        .<KV<K, V>>into(new PassThroughWindowFn<>(originalStrategy.getWindowFn()))
        .triggering(new ReshuffleTrigger<>())
        .discardingFiredPanes()
        .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    return input.apply(rewindow)
        .apply(GroupByKey.<K, V>create())
        // Set the windowing strategy directly, so that it doesn't get counted as the user having
        // set allowed lateness.
        .setWindowingStrategyInternal(originalStrategy)
        .apply(ParDo.named("ExpandIterable").of(
            new DoFn<KV<K, Iterable<V>>, KV<K, V>>() {
              @Override
              public void processElement(ProcessContext c) {
                K key = c.element().getKey();
                for (V value : c.element().getValue()) {
                  c.output(KV.of(key, value));
                }
              }
            }));
  }

  /**
   * A {@link WindowFn} that leaves all associations between elements and windows unchanged.
   *
   * <p>In order to implement all the abstract methods of {@link WindowFn}, this requires the
   * prior {@link WindowFn}, to which all auxiliary functionality is delegated.
   */
  private static class PassThroughWindowFn<T> extends NonMergingWindowFn<T, BoundedWindow> {

    /** The WindowFn prior to this. Used for its windowCoder, etc. */
    private final WindowFn<?, BoundedWindow> priorWindowFn;

    public PassThroughWindowFn(WindowFn<?, ?> priorWindowFn) {
      // Safe because it is only used privately here.
      // At every point where a window is returned or accepted, it has been provided
      // by priorWindowFn, so it is of the type expected.
      @SuppressWarnings("unchecked")
      WindowFn<?, BoundedWindow> internalWindowFn = (WindowFn<?, BoundedWindow>) priorWindowFn;
      this.priorWindowFn = internalWindowFn;
    }

    @Override
    public Collection<BoundedWindow> assignWindows(WindowFn<T, BoundedWindow>.AssignContext c)
        throws Exception {
      // The windows are provided by priorWindowFn, which also provides the coder for them
      @SuppressWarnings("unchecked")
      Collection<BoundedWindow> priorWindows = (Collection<BoundedWindow>) c.windows();
      return priorWindows;
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      throw new UnsupportedOperationException(
          String.format("%s.isCompatible() should never be called."
              + " It is a private implementation detail of Reshuffle."
              + " This message indicates a bug in the Dataflow SDK.",
              getClass().getCanonicalName()));
    }

    @Override
    public Coder<BoundedWindow> windowCoder() {
      // Safe because priorWindowFn provides the windows also.
      // The Coder is _not_ actually a coder for an arbitrary BoundedWindow.
      return priorWindowFn.windowCoder();
    }

    @Override
    public BoundedWindow getSideInputWindow(BoundedWindow window) {
      throw new UnsupportedOperationException(
          String.format("%s.getSideInputWindow() should never be called."
              + " It is a private implementation detail of Reshuffle."
              + " This message indicates a bug in the Dataflow SDK.",
              getClass().getCanonicalName()));
    }

    @Override
    public Instant getOutputTime(Instant inputTimestamp, BoundedWindow window) {
      return inputTimestamp;
    }
  }
}
