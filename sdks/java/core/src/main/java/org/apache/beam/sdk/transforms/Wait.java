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

import static org.apache.beam.sdk.transforms.Contextful.fn;
import static org.apache.beam.sdk.transforms.Requirements.requiresSideInputs;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Delays processing of each window in a {@link PCollection} until signaled.
 *
 * <p>Given a main {@link PCollection} and a signal {@link PCollection}, produces output identical
 * to its main input, but all elements for a window are produced only once that window is closed in
 * the signal {@link PCollection}.
 *
 * <p>To express the pattern "apply T to X after Y is ready", use {@code
 * X.apply(Wait.on(Y)).apply(T)}.
 *
 * <p>In particular: returns a {@link PCollection} with contents identical to the input, but delays
 * producing elements of the output in window W until the signal's window W closes (i.e. signal's
 * watermark passes W.end + signal.allowedLateness).
 *
 * <p>In other words, an element of the output at timestamp "t" will be produced only after no more
 * elements of the signal can appear with a timestamp below "t".
 *
 * <p>Example usage: write a {@link PCollection} to one database and then to another database,
 * making sure that writing a window of data to the second database starts only after the respective
 * window has been fully written to the first database.
 *
 * <pre>{@code
 * PCollection<Void> firstWriteResults = data.apply(ParDo.of(...write to first database...));
 * data.apply(Wait.on(firstWriteResults))
 *     // Windows of this intermediate PCollection will be processed no earlier than when
 *     // the respective window of firstWriteResults closes.
 *     .apply(ParDo.of(...write to second database...));
 * }</pre>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>If signal is globally windowed, main input must also be. This typically would be useful
 *       only in a batch pipeline, because the global window of an infinite PCollection never
 *       closes, so the wait signal will never be ready.
 *   <li>Beware that if the signal has large allowed lateness, the wait signal will fire only after
 *       that lateness elapses, i.e. after the watermark of the signal passes end of the window plus
 *       allowed lateness. In other words: do not use this with signals that specify a large allowed
 *       lateness.
 * </ul>
 */
@Experimental
public class Wait {
  /** Waits on the given signal collections. */
  public static <T> OnSignal<T> on(PCollection<?>... signals) {
    return on(Arrays.asList(signals));
  }

  /** Waits on the given signal collections. */
  public static <T> OnSignal<T> on(List<PCollection<?>> signals) {
    return new OnSignal<>(signals);
  }

  /** Implementation of {@link #on}. */
  public static class OnSignal<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final transient List<PCollection<?>> signals;

    private OnSignal(List<PCollection<?>> signals) {
      this.signals = signals;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      List<PCollectionView<?>> views = Lists.newArrayList();
      for (int i = 0; i < signals.size(); ++i) {
        views.add(signals.get(i).apply("To wait view " + i, new ToWaitView()));
      }

      return input.apply(
          "Wait",
          MapElements.into(input.getCoder().getEncodedTypeDescriptor())
              .via(fn((t, c) -> t, requiresSideInputs(views))));
    }
  }

  private static class ToWaitView extends PTransform<PCollection<?>, PCollectionView<?>> {
    @Override
    public PCollectionView<?> expand(PCollection<?> input) {
      return expandTyped(input);
    }

    private <SignalT> PCollectionView<?> expandTyped(PCollection<SignalT> input) {
      return input
          .apply(Window.<SignalT>configure().triggering(Never.ever()).discardingFiredPanes())
          // Perform a per-window pre-combine so that our performance does not critically depend
          // on combiner lifting.
          .apply(ParDo.of(new CollectWindowsFn<>()))
          .apply(Sample.any(1))
          .apply(View.asList());
    }
  }

  private static class CollectWindowsFn<T> extends DoFn<T, Void> {
    private @Nullable Set<BoundedWindow> windows;

    @StartBundle
    public void startBundle() {
      windows = Sets.newHashSetWithExpectedSize(1);
    }

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow w) {
      windows.add(w);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      for (BoundedWindow w : windows) {
        c.output(null, w.maxTimestamp(), w);
      }
    }
  }
}
