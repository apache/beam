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

package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValueBase;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;
import java.util.Objects;

/**
 * Methods for creating and using {@link PCollectionView} instances.
 */
public final class PCollectionViewTesting {

  // Do not instantiate; static methods only
  private PCollectionViewTesting() { }

  /**
   * The length of the default window, which is an {@link IntervalWindow}, but kept encapsulated
   * as it is not for the user to know what sort of window it is.
   */
  private static final long DEFAULT_WINDOW_MSECS = 1000 * 60 * 60;

  /**
   * A default windowing strategy. Tests that are not concerned with the windowing
   * strategy should not specify it, and all views will use this.
   */
  public static final WindowingStrategy<?, ?> DEFAULT_WINDOWING_STRATEGY =
      WindowingStrategy.of(FixedWindows.of(new Duration(DEFAULT_WINDOW_MSECS)));

  /**
   * A default window into which test elements will be placed, if the window is
   * not explicitly overridden.
   */
  public static final BoundedWindow DEFAULT_NONEMPTY_WINDOW =
      new IntervalWindow(new Instant(0), new Instant(DEFAULT_WINDOW_MSECS));

  /**
   * A timestamp in the {@link #DEFAULT_NONEMPTY_WINDOW}.
   */
  public static final Instant DEFAULT_TIMESTAMP = DEFAULT_NONEMPTY_WINDOW.maxTimestamp().minus(1);

  /**
   * A window into which no element will be placed by methods in this class, unless explicitly
   * requested.
   */
  public static final BoundedWindow DEFAULT_EMPTY_WINDOW = new IntervalWindow(
      DEFAULT_NONEMPTY_WINDOW.maxTimestamp(),
      DEFAULT_NONEMPTY_WINDOW.maxTimestamp().plus(DEFAULT_WINDOW_MSECS));

  /**
   * A specialization of {@link SerializableFunction} just for putting together
   * {@link PCollectionView} instances.
   */
  public static interface ViewFn<ElemT, ViewT>
      extends SerializableFunction<Iterable<WindowedValue<ElemT>>, ViewT> { }

  /**
   * A {@link ViewFn} that returns the provided contents as a fully lazy iterable.
   */
  public static class IdentityViewFn<T> implements ViewFn<T, Iterable<T>> {
    @Override
    public Iterable<T> apply(Iterable<WindowedValue<T>> contents) {
      return Iterables.transform(contents, new Function<WindowedValue<T>, T>() {
        @Override
        public T apply(WindowedValue<T> windowedValue) {
          return windowedValue.getValue();
        }
      });
    }
  }

  /**
   * A {@link ViewFn} that traverses the whole iterable eagerly and returns the number of elements.
   *
   * <p>Only for use in testing scenarios with small collections. If there are more elements
   * provided than {@code Integer.MAX_VALUE} then behavior is unpredictable.
   */
  public static class LengthViewFn<T> implements ViewFn<T, Long> {
    @Override
    public Long apply(Iterable<WindowedValue<T>> contents) {
      return (long) Iterables.size(contents);
    }
  }

  /**
   * A {@link ViewFn} that always returns the value with which it is instantiated.
   */
  public static class ConstantViewFn<ElemT, ViewT> implements ViewFn<ElemT, ViewT> {
    private ViewT value;

    public ConstantViewFn(ViewT value) {
      this.value = value;
    }

    @Override
    public ViewT apply(Iterable<WindowedValue<ElemT>> contents) {
      return value;
    }
  }

  /**
   * A {@link PCollectionView} explicitly built from a {@link TupleTag}
   * and conversion {@link ViewFn}, and an element coder, using the
   * {@link #DEFAULT_WINDOWING_STRATEGY}.
   *
   * <p>This method is only recommended for use by runner implementors to test their
   * implementations. It is very easy to construct a {@link PCollectionView} that does
   * not respect the invariants required for proper functioning.
   *
   * <p>Note that if the provided {@code WindowingStrategy} does not match that of the windowed
   * values provided to the view during execution, results are unpredictable. It is recommended
   * that the values be prepared via {@link #contentsInDefaultWindow}.
   */
  public static <ElemT, ViewT> PCollectionView<ViewT> testingView(
      TupleTag<Iterable<WindowedValue<ElemT>>> tag,
      ViewFn<ElemT, ViewT> viewFn,
      Coder<ElemT> elemCoder) {
    return testingView(
        tag,
        viewFn,
        elemCoder,
        DEFAULT_WINDOWING_STRATEGY);
  }

  /**
   * The default {@link Coder} used for windowed values, given an element {@link Coder}.
   */
  public static <T> Coder<WindowedValue<T>> defaultWindowedValueCoder(Coder<T> elemCoder) {
    return WindowedValue.getFullCoder(
        elemCoder, DEFAULT_WINDOWING_STRATEGY.getWindowFn().windowCoder());
  }

  /**
   * A {@link PCollectionView} explicitly built from its {@link TupleTag},
   * {@link WindowingStrategy}, {@link Coder}, and conversion function.
   *
   * <p>This method is only recommended for use by runner implementors to test their
   * implementations. It is very easy to construct a {@link PCollectionView} that does
   * not respect the invariants required for proper functioning.
   *
   * <p>Note that if the provided {@code WindowingStrategy} does not match that of the windowed
   * values provided to the view during execution, results are unpredictable.
   */
  public static <ElemT, ViewT> PCollectionView<ViewT> testingView(
      TupleTag<Iterable<WindowedValue<ElemT>>> tag,
      ViewFn<ElemT, ViewT> viewFn,
      Coder<ElemT> elemCoder,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new PCollectionViewFromParts<>(
        tag,
        viewFn,
        windowingStrategy,
        IterableCoder.of(
            WindowedValue.getFullCoder(elemCoder, windowingStrategy.getWindowFn().windowCoder())));
  }

  /**
   * Places the given {@code value} in the {@link #DEFAULT_NONEMPTY_WINDOW}.
   */
  public static <T> WindowedValue<T> valueInDefaultWindow(T value) {
    return WindowedValue.of(value, DEFAULT_TIMESTAMP, DEFAULT_NONEMPTY_WINDOW, PaneInfo.NO_FIRING);
  }

  /**
   * Prepares {@code values} for reading as the contents of a {@link PCollectionView} side input.
   */
  @SafeVarargs
  public static <T> Iterable<WindowedValue<T>> contentsInDefaultWindow(T... values)
      throws Exception {
    List<WindowedValue<T>> windowedValues = Lists.newArrayList();
    for (T value : values) {
      windowedValues.add(valueInDefaultWindow(value));
    }
    return windowedValues;
  }

  /**
   * Prepares {@code values} for reading as the contents of a {@link PCollectionView} side input.
   */
  public static <T> Iterable<WindowedValue<T>> contentsInDefaultWindow(Iterable<T> values)
      throws Exception {
    List<WindowedValue<T>> windowedValues = Lists.newArrayList();
    for (T value : values) {
      windowedValues.add(valueInDefaultWindow(value));
    }
    return windowedValues;
  }

  // Internal details below here

  /**
   * A {@link PCollectionView} explicitly built from its {@link TupleTag},
   * {@link WindowingStrategy}, and conversion function.
   *
   * <p>Instantiate via {@link #testingView}.
   */
  private static class PCollectionViewFromParts<ElemT, ViewT>
      extends PValueBase
      implements PCollectionView<ViewT> {
    private TupleTag<Iterable<WindowedValue<ElemT>>> tag;
    private ViewFn<ElemT, ViewT> viewFn;
    private WindowingStrategy<?, ?> windowingStrategy;
    private Coder<Iterable<WindowedValue<ElemT>>> coder;

    public PCollectionViewFromParts(
        TupleTag<Iterable<WindowedValue<ElemT>>> tag,
        ViewFn<ElemT, ViewT> viewFn,
        WindowingStrategy<?, ?> windowingStrategy,
        Coder<Iterable<WindowedValue<ElemT>>> coder) {
      this.tag = tag;
      this.viewFn = viewFn;
      this.windowingStrategy = windowingStrategy;
      this.coder = coder;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      return (TupleTag) tag;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public ViewT fromIterableInternal(Iterable<WindowedValue<?>> contents) {
      return (ViewT) viewFn.apply((Iterable) contents);
    }

    @Override
    public WindowingStrategy<?, ?> getWindowingStrategyInternal() {
      return windowingStrategy;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Coder<Iterable<WindowedValue<?>>> getCoderInternal() {
      return (Coder) coder;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof PCollectionView)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      PCollectionView<?> otherView = (PCollectionView<?>) other;
      return tag.equals(otherView.getTagInternal());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("tag", tag)
          .add("viewFn", viewFn)
          .toString();
    }
  }
}
