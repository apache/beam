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

package org.apache.beam.sdk.testing;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

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
   * A {@link ViewFn} that returns the provided contents as a fully lazy iterable.
   */
  public static class IdentityViewFn<T> extends ViewFn<Iterable<WindowedValue<T>>, Iterable<T>> {
    @Override
    public Materialization<Iterable<WindowedValue<T>>> getMaterialization() {
      return Materializations.iterable();
    }

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
  public static class LengthViewFn<T> extends ViewFn<Iterable<WindowedValue<T>>, Long> {
    @Override
    public Materialization<Iterable<WindowedValue<T>>> getMaterialization() {
      return Materializations.iterable();
    }

    @Override
    public Long apply(Iterable<WindowedValue<T>> contents) {
      return (long) Iterables.size(contents);
    }
  }

  /**
   * A {@link ViewFn} that always returns the value with which it is instantiated.
   */
  public static class ConstantViewFn<ElemT, ViewT>
      extends ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> {
    private ViewT value;

    public ConstantViewFn(ViewT value) {
      this.value = value;
    }

    @Override
    public Materialization<Iterable<WindowedValue<ElemT>>> getMaterialization() {
      return Materializations.iterable();
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
      ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn,
      Coder<ElemT> elemCoder,
      WindowingStrategy<?, ?> windowingStrategy) {
    return testingView(null, tag, viewFn, elemCoder, windowingStrategy);
  }

  /**
   * The default {@link Coder} used for windowed values, given an element {@link Coder}.
   */
  public static <T> Coder<WindowedValue<T>> defaultWindowedValueCoder(Coder<T> elemCoder) {
    return WindowedValue.getFullCoder(
        elemCoder, DEFAULT_WINDOWING_STRATEGY.getWindowFn().windowCoder());
  }

  /**
   * A {@link PCollectionView} explicitly built from its {@link TupleTag}, {@link
   * WindowingStrategy}, {@link Coder}, and conversion function.
   *
   * <p>This method is only recommended for use by runner implementors to test their
   * implementations. It is very easy to construct a {@link PCollectionView} that does not respect
   * the invariants required for proper functioning.
   *
   * <p>Note that if the provided {@code WindowingStrategy} does not match that of the windowed
   * values provided to the view during execution, results are unpredictable.
   */
  public static <ElemT, ViewT> PCollectionView<ViewT> testingView(
      PCollection<ElemT> pCollection,
      TupleTag<Iterable<WindowedValue<ElemT>>> tag,
      ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn,
      Coder<ElemT> elemCoder,
      WindowingStrategy<?, ?> windowingStrategy) {
    return testingView(
        pCollection,
        tag,
        viewFn,
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        elemCoder,
        windowingStrategy);
  }

  /**
   * A {@link PCollectionView} explicitly built from its {@link TupleTag}, {@link
   * WindowingStrategy}, {@link Coder}, {@link ViewFn} and {@link WindowMappingFn}.
   *
   * <p>This method is only recommended for use by runner implementors to test their
   * implementations. It is very easy to construct a {@link PCollectionView} that does not respect
   * the invariants required for proper functioning.
   *
   * <p>Note that if the provided {@code WindowingStrategy} does not match that of the windowed
   * values provided to the view during execution, results are unpredictable.
   */
  public static <ElemT, ViewT> PCollectionView<ViewT> testingView(
      PCollection<ElemT> pCollection,
      TupleTag<Iterable<WindowedValue<ElemT>>> tag,
      ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn,
      WindowMappingFn<?> windowMappingFn,
      Coder<ElemT> elemCoder,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new PCollectionViewFromParts<>(
        pCollection,
        tag,
        viewFn,
        windowMappingFn,
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
    private PCollection<ElemT> pCollection;
    private TupleTag<Iterable<WindowedValue<ElemT>>> tag;
    private ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn;
    private WindowMappingFn<?> windowMappingFn;
    private WindowingStrategy<?, ?> windowingStrategy;
    private Coder<Iterable<WindowedValue<ElemT>>> coder;

    public PCollectionViewFromParts(
        PCollection<ElemT> pCollection,
        TupleTag<Iterable<WindowedValue<ElemT>>> tag,
        ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn,
        WindowMappingFn<?> windowMappingFn,
        WindowingStrategy<?, ?> windowingStrategy,
        Coder<Iterable<WindowedValue<ElemT>>> coder) {
      this.pCollection = pCollection;
      this.tag = tag;
      this.viewFn = viewFn;
      this.windowMappingFn = windowMappingFn;
      this.windowingStrategy = windowingStrategy;
      this.coder = coder;
    }

    @Override
    public PCollection<?> getPCollection() {
      return pCollection;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      return (TupleTag) tag;
    }

    @Override
    public ViewFn<Iterable<WindowedValue<?>>, ViewT> getViewFn() {
      // Safe cast; runners must maintain type safety
      @SuppressWarnings({"unchecked", "rawtypes"})
      ViewFn<Iterable<WindowedValue<?>>, ViewT> untypedViewFn = (ViewFn) viewFn;
      return untypedViewFn;
    }

    @Override
    public WindowMappingFn<?> getWindowMappingFn() {
      return windowMappingFn;
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
