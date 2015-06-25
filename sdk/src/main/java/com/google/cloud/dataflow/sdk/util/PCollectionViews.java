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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.InvalidWindows;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValueBase;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Implementations of {@link PCollectionView} shared across the SDK.
 *
 * <p>For internal use only, subject to change.
 */
public class PCollectionViews {

  /**
   * Returns a {@code PCollectionView<T>} capable of processing elements encoded using the provided
   * {@link Coder} and windowed using the provided * {@link WindowingStrategy}.
   *
   * <p>If {@code hasDefault} is {@code true}, then the view will take on the value
   * {@code defaultValue} for any empty windows.
   */
  public static <T, W extends BoundedWindow> PCollectionView<T> singletonView(
      Pipeline pipeline,
      WindowingStrategy<?, W> windowingStrategy,
      boolean hasDefault,
      T defaultValue,
      Coder<T> valueCoder) {
    return new SingletonPCollectionView(
        pipeline, windowingStrategy, hasDefault, defaultValue, valueCoder);
  }

  /**
   * Returns a {@code PCollectionView<Iterable<T>>} capable of processing elements encoded using the
   * provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<Iterable<T>> iterableView(
      Pipeline pipeline,
      WindowingStrategy<?, W> windowingStrategy,
      Coder<T> valueCoder) {
    return new IterablePCollectionView(pipeline, windowingStrategy, valueCoder);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements encoded using the
   * provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, V>> mapView(
      Pipeline pipeline,
      WindowingStrategy<?, W> windowingStrategy,
      Coder<KV<K, V>> valueCoder) {

    return new MapPCollectionView(pipeline, windowingStrategy, valueCoder);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements encoded
   * using the provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>> multimapView(
      Pipeline pipeline,
      WindowingStrategy<?, W> windowingStrategy,
      Coder<KV<K, V>> valueCoder) {
    return new MultimapPCollectionView(pipeline, windowingStrategy, valueCoder);
  }

  /**
   * Implementation of conversion of singleton {@code Iterable<WindowedValue<T>>} to {@code T}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#singletonView}.
   */
  private static class SingletonPCollectionView<T, W extends BoundedWindow>
     extends PCollectionViewBase<T, T, W> {
    private static final long serialVersionUID = 0;
    private byte[] encodedDefaultValue;
    private transient T defaultValue;
    private Coder<T> valueCoder;

    public SingletonPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, W> windowingStrategy,
        boolean hasDefault, T defaultValue, Coder<T> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
      this.defaultValue = defaultValue;
      this.valueCoder = valueCoder;
      if (hasDefault) {
        try {
          this.encodedDefaultValue = CoderUtils.encodeToByteArray(valueCoder, defaultValue);
        } catch (IOException e) {
          throw new RuntimeException("Unexpected IOException: ", e);
        }
      }
    }

    @Override
    protected T fromElements(Iterable<WindowedValue<T>> contents) {
      if (encodedDefaultValue != null && defaultValue == null) {
        try {
          defaultValue = CoderUtils.decodeFromByteArray(valueCoder, encodedDefaultValue);
        } catch (IOException e) {
          throw new RuntimeException("Unexpected IOException: ", e);
        }
      }

      if (encodedDefaultValue != null && !contents.iterator().hasNext()) {
        return defaultValue;
      }
      try {
        return Iterables.getOnlyElement(contents).getValue();
      } catch (NoSuchElementException exc) {
        throw new NoSuchElementException(
            "Empty PCollection accessed as a singleton view.");
      } catch (IllegalArgumentException exc) {
        throw new IllegalArgumentException(
            "PCollection with more than one element "
            + "accessed as a singleton view.");
      }
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<T>>} to {@code Iterable<T>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#iterableView}.
   */
  private static class IterablePCollectionView<T, W extends BoundedWindow>
      extends PCollectionViewBase<T, Iterable<T>, W> {
    private static final long serialVersionUID = 0;

    public IterablePCollectionView(
        Pipeline pipeline, WindowingStrategy<?, W> windowingStrategy, Coder<T> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    @Override
    protected Iterable<T> fromElements(Iterable<WindowedValue<T>> contents) {
      return Iterables.transform(contents, new Function<WindowedValue<T>, T>() {
        @SuppressWarnings("unchecked")
        @Override
        public T apply(WindowedValue<T> input) {
          return input.getValue();
        }
      });
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>>}
   * to {@code Map<K, Iterable<V>>}.
   *
   * <p> For internal use only.
   */
  private static class MultimapPCollectionView<K, V, W extends BoundedWindow>
      extends PCollectionViewBase<KV<K, V>, Map<K, Iterable<V>>, W> {
    private static final long serialVersionUID = 0;

    public MultimapPCollectionView(
        Pipeline pipeline,
        WindowingStrategy<KV<K, V>, W> windowingStrategy,
        Coder<KV<K, V>> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    @Override
    protected Map<K, Iterable<V>> fromElements(Iterable<WindowedValue<KV<K, V>>> elements) {
      Multimap<K, V> multimap = HashMultimap.create();
      for (WindowedValue<KV<K, V>> elem : elements) {
        KV<K, V> kv = elem.getValue();
        multimap.put(kv.getKey(), kv.getValue());
      }
      // Safe covariant cast that Java cannot express without rawtypes, even with unchecked casts
      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<K, Iterable<V>> resultMap = (Map) multimap.asMap();
      return resultMap;
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>} with
   * one value per key to {@code Map<K, V>}.
   *
   * <p> For internal use only.
   */
  private static class MapPCollectionView<K, V, W extends BoundedWindow>
      extends PCollectionViewBase<KV<K, V>, Map<K, V>, W> {
    private static final long serialVersionUID = 0;

    public MapPCollectionView(
        Pipeline pipeline,
        WindowingStrategy<KV<K, V>, W> windowingStrategy,
        Coder<KV<K, V>> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    /**
     * Input iterable must actually be {@code Iterable<WindowedValue<KV<K, V>>>}.
     */
    @Override
    protected Map<K, V> fromElements(Iterable<WindowedValue<KV<K, V>>> elements) {
      Map<K, V> map = new HashMap<>();
      for (WindowedValue<KV<K, V>> elem : elements) {
        KV<K, V> kv = elem.getValue();
        if (map.put(kv.getKey(), kv.getValue()) != null) {
          throw new IllegalArgumentException("Duplicate values for " + kv.getKey());
        }
      }
      return Collections.unmodifiableMap(map);
    }
  }

  /**
   * A base class for {@link PCollectionView} implementations, with additional type parameters
   * that are not visible at pipeline assembly time when the view is used as a side input.
   */
  private abstract static class PCollectionViewBase<ElemT, ViewT, W extends BoundedWindow>
      extends PValueBase
      implements PCollectionView<ViewT> {
    private static final long serialVersionUID = 0L;

    /** A unique tag for the view, typed according to the elements underlying the view. */
    private TupleTag<Iterable<WindowedValue<ElemT>>> tag;

    /** The windowing strategy for the PCollection underlying the view. */
    private WindowingStrategy<?, W> windowingStrategy;

    /** The coder for the elements underlying the view. */
    private Coder<Iterable<WindowedValue<ElemT>>> coder;

    /**
     * Implement this to complete the implementation. It is a conversion function from
     * all of the elements of the underlying {@link PCollection} to the value of the view.
     */
    protected abstract ViewT fromElements(Iterable<WindowedValue<ElemT>> elements);

    /**
     * Call this constructor to initialize the fields for which this base class provides
     * boilerplate accessors.
     */
    protected PCollectionViewBase(
        Pipeline pipeline,
        TupleTag<Iterable<WindowedValue<ElemT>>> tag,
        WindowingStrategy<?, W> windowingStrategy,
        Coder<ElemT> valueCoder) {
      super(pipeline);
      if (windowingStrategy.getWindowFn() instanceof InvalidWindows) {
        throw new IllegalArgumentException("WindowFn of PCollectionView cannot be InvalidWindows");
      }
      this.tag = tag;
      this.windowingStrategy = windowingStrategy;
      this.coder =
          IterableCoder.of(WindowedValue.getFullCoder(
              valueCoder, windowingStrategy.getWindowFn().windowCoder()));
    }

    /**
     * Call this constructor to initialize the fields for which this base class provides
     * boilerplate accessors, with an auto-generated tag.
     */
    protected PCollectionViewBase(
        Pipeline pipeline,
        WindowingStrategy<?, W> windowingStrategy,
        Coder<ElemT> valueCoder) {
      this(pipeline, new TupleTag<Iterable<WindowedValue<ElemT>>>(), windowingStrategy, valueCoder);
    }

    /**
     * For serialization only. Do not use directly. Subclasses should call from their own
     * protected no-argument constructor.
     */
    protected PCollectionViewBase() {
      super();
    }

    @Override
    public ViewT fromIterableInternal(Iterable<WindowedValue<?>> elements) {
      // Safe cast: it is required that the rest of the SDK maintain the invariant
      // that a PCollectionView is only provided an iterable for the elements of an
      // appropriately typed PCollection.
      @SuppressWarnings({"rawtypes", "unchecked"})
      Iterable<WindowedValue<ElemT>> typedElements = (Iterable) elements;
      return fromElements(typedElements);
    }

    /**
     * Returns a unique {@link TupleTag} identifying this {@link PCollectionView}.
     *
     * <p> For internal use only by runner implementors.
     */
    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      // Safe cast: It is required that the rest of the SDK maintain the invariant that
      // this tag is only used to access the contents of an appropriately typed underlying
      // PCollection
      @SuppressWarnings({"rawtypes, unchecked"})
      TupleTag<Iterable<WindowedValue<?>>> untypedTag = (TupleTag) tag;
      return untypedTag;
    }

    /**
     * Returns the {@link WindowingStrategy} of this {@link PCollectionView}, which should
     * be that of the underlying {@link PCollection}.
     *
     * <p> For internal use only by runner implementors.
     */
    @Override
    public WindowingStrategy<?, ?> getWindowingStrategyInternal() {
      return windowingStrategy;
    }

    @Override
    public Coder<Iterable<WindowedValue<?>>> getCoderInternal() {
      // Safe cast: It is required that the rest of the SDK only use this untyped coder
      // for the elements of an appropriately typed underlying PCollection.
      @SuppressWarnings({"rawtypes", "unchecked"})
      Coder<Iterable<WindowedValue<?>>> untypedCoder = (Coder) coder;
      return untypedCoder;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof PCollectionView) || other == null) {
        return false;
      }
      @SuppressWarnings("unchecked")
      PCollectionView<?> otherView = (PCollectionView<?>) other;
      return tag.equals(otherView.getTagInternal());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("tag", tag).toString();
    }
  }
}
