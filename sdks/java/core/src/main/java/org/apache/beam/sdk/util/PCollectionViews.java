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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import javax.annotation.Nullable;

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
    return new SingletonPCollectionView<>(
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
    return new IterablePCollectionView<>(pipeline, windowingStrategy, valueCoder);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements encoded using the
   * provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listView(
      Pipeline pipeline,
      WindowingStrategy<?, W> windowingStrategy,
      Coder<T> valueCoder) {
    return new ListPCollectionView<>(pipeline, windowingStrategy, valueCoder);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements encoded using the
   * provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, V>> mapView(
      Pipeline pipeline,
      WindowingStrategy<?, W> windowingStrategy,
      Coder<KV<K, V>> valueCoder) {

    return new MapPCollectionView<K, V, W>(pipeline, windowingStrategy, valueCoder);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements encoded
   * using the provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>> multimapView(
      Pipeline pipeline,
      WindowingStrategy<?, W> windowingStrategy,
      Coder<KV<K, V>> valueCoder) {
    return new MultimapPCollectionView<K, V, W>(pipeline, windowingStrategy, valueCoder);
  }

  /**
   * Implementation of conversion of singleton {@code Iterable<WindowedValue<T>>} to {@code T}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#singletonView}.
   */
  public static class SingletonPCollectionView<T, W extends BoundedWindow>
     extends PCollectionViewBase<T, T, W> {
    @Nullable private byte[] encodedDefaultValue;
    @Nullable private transient T defaultValue;
    @Nullable private Coder<T> valueCoder;
    private boolean hasDefault;

    private SingletonPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, W> windowingStrategy,
        boolean hasDefault, T defaultValue, Coder<T> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
      this.hasDefault = hasDefault;
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

    /**
     * Returns the default value that was specified.
     *
     * <p>For internal use only.
     *
     * @throws NoSuchElementException if no default was specified.
     */
    public T getDefaultValue() {
      if (!hasDefault) {
        throw new NoSuchElementException("Empty PCollection accessed as a singleton view.");
      }
      // Lazily decode the default value once
      synchronized (this) {
        if (encodedDefaultValue != null) {
          try {
            defaultValue = CoderUtils.decodeFromByteArray(valueCoder, encodedDefaultValue);
            encodedDefaultValue = null;
          } catch (IOException e) {
            throw new RuntimeException("Unexpected IOException: ", e);
          }
        }
        return defaultValue;
      }
    }

    @Override
    protected T fromElements(Iterable<WindowedValue<T>> contents) {
      try {
        return Iterables.getOnlyElement(contents).getValue();
      } catch (NoSuchElementException exc) {
        return getDefaultValue();
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
  public static class IterablePCollectionView<T, W extends BoundedWindow>
      extends PCollectionViewBase<T, Iterable<T>, W> {
    private IterablePCollectionView(
        Pipeline pipeline, WindowingStrategy<?, W> windowingStrategy, Coder<T> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    @Override
    protected Iterable<T> fromElements(Iterable<WindowedValue<T>> contents) {
      return Iterables.unmodifiableIterable(
          Iterables.transform(contents, new Function<WindowedValue<T>, T>() {
        @SuppressWarnings("unchecked")
        @Override
        public T apply(WindowedValue<T> input) {
          return input.getValue();
        }
      }));
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<T>>} to {@code List<T>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#listView}.
   */
  public static class ListPCollectionView<T, W extends BoundedWindow>
      extends PCollectionViewBase<T, List<T>, W> {
    private ListPCollectionView(
        Pipeline pipeline, WindowingStrategy<?, W> windowingStrategy, Coder<T> valueCoder) {
      super(pipeline, windowingStrategy, valueCoder);
    }

    @Override
    protected List<T> fromElements(Iterable<WindowedValue<T>> contents) {
      return ImmutableList.copyOf(
          Iterables.transform(contents, new Function<WindowedValue<T>, T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T apply(WindowedValue<T> input) {
              return input.getValue();
            }
          }));
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>>}
   * to {@code Map<K, Iterable<V>>}.
   *
   * <p>For internal use only.
   */
  public static class MultimapPCollectionView<K, V, W extends BoundedWindow>
      extends PCollectionViewBase<KV<K, V>, Map<K, Iterable<V>>, W> {
    private MultimapPCollectionView(
        Pipeline pipeline,
        WindowingStrategy<?, W> windowingStrategy,
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
      return Collections.unmodifiableMap(resultMap);
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>} with
   * one value per key to {@code Map<K, V>}.
   *
   * <p>For internal use only.
   */
  public static class MapPCollectionView<K, V, W extends BoundedWindow>
      extends PCollectionViewBase<KV<K, V>, Map<K, V>, W> {
    private MapPCollectionView(
        Pipeline pipeline,
        WindowingStrategy<?, W> windowingStrategy,
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
        if (map.containsKey(kv.getKey())) {
          throw new IllegalArgumentException("Duplicate values for " + kv.getKey());
        }
        map.put(kv.getKey(), kv.getValue());
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
    @SuppressWarnings("unused")  // used for serialization
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
     * <p>For internal use only by runner implementors.
     */
    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      // Safe cast: It is required that the rest of the SDK maintain the invariant that
      // this tag is only used to access the contents of an appropriately typed underlying
      // PCollection
      @SuppressWarnings({"rawtypes", "unchecked"})
      TupleTag<Iterable<WindowedValue<?>>> untypedTag = (TupleTag) tag;
      return untypedTag;
    }

    /**
     * Returns the {@link WindowingStrategy} of this {@link PCollectionView}, which should
     * be that of the underlying {@link PCollection}.
     *
     * <p>For internal use only by runner implementors.
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
      if (!(other instanceof PCollectionView)) {
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
