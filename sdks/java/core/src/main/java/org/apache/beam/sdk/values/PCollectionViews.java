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
package org.apache.beam.sdk.values;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.RandomAccess;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Supplier;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSortedMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * <b>For internal use only; no backwards compatibility guarantees.</b>
 *
 * <p>Implementations of {@link PCollectionView} shared across the SDK.
 */
@Internal
@SuppressWarnings({
  "keyfor",
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class PCollectionViews {
  public interface TypeDescriptorSupplier<T> extends Supplier<TypeDescriptor<T>>, Serializable {}

  /**
   * Returns a {@code PCollectionView<T>} capable of processing elements windowed using the provided
   * {@link WindowingStrategy}.
   *
   * <p>If {@code hasDefault} is {@code true}, then the view will take on the value {@code
   * defaultValue} for any empty windows.
   */
  public static <T, W extends BoundedWindow> PCollectionView<T> singletonView(
      PCollection<T> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy,
      boolean hasDefault,
      @Nullable T defaultValue,
      Coder<T> defaultValueCoder) {
    return new SimplePCollectionView<>(
        pCollection,
        new SingletonViewFn2<>(hasDefault, defaultValue, defaultValueCoder, typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<T>} capable of processing elements windowed using the provided
   * {@link WindowingStrategy}.
   *
   * <p>If {@code hasDefault} is {@code true}, then the view will take on the value {@code
   * defaultValue} for any empty windows.
   *
   * @deprecated See {@link #singletonView}.
   */
  @Deprecated
  public static <T, W extends BoundedWindow> PCollectionView<T> singletonViewUsingVoidKey(
      TupleTag<MultimapView<Void, T>> tag,
      PCollection<KV<Void, T>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy,
      boolean hasDefault,
      @Nullable T defaultValue,
      Coder<T> defaultValueCoder) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new SingletonViewFn<>(hasDefault, defaultValue, defaultValueCoder, typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Iterable<T>>} capable of processing elements windowed using
   * the provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<Iterable<T>> iterableView(
      PCollection<T> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new IterableViewFn2<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Iterable<T>>} capable of processing elements windowed using
   * the provided {@link WindowingStrategy}.
   *
   * @deprecated See {@link #iterableView}.
   */
  @Deprecated
  public static <T, W extends BoundedWindow> PCollectionView<Iterable<T>> iterableViewUsingVoidKey(
      TupleTag<MultimapView<Void, T>> tag,
      PCollection<KV<Void, T>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new IterableViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listView(
      PCollection<T> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new IterableBackedListViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> inMemoryListView(
      PCollection<T> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new InMemoryListViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listView(
      PCollection<T> pCollection,
      TupleTag<Materializations.IterableView<T>> tag,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new IterableBackedListViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listViewWithRandomAccess(
      PCollection<KV<Long, ValueOrMetadata<T, OffsetRange>>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new ListViewFn2<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listViewWithRandomAccess(
      PCollection<KV<Long, ValueOrMetadata<T, OffsetRange>>> pCollection,
      TupleTag<Materializations.MultimapView<Long, ValueOrMetadata<T, OffsetRange>>> tag,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new ListViewFn2<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   *
   * @deprecated See {@link #listView}.
   */
  @Deprecated
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listViewUsingVoidKey(
      TupleTag<MultimapView<Void, T>> tag,
      PCollection<KV<Void, T>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new ListViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   *
   * @deprecated See {@link #listView}.
   */
  @Deprecated
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listViewUsingVoidKey(
      PCollection<KV<Void, T>> pCollection,
      TupleTag<MultimapView<Void, T>> tag,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new ListViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> inMemoryListViewUsingVoidKey(
      PCollection<KV<Void, T>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new InMemoryListFromMultimapViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, V>> mapView(
      PCollection<KV<K, V>> pCollection,
      TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
      TypeDescriptorSupplier<V> valueTypeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new MapViewFn2<>(keyTypeDescriptorSupplier, valueTypeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, V>> inMemoryMapView(
      PCollection<KV<K, V>> pCollection,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new InMemoryMapViewFn<>(keyCoder, valueCoder),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   *
   * @deprecated See {@link #mapView}.
   */
  @Deprecated
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, V>> mapViewUsingVoidKey(
      TupleTag<MultimapView<Void, KV<K, V>>> tag,
      PCollection<KV<Void, KV<K, V>>> pCollection,
      TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
      TypeDescriptorSupplier<V> valueTypeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new MapViewFn<>(keyTypeDescriptorSupplier, valueTypeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow>
      PCollectionView<Map<K, V>> inMemoryMapViewUsingVoidKey(
          PCollection<KV<Void, KV<K, V>>> pCollection,
          Coder<K> keyCoder,
          Coder<V> valueCoder,
          WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new InMemoryMapFromVoidKeyViewFn<>(keyCoder, valueCoder),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements windowed
   * using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>> multimapView(
      PCollection<KV<K, V>> pCollection,
      TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
      TypeDescriptorSupplier<V> valueTypeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new MultimapViewFn2<>(keyTypeDescriptorSupplier, valueTypeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements windowed
   * using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow>
      PCollectionView<Map<K, Iterable<V>>> inMemoryMultimapView(
          PCollection<KV<K, V>> pCollection,
          Coder<K> keyCoder,
          Coder<V> valueCoder,
          WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new InMemoryMultimapViewFn<>(keyCoder, valueCoder),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements windowed
   * using the provided {@link WindowingStrategy}.
   *
   * @deprecated See {@link #multimapView}.
   */
  @Deprecated
  public static <K, V, W extends BoundedWindow>
      PCollectionView<Map<K, Iterable<V>>> multimapViewUsingVoidKey(
          TupleTag<MultimapView<Void, KV<K, V>>> tag,
          PCollection<KV<Void, KV<K, V>>> pCollection,
          TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
          TypeDescriptorSupplier<V> valueTypeDescriptorSupplier,
          WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        tag,
        new MultimapViewFn<>(keyTypeDescriptorSupplier, valueTypeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements windowed
   * using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow>
      PCollectionView<Map<K, Iterable<V>>> inMemoryMultimapViewUsingVoidKey(
          PCollection<KV<Void, KV<K, V>>> pCollection,
          Coder<K> keyCoder,
          Coder<V> valueCoder,
          WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new InMemoryMultimapFromVoidKeyViewFn<>(keyCoder, valueCoder),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Expands a list of {@link PCollectionView} into the form needed for {@link
   * PTransform#getAdditionalInputs()}.
   */
  public static Map<TupleTag<?>, PValue> toAdditionalInputs(Iterable<PCollectionView<?>> views) {
    ImmutableMap.Builder<TupleTag<?>, PValue> additionalInputs = ImmutableMap.builder();
    for (PCollectionView<?> view : views) {
      additionalInputs.put(view.getTagInternal(), view.getPCollection());
    }
    return additionalInputs.build();
  }

  /**
   * Implementation which is able to adapt an iterable materialization to a {@code T}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#singletonView}.
   *
   * <p>{@link SingletonViewFn} is meant to be removed in the future and replaced with this class.
   */
  @Internal
  public static class SingletonViewFn2<T> extends ViewFn<IterableView<T>, T>
      implements HasDefaultValue<T>, IsSingletonView<T> {
    private byte @Nullable [] encodedDefaultValue;
    private transient @Nullable T defaultValue;
    private @Nullable Coder<T> valueCoder;
    private boolean hasDefault;
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    private SingletonViewFn2(
        boolean hasDefault,
        T defaultValue,
        Coder<T> valueCoder,
        TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.hasDefault = hasDefault;
      this.defaultValue = defaultValue;
      this.valueCoder = valueCoder;
      this.typeDescriptorSupplier = typeDescriptorSupplier;
      if (hasDefault) {
        try {
          this.encodedDefaultValue = CoderUtils.encodeToByteArray(valueCoder, defaultValue);
        } catch (IOException e) {
          throw new RuntimeException("Unexpected IOException: ", e);
        }
      }
    }

    /** Returns if a default value was specified. */
    @Internal
    public boolean hasDefault() {
      return hasDefault;
    }

    /**
     * Returns the default value that was specified.
     *
     * <p>For internal use only.
     *
     * @throws NoSuchElementException if no default was specified.
     */
    @Override
    public T getDefaultValue() {
      if (!hasDefault) {
        throw new NoSuchElementException("Empty PCollection accessed as a singleton view.");
      }
      // Lazily decode the default value once
      synchronized (this) {
        if (encodedDefaultValue != null) {
          try {
            defaultValue = CoderUtils.decodeFromByteArray(valueCoder, encodedDefaultValue);
            // Clear the encoded default value to free the reference once we have the object
            // version. Also, this will guarantee that the value will only be decoded once.
            encodedDefaultValue = null;
          } catch (IOException e) {
            throw new RuntimeException("Unexpected IOException: ", e);
          }
        }
        return defaultValue;
      }
    }

    @Override
    public Materialization<IterableView<T>> getMaterialization() {
      return Materializations.iterable();
    }

    @Override
    public T apply(IterableView<T> primitiveViewT) {
      try {
        return Iterables.getOnlyElement(primitiveViewT.get());
      } catch (NoSuchElementException exc) {
        return getDefaultValue();
      } catch (IllegalArgumentException exc) {
        throw new IllegalArgumentException(
            "PCollection with more than one element accessed as a singleton view.");
      }
    }

    @Override
    public TypeDescriptor<T> getTypeDescriptor() {
      return typeDescriptorSupplier.get();
    }
  }

  @Internal
  public interface HasDefaultValue<T> {
    T getDefaultValue();
  }

  @Internal
  public interface IsSingletonView<T> {}

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code T}.
   *
   * <p>For internal use only.
   *
   * @deprecated See {@link SingletonViewFn2}.
   */
  @Deprecated
  public static class SingletonViewFn<T> extends ViewFn<MultimapView<Void, T>, T>
      implements HasDefaultValue<T>, IsSingletonView<T> {
    private byte @Nullable [] encodedDefaultValue;
    private transient @Nullable T defaultValue;
    private @Nullable Coder<T> valueCoder;
    private boolean hasDefault;
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    private SingletonViewFn(
        boolean hasDefault,
        T defaultValue,
        Coder<T> valueCoder,
        TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.hasDefault = hasDefault;
      this.defaultValue = defaultValue;
      this.valueCoder = valueCoder;
      this.typeDescriptorSupplier = typeDescriptorSupplier;
      if (hasDefault) {
        try {
          this.encodedDefaultValue = CoderUtils.encodeToByteArray(valueCoder, defaultValue);
        } catch (IOException e) {
          throw new RuntimeException("Unexpected IOException: ", e);
        }
      }
    }

    /** Returns if a default value was specified. */
    public boolean hasDefault() {
      return hasDefault;
    }

    /**
     * Returns the default value that was specified.
     *
     * <p>For internal use only.
     *
     * @throws NoSuchElementException if no default was specified.
     */
    @Override
    public T getDefaultValue() {
      if (!hasDefault) {
        throw new NoSuchElementException("Empty PCollection accessed as a singleton view.");
      }
      // Lazily decode the default value once
      synchronized (this) {
        if (encodedDefaultValue != null) {
          try {
            defaultValue = CoderUtils.decodeFromByteArray(valueCoder, encodedDefaultValue);
            // Clear the encoded default value to free the reference once we have the object
            // version. Also, this will guarantee that the value will only be decoded once.
            encodedDefaultValue = null;
          } catch (IOException e) {
            throw new RuntimeException("Unexpected IOException: ", e);
          }
        }
        return defaultValue;
      }
    }

    @Override
    public Materialization<MultimapView<Void, T>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public T apply(MultimapView<Void, T> primitiveViewT) {
      try {
        return Iterables.getOnlyElement(primitiveViewT.get(null));
      } catch (NoSuchElementException exc) {
        return getDefaultValue();
      } catch (IllegalArgumentException exc) {
        throw new IllegalArgumentException(
            "PCollection with more than one element accessed as a singleton view.");
      }
    }

    @Override
    public TypeDescriptor<T> getTypeDescriptor() {
      return typeDescriptorSupplier.get();
    }
  }

  /**
   * Implementation which is able to adapt an iterable materialization to a {@code Iterable<T>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#iterableView}.
   *
   * <p>{@link IterableViewFn} is meant to be removed in the future and replaced with this class.
   */
  @Internal
  public static class IterableViewFn2<T> extends ViewFn<IterableView<T>, Iterable<T>> {
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    public IterableViewFn2(TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.typeDescriptorSupplier = typeDescriptorSupplier;
    }

    @Override
    public Materialization<IterableView<T>> getMaterialization() {
      return Materializations.iterable();
    }

    @Override
    public Iterable<T> apply(IterableView<T> primitiveViewT) {
      return Iterables.unmodifiableIterable(primitiveViewT.get());
    }

    @Override
    public TypeDescriptor<Iterable<T>> getTypeDescriptor() {
      return TypeDescriptors.iterables(typeDescriptorSupplier.get());
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code Iterable<T>}.
   *
   * <p>For internal use only.
   *
   * @deprecated See {@link IterableViewFn2}.
   */
  @Deprecated
  public static class IterableViewFn<T> extends ViewFn<MultimapView<Void, T>, Iterable<T>> {
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    public IterableViewFn(TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.typeDescriptorSupplier = typeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<Void, T>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public Iterable<T> apply(MultimapView<Void, T> primitiveViewT) {
      return Iterables.unmodifiableIterable(primitiveViewT.get(null));
    }

    @Override
    public TypeDescriptor<Iterable<T>> getTypeDescriptor() {
      return TypeDescriptors.iterables(typeDescriptorSupplier.get());
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code List<T>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#listView}.
   *
   * <p>{@link ListViewFn} is meant to be removed in the future and replaced with this class.
   */
  @VisibleForTesting
  public static class ListViewFn2<T>
      extends ViewFn<MultimapView<Long, ValueOrMetadata<T, OffsetRange>>, List<T>> {
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    public ListViewFn2(TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.typeDescriptorSupplier = typeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<Long, ValueOrMetadata<T, OffsetRange>>>
        getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public List<T> apply(MultimapView<Long, ValueOrMetadata<T, OffsetRange>> primitiveViewT) {
      return Collections.unmodifiableList(new ListOverMultimapView<>(primitiveViewT));
    }

    @Override
    public TypeDescriptor<List<T>> getTypeDescriptor() {
      return TypeDescriptors.lists(typeDescriptorSupplier.get());
    }

    private static final Counter listViewIteratorCount =
        Metrics.counter(ListViewFn2.class, "iteratorCount");
    private static final Counter listViewGetCount = Metrics.counter(ListViewFn2.class, "getCount");

    /**
     * A {@link List} adapter over a {@link MultimapView}.
     *
     * <p>See {@link View.AsList} for a description of the materialized format and {@code index} to
     * {@code (position, sub-position)} mapping details.
     */
    private static class ListOverMultimapView<T> extends AbstractList<T> implements RandomAccess {
      private final MultimapView<Long, ValueOrMetadata<T, OffsetRange>> primitiveView;
      /**
       * A mapping from non over-lapping ranges to the number of elements at each position within
       * that range. Ranges not specified in the mapping implicitly have 0 elements at those
       * positions.
       *
       * <p>Used to quickly compute the {@code index} -> {@code (position, sub-position} within the
       * map.
       */
      private final Supplier<SortedMap<OffsetRange, Integer>>
          nonOverlappingRangesToNumElementsPerPosition;

      private final Supplier<Integer> size;

      private final boolean recordGets;

      private ListOverMultimapView(
          MultimapView<Long, ValueOrMetadata<T, OffsetRange>> primitiveView) {
        this(
            primitiveView,
            Suppliers.memoize(
                () ->
                    computeOverlappingRanges(
                        Iterables.transform(
                            primitiveView.get(Long.MIN_VALUE), (value) -> value.getMetadata()))));
      }

      private ListOverMultimapView(
          MultimapView<Long, ValueOrMetadata<T, OffsetRange>> primitiveView,
          Supplier<SortedMap<OffsetRange, Integer>> nonOverlappingRangesToNumElementsPerPosition) {
        this(
            primitiveView,
            nonOverlappingRangesToNumElementsPerPosition,
            Suppliers.memoize(
                () -> computeTotalNumElements(nonOverlappingRangesToNumElementsPerPosition.get())),
            true);
      }

      private ListOverMultimapView(
          MultimapView<Long, ValueOrMetadata<T, OffsetRange>> primitiveView,
          Supplier<SortedMap<OffsetRange, Integer>> nonOverlappingRangesToNumElementsPerPosition,
          Supplier<Integer> size,
          boolean recordGets) {
        this.primitiveView = primitiveView;
        this.nonOverlappingRangesToNumElementsPerPosition =
            nonOverlappingRangesToNumElementsPerPosition;
        this.size = size;
        this.recordGets = recordGets;
      }

      @Override
      public T get(int index) {
        if (recordGets) {
          listViewGetCount.inc();
        }
        if (index < 0 || index >= size.get()) {
          throw new IndexOutOfBoundsException();
        }
        KV<Long, Integer> position =
            computePositionForIndex(nonOverlappingRangesToNumElementsPerPosition.get(), index);
        return Iterables.get(primitiveView.get(position.getKey()), position.getValue()).get();
      }

      @Override
      public int size() {
        return size.get();
      }

      @Override
      public Iterator<T> iterator() {
        return listIterator();
      }

      @Override
      public ListIterator<T> listIterator() {
        listViewIteratorCount.inc();
        if (recordGets) {
          // AbstractList's iterable is implemented in terms of get().
          // We don't want to count those as user-initiated gets.
          return new ListOverMultimapView<>(
                  primitiveView, nonOverlappingRangesToNumElementsPerPosition, size, false)
              .listIterator();
        } else {
          return super.listIterator();
        }
      }
    }
  }

  /**
   * Compares {@link OffsetRange}s such that ranges are ordered by the smallest {@code from} and in
   * case of a tie the smallest {@code to}.
   */
  @VisibleForTesting
  static class OffsetRangeComparator implements Comparator<OffsetRange> {
    private static final OffsetRangeComparator INSTANCE = new OffsetRangeComparator();

    @Override
    public int compare(OffsetRange o1, OffsetRange o2) {
      int fromComparison = Longs.compare(o1.getFrom(), o2.getFrom());
      if (fromComparison != 0) {
        return fromComparison;
      }
      return Longs.compare(o1.getTo(), o2.getTo());
    }
  }

  @VisibleForTesting
  static SortedMap<OffsetRange, Integer> computeOverlappingRanges(Iterable<OffsetRange> ranges) {
    ImmutableSortedMap.Builder<OffsetRange, Integer> rval =
        ImmutableSortedMap.orderedBy(OffsetRangeComparator.INSTANCE);
    List<OffsetRange> sortedRanges = Lists.newArrayList(ranges);
    if (sortedRanges.isEmpty()) {
      return rval.build();
    }
    Collections.sort(sortedRanges, OffsetRangeComparator.INSTANCE);

    // Stores ranges in smallest 'from' and then smallest 'to' order
    // e.g. [2, 7), [3, 4), [3, 5), [3, 5), [3, 6), [4, 0)
    PriorityQueue<OffsetRange> rangesWithSameFrom =
        new PriorityQueue<>(OffsetRangeComparator.INSTANCE);
    Iterator<OffsetRange> iterator = sortedRanges.iterator();

    // Stored in reverse sorted order so that when we iterate and re-add them back to
    // overlappingRanges they are stored in sorted order from smallest to largest range.to
    List<OffsetRange> rangesToProcess = new ArrayList<>();
    while (iterator.hasNext()) {
      OffsetRange current = iterator.next();
      // Skip empty ranges
      if (current.getFrom() == current.getTo()) {
        continue;
      }

      // If the current range has a different 'from' then a prior range then we must produce
      // ranges in [rangesWithSameFrom.from, current.from)
      while (!rangesWithSameFrom.isEmpty()
          && rangesWithSameFrom.peek().getFrom() != current.getFrom()) {
        rangesToProcess.addAll(rangesWithSameFrom);
        Collections.sort(rangesToProcess, OffsetRangeComparator.INSTANCE);
        rangesWithSameFrom.clear();

        int i = 0;
        long lastTo = rangesToProcess.get(i).getFrom();
        // Output all the ranges that are strictly less then current.from
        // e.g. current.to := 7 for [3, 4), [3, 5), [3, 5), [3, 6) will produce
        // [3, 4) := 4
        // [4, 5) := 3
        // [5, 6) := 1
        for (; i < rangesToProcess.size(); ++i) {
          if (rangesToProcess.get(i).getTo() > current.getFrom()) {
            break;
          }
          // Output only the first of any subsequent duplicate ranges
          if (i == 0 || rangesToProcess.get(i - 1).getTo() != rangesToProcess.get(i).getTo()) {
            rval.put(
                new OffsetRange(lastTo, rangesToProcess.get(i).getTo()),
                rangesToProcess.size() - i);
            lastTo = rangesToProcess.get(i).getTo();
          }
        }

        // We exitted the loop with 'to' > current.from, we must add the range [lastTo,
        // current.from) if it is non-empty
        if (lastTo < current.getFrom() && i != rangesToProcess.size()) {
          rval.put(new OffsetRange(lastTo, current.getFrom()), rangesToProcess.size() - i);
        }

        // The remaining ranges have a 'to' that is greater then 'current.from' and will overlap
        // with current so add them back to rangesWithSameFrom with the updated 'from'
        for (; i < rangesToProcess.size(); ++i) {
          rangesWithSameFrom.add(
              new OffsetRange(current.getFrom(), rangesToProcess.get(i).getTo()));
        }

        rangesToProcess.clear();
      }
      rangesWithSameFrom.add(current);
    }

    // Process the last chunk of overlapping ranges
    while (!rangesWithSameFrom.isEmpty()) {
      // This range always represents the range with with the smallest 'to'
      OffsetRange current = rangesWithSameFrom.remove();

      rangesToProcess.addAll(rangesWithSameFrom);
      Collections.sort(rangesToProcess, OffsetRangeComparator.INSTANCE);
      rangesWithSameFrom.clear();

      rval.put(current, rangesToProcess.size() + 1 /* include current */);

      // Shorten all the remaining ranges such that they start with current.to
      for (OffsetRange rangeWithDifferentFrom : rangesToProcess) {
        // Skip any duplicates of current
        if (rangeWithDifferentFrom.getTo() > current.getTo()) {
          rangesWithSameFrom.add(new OffsetRange(current.getTo(), rangeWithDifferentFrom.getTo()));
        }
      }
      rangesToProcess.clear();
    }
    return rval.build();
  }

  @VisibleForTesting
  static int computeTotalNumElements(
      Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition) {
    long sum = 0;
    for (Map.Entry<OffsetRange, Integer> range :
        nonOverlappingRangesToNumElementsPerPosition.entrySet()) {
      sum +=
          Math.multiplyExact(
              Math.subtractExact(range.getKey().getTo(), range.getKey().getFrom()),
              range.getValue());
    }
    return Ints.checkedCast(sum);
  }

  @VisibleForTesting
  static KV<Long, Integer> computePositionForIndex(
      Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition, int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Position %s was out of bounds for ranges %s.",
              index, nonOverlappingRangesToNumElementsPerPosition));
    }
    for (Map.Entry<OffsetRange, Integer> range :
        nonOverlappingRangesToNumElementsPerPosition.entrySet()) {
      int numElementsInRange =
          Ints.checkedCast(
              Math.multiplyExact(
                  Math.subtractExact(range.getKey().getTo(), range.getKey().getFrom()),
                  range.getValue()));
      if (numElementsInRange <= index) {
        index -= numElementsInRange;
        continue;
      }
      long position = range.getKey().getFrom() + index / range.getValue();
      int subPosition = index % range.getValue();
      return KV.of(position, subPosition);
    }
    throw new IndexOutOfBoundsException(
        String.format(
            "Position %s was out of bounds for ranges %s.",
            index, nonOverlappingRangesToNumElementsPerPosition));
  }

  /** Stores values or metadata about values. */
  public static class ValueOrMetadata<T, MetaT> {
    private final T value;
    private final boolean isMetadata;
    private final MetaT metadata;

    public static <T, MetaT> ValueOrMetadata<T, MetaT> create(T value) {
      return new ValueOrMetadata(false, value, null);
    }

    public static <T, MetaT> ValueOrMetadata<T, MetaT> createMetadata(MetaT metadata) {
      return new ValueOrMetadata(true, null, metadata);
    }

    public ValueOrMetadata(boolean isMetadata, T value, MetaT metadata) {
      this.isMetadata = isMetadata;
      this.value = value;
      this.metadata = metadata;
    }

    public T get() {
      checkState(!isMetadata);
      return value;
    }

    public boolean isMetadata() {
      return isMetadata;
    }

    public MetaT getMetadata() {
      checkState(isMetadata);
      return metadata;
    }
  }

  /** A coder for {@link ValueOrMetadata}. */
  public static class ValueOrMetadataCoder<T, MetaT>
      extends StructuredCoder<ValueOrMetadata<T, MetaT>> {
    public static <T, MetaT> ValueOrMetadataCoder<T, MetaT> create(
        Coder<T> valueCoder, Coder<MetaT> metadataCoder) {
      return new ValueOrMetadataCoder<>(valueCoder, metadataCoder);
    }

    private final Coder<T> valueCoder;
    private final Coder<MetaT> metadataCoder;

    private ValueOrMetadataCoder(Coder<T> valueCoder, Coder<MetaT> metadataCoder) {
      this.valueCoder = valueCoder;
      this.metadataCoder = metadataCoder;
    }

    @Override
    public void encode(ValueOrMetadata<T, MetaT> value, OutputStream outStream)
        throws CoderException, IOException {
      BooleanCoder.of().encode(value.isMetadata(), outStream);
      if (value.isMetadata()) {
        metadataCoder.encode(value.getMetadata(), outStream);
      } else {
        valueCoder.encode(value.get(), outStream);
      }
    }

    @Override
    public ValueOrMetadata<T, MetaT> decode(InputStream inStream)
        throws CoderException, IOException {
      boolean isMetadata = BooleanCoder.of().decode(inStream);
      if (isMetadata) {
        return ValueOrMetadata.createMetadata(metadataCoder.decode(inStream));
      } else {
        return ValueOrMetadata.create(valueCoder.decode(inStream));
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(valueCoder, metadataCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(valueCoder, "value coder");
      verifyDeterministic(metadataCoder, "metadata coder");
    }
  }

  /**
   * Implementation which is able to adapt an iterable materialization to a {@code List<T>}.
   *
   * <p>Unlike ListViewFn2, this implementation is optimized for iteration rather than indexing.
   *
   * <p>For internal use only.
   */
  public static class IterableBackedListViewFn<T> extends ViewFn<IterableView<T>, List<T>> {
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    public IterableBackedListViewFn(TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.typeDescriptorSupplier = typeDescriptorSupplier;
    }

    @Override
    public Materialization<IterableView<T>> getMaterialization() {
      return Materializations.iterable();
    }

    @Override
    public List<T> apply(IterableView<T> primitiveViewT) {
      Supplier<Integer> size = Suppliers.memoize(() -> Iterables.size(primitiveViewT.get()));

      return new List<T>() {
        @Override
        public int size() {
          return size.get();
        }

        @Override
        public boolean isEmpty() {
          return Iterables.isEmpty(primitiveViewT.get());
        }

        @Override
        public boolean contains(Object o) {
          return Iterables.contains(primitiveViewT.get(), o);
        }

        @Override
        public Iterator<T> iterator() {
          return primitiveViewT.get().iterator();
        }

        @Override
        public T get(int index) {
          return Iterables.get(primitiveViewT.get(), index);
        }

        @Override
        public Object[] toArray() {
          return Iterables.toArray(primitiveViewT.get(), Object.class);
        }

        @Override
        public <T1> T1[] toArray(T1[] a) {
          return Iterables.toArray(
              (Iterable<T1>) primitiveViewT.get(), (Class<T1>) a.getClass().getComponentType());
        }

        @Override
        public boolean containsAll(Collection<?> c) {
          for (Object o : c) {
            if (!contains(o)) {
              return false;
            }
          }
          return true;
        }

        @Override
        public int indexOf(Object o) {
          return Iterables.indexOf(primitiveViewT.get(), v -> Objects.equals(v, o));
        }

        @Override
        public int lastIndexOf(Object o) {
          return ImmutableList.copyOf(primitiveViewT.get()).lastIndexOf(o);
        }

        @Override
        public List<T> subList(int fromIndex, int toIndex) {
          Iterator<T> iterator = primitiveViewT.get().iterator();
          if (Iterators.advance(iterator, fromIndex) != fromIndex) {
            throw new IndexOutOfBoundsException();
          }
          List<T> subList = ImmutableList.copyOf(Iterators.limit(iterator, toIndex - fromIndex));
          if (subList.size() != toIndex - fromIndex) {
            throw new IndexOutOfBoundsException();
          }
          return subList;
        }

        @Override
        public ListIterator<T> listIterator() {
          return ImmutableList.copyOf(primitiveViewT.get()).listIterator();
        }

        @Override
        public ListIterator<T> listIterator(int index) {
          return ImmutableList.copyOf(primitiveViewT.get()).listIterator(index);
        }

        // Unimplemented mutable list methods.

        @Override
        public boolean add(T t) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(int index, Collection<? extends T> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
          throw new UnsupportedOperationException();
        }

        @Override
        public T set(int index, T element) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void add(int index, T element) {
          throw new UnsupportedOperationException();
        }

        @Override
        public T remove(int index) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public TypeDescriptor<List<T>> getTypeDescriptor() {
      return TypeDescriptors.lists(typeDescriptorSupplier.get());
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof IterableBackedListViewFn;
    }

    @Override
    public int hashCode() {
      return ListViewFn.class.hashCode();
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code List<T>}.
   *
   * <p>For internal use only.
   *
   * @deprecated See {@link ListViewFn2}.
   */
  @Deprecated
  public static class ListViewFn<T> extends ViewFn<MultimapView<Void, T>, List<T>> {
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    public ListViewFn(TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.typeDescriptorSupplier = typeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<Void, T>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public List<T> apply(MultimapView<Void, T> primitiveViewT) {
      List<T> list = new ArrayList<>();
      for (T t : primitiveViewT.get(null)) {
        list.add(t);
      }
      return Collections.unmodifiableList(list);
    }

    @Override
    public TypeDescriptor<List<T>> getTypeDescriptor() {
      return TypeDescriptors.lists(typeDescriptorSupplier.get());
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof ListViewFn;
    }

    @Override
    public int hashCode() {
      return ListViewFn.class.hashCode();
    }
  }

  /**
   * Implementation which is able to adapt an iterable materialization to an in-memory {@code
   * List<T>}.
   *
   * <p>For internal use only.
   */
  public static class InMemoryListViewFn<T> extends ViewFn<IterableView<T>, List<T>> {
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    public InMemoryListViewFn(TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.typeDescriptorSupplier = typeDescriptorSupplier;
    }

    @Override
    public Materialization<IterableView<T>> getMaterialization() {
      return Materializations.iterable();
    }

    @Override
    public List<T> apply(IterableView<T> primitiveView) {
      return ImmutableList.copyOf(primitiveView.get());
    }

    @Override
    public TypeDescriptor<List<T>> getTypeDescriptor() {
      return TypeDescriptors.lists(typeDescriptorSupplier.get());
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof InMemoryListViewFn;
    }

    @Override
    public int hashCode() {
      return InMemoryListViewFn.class.hashCode();
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to an in-memory {@code
   * List<T>}.
   *
   * <p>For internal use only.
   */
  public static class InMemoryListFromMultimapViewFn<T>
      extends ViewFn<MultimapView<Void, T>, List<T>> {
    private TypeDescriptorSupplier<T> typeDescriptorSupplier;

    public InMemoryListFromMultimapViewFn(TypeDescriptorSupplier<T> typeDescriptorSupplier) {
      this.typeDescriptorSupplier = typeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<Void, T>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public List<T> apply(MultimapView<Void, T> primitiveView) {
      return ImmutableList.copyOf(primitiveView.get(null));
    }

    @Override
    public TypeDescriptor<List<T>> getTypeDescriptor() {
      return TypeDescriptors.lists(typeDescriptorSupplier.get());
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof InMemoryListFromMultimapViewFn;
    }

    @Override
    public int hashCode() {
      return InMemoryListFromMultimapViewFn.class.hashCode();
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code Map<K,
   * Iterable<V>>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#multimapView}.
   *
   * <p>{@link MultimapViewFn} is meant to be removed in the future and replaced with this class.
   */
  @Internal
  public static class MultimapViewFn2<K, V>
      extends ViewFn<MultimapView<K, V>, Map<K, Iterable<V>>> {
    private TypeDescriptorSupplier<K> keyTypeDescriptorSupplier;
    private TypeDescriptorSupplier<V> valueTypeDescriptorSupplier;

    public MultimapViewFn2(
        TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
        TypeDescriptorSupplier<V> valueTypeDescriptorSupplier) {
      this.keyTypeDescriptorSupplier = keyTypeDescriptorSupplier;
      this.valueTypeDescriptorSupplier = valueTypeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<K, V>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public Map<K, Iterable<V>> apply(MultimapView<K, V> primitiveViewT) {
      return Collections.unmodifiableMap(new MultimapViewToMultimapAdapter<>(primitiveViewT));
    }

    @Override
    public TypeDescriptor<Map<K, Iterable<V>>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyTypeDescriptorSupplier.get(),
          TypeDescriptors.iterables(valueTypeDescriptorSupplier.get()));
    }
  }

  /**
   * Implementation which is able to adapt an iterable materialization to an in-memory {@code Map<K,
   * Iterable<V>>}.
   *
   * <p>For internal use only.
   */
  public static class InMemoryMultimapViewFn<K, V>
      extends ViewFn<IterableView<KV<K, V>>, Map<K, Iterable<V>>> {
    private Coder<K> keyCoder;
    private Coder<V> valueCoder;

    public InMemoryMultimapViewFn(Coder<K> keyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public Materialization<IterableView<KV<K, V>>> getMaterialization() {
      return Materializations.iterable();
    }

    @Override
    public Map<K, Iterable<V>> apply(IterableView<KV<K, V>> primitiveView) {
      return StructuralValueMap.createMultimap(primitiveView.get(), keyCoder);
    }

    @Override
    public TypeDescriptor<Map<K, Iterable<V>>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyCoder.getEncodedTypeDescriptor(),
          TypeDescriptors.iterables(valueCoder.getEncodedTypeDescriptor()));
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to an in-memory {@code Map<K,
   * Iterable<V>>}.
   *
   * <p>For internal use only.
   */
  public static class InMemoryMultimapFromVoidKeyViewFn<K, V>
      extends ViewFn<MultimapView<Void, KV<K, V>>, Map<K, Iterable<V>>> {
    private Coder<K> keyCoder;
    private Coder<V> valueCoder;

    public InMemoryMultimapFromVoidKeyViewFn(Coder<K> keyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public Materialization<MultimapView<Void, KV<K, V>>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public Map<K, Iterable<V>> apply(MultimapView<Void, KV<K, V>> primitiveView) {
      return StructuralValueMap.createMultimap(primitiveView.get(null), keyCoder);
    }

    @Override
    public TypeDescriptor<Map<K, Iterable<V>>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyCoder.getEncodedTypeDescriptor(),
          TypeDescriptors.iterables(valueCoder.getEncodedTypeDescriptor()));
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code Map<K,
   * Iterable<V>>}.
   *
   * <p>For internal use only.
   *
   * @deprecated See {@link MultimapViewFn2}.
   */
  @Deprecated
  public static class MultimapViewFn<K, V>
      extends ViewFn<MultimapView<Void, KV<K, V>>, Map<K, Iterable<V>>> {
    private TypeDescriptorSupplier<K> keyTypeDescriptorSupplier;
    private TypeDescriptorSupplier<V> valueTypeDescriptorSupplier;

    public MultimapViewFn(
        TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
        TypeDescriptorSupplier<V> valueTypeDescriptorSupplier) {
      this.keyTypeDescriptorSupplier = keyTypeDescriptorSupplier;
      this.valueTypeDescriptorSupplier = valueTypeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<Void, KV<K, V>>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public Map<K, Iterable<V>> apply(MultimapView<Void, KV<K, V>> primitiveViewT) {
      // TODO: https://github.com/apache/beam/issues/18569 - fix this so that we aren't relying on
      // Java equality and are
      // using structural value equality.
      Multimap<K, V> multimap = ArrayListMultimap.create();
      for (KV<K, V> elem : primitiveViewT.get(null)) {
        multimap.put(elem.getKey(), elem.getValue());
      }
      // Safe covariant cast that Java cannot express without rawtypes, even with unchecked casts
      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<K, Iterable<V>> resultMap = (Map) multimap.asMap();
      return Collections.unmodifiableMap(resultMap);
    }

    @Override
    public TypeDescriptor<Map<K, Iterable<V>>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyTypeDescriptorSupplier.get(),
          TypeDescriptors.iterables(valueTypeDescriptorSupplier.get()));
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code Map<K, V>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#mapView}.
   *
   * <p>{@link MapViewFn} is meant to be removed in the future and replaced with this class.
   */
  @Internal
  public static class MapViewFn2<K, V> extends ViewFn<MultimapView<K, V>, Map<K, V>> {
    private TypeDescriptorSupplier<K> keyTypeDescriptorSupplier;
    private TypeDescriptorSupplier<V> valueTypeDescriptorSupplier;

    public MapViewFn2(
        TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
        TypeDescriptorSupplier<V> valueTypeDescriptorSupplier) {
      this.keyTypeDescriptorSupplier = keyTypeDescriptorSupplier;
      this.valueTypeDescriptorSupplier = valueTypeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<K, V>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public Map<K, V> apply(MultimapView<K, V> primitiveViewT) {
      return Collections.unmodifiableMap(new MultimapViewToMapAdapter<>(primitiveViewT));
    }

    @Override
    public TypeDescriptor<Map<K, V>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyTypeDescriptorSupplier.get(), valueTypeDescriptorSupplier.get());
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code Map<K, V>}.
   *
   * <p>For internal use only.
   *
   * @deprecated See {@link MapViewFn2}.
   */
  @Deprecated
  public static class MapViewFn<K, V> extends ViewFn<MultimapView<Void, KV<K, V>>, Map<K, V>> {
    private TypeDescriptorSupplier<K> keyTypeDescriptorSupplier;
    private TypeDescriptorSupplier<V> valueTypeDescriptorSupplier;

    public MapViewFn(
        TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
        TypeDescriptorSupplier<V> valueTypeDescriptorSupplier) {
      this.keyTypeDescriptorSupplier = keyTypeDescriptorSupplier;
      this.valueTypeDescriptorSupplier = valueTypeDescriptorSupplier;
    }

    @Override
    public Materialization<MultimapView<Void, KV<K, V>>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public Map<K, V> apply(MultimapView<Void, KV<K, V>> primitiveViewT) {
      // TODO: https://github.com/apache/beam/issues/18569 - fix this so that we aren't relying on
      // Java equality and are
      // using structural value equality.
      Map<K, V> map = new HashMap<>();
      for (KV<K, V> elem : primitiveViewT.get(null)) {
        if (map.containsKey(elem.getKey())) {
          throw new IllegalArgumentException("Duplicate values for " + elem.getKey());
        }
        map.put(elem.getKey(), elem.getValue());
      }
      return Collections.unmodifiableMap(map);
    }

    @Override
    public TypeDescriptor<Map<K, V>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyTypeDescriptorSupplier.get(), valueTypeDescriptorSupplier.get());
    }
  }

  /**
   * Implementation which is able to adapt an iterable materialization to an in-memory {@code Map<K,
   * V>}.
   *
   * <p>For internal use only.
   */
  public static class InMemoryMapViewFn<K, V> extends ViewFn<IterableView<KV<K, V>>, Map<K, V>> {
    private Coder<K> keyCoder;
    private Coder<V> valueCoder;

    public InMemoryMapViewFn(Coder<K> keyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public Materialization<IterableView<KV<K, V>>> getMaterialization() {
      return Materializations.iterable();
    }

    @Override
    public Map<K, V> apply(IterableView<KV<K, V>> primitiveView) {
      return StructuralValueMap.createMap(primitiveView.get(), keyCoder);
    }

    @Override
    public TypeDescriptor<Map<K, V>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyCoder.getEncodedTypeDescriptor(), valueCoder.getEncodedTypeDescriptor());
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to an in-memory {@code Map<K,
   * V>}.
   *
   * <p>For internal use only.
   */
  public static class InMemoryMapFromVoidKeyViewFn<K, V>
      extends ViewFn<MultimapView<Void, KV<K, V>>, Map<K, V>> {
    private Coder<K> keyCoder;
    private Coder<V> valueCoder;

    public InMemoryMapFromVoidKeyViewFn(Coder<K> keyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public Materialization<MultimapView<Void, KV<K, V>>> getMaterialization() {
      return Materializations.multimap();
    }

    @Override
    public Map<K, V> apply(MultimapView<Void, KV<K, V>> primitiveView) {
      return StructuralValueMap.createMap(primitiveView.get(null), keyCoder);
    }

    @Override
    public TypeDescriptor<Map<K, V>> getTypeDescriptor() {
      return TypeDescriptors.maps(
          keyCoder.getEncodedTypeDescriptor(), valueCoder.getEncodedTypeDescriptor());
    }
  }

  /**
   * A map looking up values based on the structural value of the key, as given by the key coder.
   *
   * @param <K> key type
   * @param <V> value type
   */
  private static class StructuralValueMap<K, V> implements Map<K, V> {
    private final Coder<K> keyCoder;

    private final Map<Object, Map.Entry<K, V>> entries;

    private static <K, V> Map<K, V> createMap(Iterable<KV<K, V>> kvs, Coder<K> keyCoder) {
      if (keyCoder.consistentWithEquals()) {
        Map<K, V> map = new HashMap<>();
        for (KV<K, V> elem : kvs) {
          if (map.containsKey(elem.getKey())) {
            throw new IllegalArgumentException("Duplicate values for " + elem.getKey());
          }
          map.put(elem.getKey(), elem.getValue());
        }
        return Collections.unmodifiableMap(map);
      } else {
        Map<Object, Map.Entry<K, V>> entries = new HashMap<>();
        for (KV<K, V> elem : kvs) {
          Object oldValue =
              entries.putIfAbsent(
                  keyCoder.structuralValue(elem.getKey()),
                  new AbstractMap.SimpleImmutableEntry<>(elem.getKey(), elem.getValue()));
          if (oldValue != null) {
            throw new IllegalArgumentException("Duplicate values for " + elem.getKey());
          }
        }
        return new StructuralValueMap<>(entries, keyCoder);
      }
    }

    private static <K, V> Map<K, Iterable<V>> createMultimap(
        Iterable<KV<K, V>> kvs, Coder<K> keyCoder) {
      if (keyCoder.consistentWithEquals()) {
        Multimap<K, V> multimap = ArrayListMultimap.create();
        for (KV<K, V> elem : kvs) {
          multimap.put(elem.getKey(), elem.getValue());
        }
        return ImmutableMap.copyOf(
            Maps.transformValues(multimap.asMap(), v -> ImmutableList.copyOf(v)));
      } else {
        Map<Object, Map.Entry<K, List<V>>> entries = new HashMap<>();
        for (KV<K, V> elem : kvs) {
          Object sk = keyCoder.structuralValue(elem.getKey());
          Entry<K, List<V>> e = entries.get(sk);
          if (e == null) {
            e = new AbstractMap.SimpleEntry<>(elem.getKey(), Lists.newArrayList(elem.getValue()));
            entries.put(sk, e);
          } else {
            e.getValue().add(elem.getValue());
          }
        }
        for (Entry<K, List<V>> e : entries.values()) {
          // Make all values immutable here rather than having to wrap at every access.
          e.setValue(ImmutableList.copyOf(e.getValue()));
        }
        // Safe covariant cast that Java cannot express without rawtypes, even with unchecked casts
        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<Object, Map.Entry<K, Iterable<V>>> typedEntries = (Map) entries;
        return new StructuralValueMap<>(typedEntries, keyCoder);
      }
    }

    protected StructuralValueMap(Map<Object, Map.Entry<K, V>> entries, Coder<K> keyCoder) {
      this.keyCoder = keyCoder;
      this.entries = entries;
    }

    @Override
    public int size() {
      return entries.size();
    }

    @Override
    public boolean isEmpty() {
      return entries.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      try {
        return entries.containsKey(keyCoder.structuralValue((K) key));
      } catch (ClassCastException exn) {
        return false;
      }
    }

    @Override
    public boolean containsValue(Object value) {
      return values().contains(value);
    }

    @Override
    public V get(Object key) {
      try {
        return entries.get(keyCoder.structuralValue((K) key)).getValue();
      } catch (ClassCastException exn) {
        return null;
      }
    }

    @Override
    public V put(K key, V value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
      return new AbstractSet<K>() {
        @Override
        public Iterator<K> iterator() {
          return Iterators.transform(entries.values().iterator(), e -> e.getKey());
        }

        @Override
        public int size() {
          return entries.size();
        }

        @Override
        public boolean contains(Object key) {
          return containsKey(key);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
          for (Object o : c) {
            if (!containsKey(o)) {
              return false;
            }
          }
          return true;
        }
      };
    }

    @Override
    public Collection<V> values() {
      return Collections2.transform(entries.values(), e -> e.getValue());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return new AbstractSet<Entry<K, V>>() {
        @Override
        public Iterator<Entry<K, V>> iterator() {
          return entries.values().iterator();
        }

        @Override
        public int size() {
          return entries.size();
        }
      };
    }
  }

  /**
   * A class for {@link PCollectionView} implementations, with additional type parameters that are
   * not visible at pipeline assembly time when the view is used as a side input.
   *
   * <p>For internal use only.
   */
  public static class SimplePCollectionView<ElemT, PrimitiveViewT, ViewT, W extends BoundedWindow>
      extends PValueBase implements PCollectionView<ViewT> {
    /** The {@link PCollection} this view was originally created from. */
    private transient PCollection<ElemT> pCollection;

    /** A unique tag for the view, typed according to the elements underlying the view. */
    private TupleTag<PrimitiveViewT> tag;

    private WindowMappingFn<W> windowMappingFn;

    /** The windowing strategy for the PCollection underlying the view. */
    private WindowingStrategy<?, W> windowingStrategy;

    /** The coder for the elements underlying the view. */
    private @Nullable Coder<ElemT> coder;

    /** The typed {@link ViewFn} for this view. */
    private ViewFn<PrimitiveViewT, ViewT> viewFn;

    /**
     * Call this constructor to initialize the fields for which this base class provides boilerplate
     * accessors.
     */
    private SimplePCollectionView(
        PCollection<ElemT> pCollection,
        TupleTag<PrimitiveViewT> tag,
        ViewFn<PrimitiveViewT, ViewT> viewFn,
        WindowMappingFn<W> windowMappingFn,
        WindowingStrategy<?, W> windowingStrategy) {
      super(pCollection.getPipeline());
      this.pCollection = pCollection;
      this.windowMappingFn = windowMappingFn;
      this.tag = tag;
      this.windowingStrategy = windowingStrategy;
      this.viewFn = viewFn;
      this.coder = pCollection.getCoder();
    }

    /**
     * Call this constructor to initialize the fields for which this base class provides boilerplate
     * accessors, with an auto-generated tag.
     */
    private SimplePCollectionView(
        PCollection<ElemT> pCollection,
        ViewFn<PrimitiveViewT, ViewT> viewFn,
        WindowMappingFn<W> windowMappingFn,
        WindowingStrategy<?, W> windowingStrategy) {
      this(pCollection, new TupleTag<>(), viewFn, windowMappingFn, windowingStrategy);
    }

    @Override
    public ViewFn<PrimitiveViewT, ViewT> getViewFn() {
      return viewFn;
    }

    @Override
    public WindowMappingFn<?> getWindowMappingFn() {
      return windowMappingFn;
    }

    @Override
    public PCollection<?> getPCollection() {
      return pCollection;
    }

    /**
     * Returns a unique {@link TupleTag} identifying this {@link PCollectionView}.
     *
     * <p>For internal use only by runner implementors.
     */
    @Override
    public TupleTag<?> getTagInternal() {
      return tag;
    }

    /**
     * Returns the {@link WindowingStrategy} of this {@link PCollectionView}, which should be that
     * of the underlying {@link PCollection}.
     *
     * <p>For internal use only by runner implementors.
     */
    @Override
    public WindowingStrategy<?, ?> getWindowingStrategyInternal() {
      return windowingStrategy;
    }

    @Override
    public Coder<?> getCoderInternal() {
      return coder;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag);
    }

    @Override
    public boolean equals(@Nullable Object other) {
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
          .add("coder", coder)
          .add("windowMappingFn", windowMappingFn)
          .add("pCollection", pCollection)
          .toString();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return Collections.singletonMap(tag, pCollection);
    }
  }

  /** A {@link MultimapView} to {@link Map Map<K, V>} adapter. */
  private static class MultimapViewToMapAdapter<K, V> extends AbstractMap<K, V> {
    private final MultimapView<K, V> primitiveViewT;
    private final Supplier<Integer> size;

    private MultimapViewToMapAdapter(MultimapView<K, V> primitiveViewT) {
      this.primitiveViewT = primitiveViewT;
      this.size = Suppliers.memoize(() -> Iterables.size(primitiveViewT.get()));
    }

    @Override
    public boolean containsKey(Object key) {
      return primitiveViewT.get((K) key).iterator().hasNext();
    }

    @Override
    public V get(Object key) {
      Iterator<V> iterator = primitiveViewT.get((K) key).iterator();
      if (!iterator.hasNext()) {
        return null;
      }
      V value = iterator.next();
      if (iterator.hasNext()) {
        throw new IllegalArgumentException("Duplicate values for " + key);
      }
      return value;
    }

    @Override
    public int size() {
      return size.get();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return new AbstractSet<Entry<K, V>>() {
        @Override
        public Iterator<Entry<K, V>> iterator() {
          return FluentIterable.from(primitiveViewT.get())
              .<Entry<K, V>>transform((K key) -> new SimpleEntry<>(key, get(key)))
              .iterator();
        }

        @Override
        public boolean contains(Object o) {
          if (!(o instanceof Entry)) {
            return false;
          }
          Entry<?, ?> entry = (Entry<?, ?>) o;
          // We treat the absence of the key in the map as a difference in these abstract sets. The
          // underlying primitive view represents missing keys as empty iterables so we use this
          // to check if the map contains the key first before comparing values.
          Iterable<V> value = primitiveViewT.get((K) entry.getKey());
          if (value.iterator().hasNext()) {
            return false;
          }
          return Objects.equals(entry.getValue(), value);
        }

        @Override
        public int size() {
          return size.get();
        }
      };
    }
  }

  /** A {@link MultimapView} to {@link Map Map<K, Iterable<V>>} adapter. */
  private static class MultimapViewToMultimapAdapter<K, V> extends AbstractMap<K, Iterable<V>> {
    private final MultimapView<K, V> primitiveViewT;
    private final Supplier<Integer> size;

    private MultimapViewToMultimapAdapter(MultimapView<K, V> primitiveViewT) {
      this.primitiveViewT = primitiveViewT;
      this.size = Suppliers.memoize(() -> Iterables.size(primitiveViewT.get()));
    }

    @Override
    public boolean containsKey(Object key) {
      return primitiveViewT.get((K) key).iterator().hasNext();
    }

    @Override
    public Iterable<V> get(Object key) {
      Iterable<V> values = primitiveViewT.get((K) key);
      // The only way for the values iterable to be empty is for us to have never seen such a key
      // during materialization and hence we return 'null' to satisfy Java's Map contract.
      if (values.iterator().hasNext()) {
        return values;
      } else {
        return null;
      }
    }

    @Override
    public int size() {
      return size.get();
    }

    @Override
    public Set<Entry<K, Iterable<V>>> entrySet() {
      return new AbstractSet<Entry<K, Iterable<V>>>() {
        @Override
        public Iterator<Entry<K, Iterable<V>>> iterator() {
          return FluentIterable.from(primitiveViewT.get())
              .<Entry<K, Iterable<V>>>transform(
                  (K key) -> new SimpleEntry<>(key, primitiveViewT.get(key)))
              .iterator();
        }

        @Override
        public boolean contains(Object o) {
          if (!(o instanceof Entry)) {
            return false;
          }
          Entry<?, ?> entry = (Entry<?, ?>) o;
          if (!(entry.getValue() instanceof Iterable)) {
            return false;
          }
          // We treat the absence of the key in the map as a difference in these abstract sets. The
          // underlying primitive view represents missing keys as empty iterables so we use this
          // to check if the map contains the key first before comparing values.
          Iterable<V> value = primitiveViewT.get((K) entry.getKey());
          if (value.iterator().hasNext()) {
            return false;
          }
          return Iterables.elementsEqual((Iterable<?>) entry.getValue(), value);
        }

        @Override
        public int size() {
          return size.get();
        }
      };
    }
  }
}
