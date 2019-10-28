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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;

/**
 * <b>For internal use only; no backwards compatibility guarantees.</b>
 *
 * <p>Implementations of {@link PCollectionView} shared across the SDK.
 */
@Internal
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
      PCollection<KV<Void, T>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy,
      boolean hasDefault,
      @Nullable T defaultValue,
      Coder<T> defaultValueCoder) {
    return new SimplePCollectionView<>(
        pCollection,
        new SingletonViewFn<T>(hasDefault, defaultValue, defaultValueCoder, typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Iterable<T>>} capable of processing elements windowed using
   * the provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<Iterable<T>> iterableView(
      PCollection<KV<Void, T>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new IterableViewFn<T>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listView(
      PCollection<KV<Void, T>> pCollection,
      TypeDescriptorSupplier<T> typeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new ListViewFn<>(typeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }
  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements windowed using the
   * provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, V>> mapView(
      PCollection<KV<Void, KV<K, V>>> pCollection,
      TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
      TypeDescriptorSupplier<V> valueTypeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new MapViewFn<>(keyTypeDescriptorSupplier, valueTypeDescriptorSupplier),
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy);
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements windowed
   * using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>> multimapView(
      PCollection<KV<Void, KV<K, V>>> pCollection,
      TypeDescriptorSupplier<K> keyTypeDescriptorSupplier,
      TypeDescriptorSupplier<V> valueTypeDescriptorSupplier,
      WindowingStrategy<?, W> windowingStrategy) {
    return new SimplePCollectionView<>(
        pCollection,
        new MultimapViewFn<>(keyTypeDescriptorSupplier, valueTypeDescriptorSupplier),
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
   * Implementation which is able to adapt a multimap materialization to a {@code T}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#singletonView}.
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
  public static class SingletonViewFn<T> extends ViewFn<MultimapView<Void, T>, T> {
    @Nullable private byte[] encodedDefaultValue;
    @Nullable private transient T defaultValue;
    @Nullable private Coder<T> valueCoder;
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
   * Implementation which is able to adapt a multimap materialization to a {@code Iterable<T>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#iterableView}.
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
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
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
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
    public boolean equals(Object other) {
      return other instanceof ListViewFn;
    }

    @Override
    public int hashCode() {
      return ListViewFn.class.hashCode();
    }
  }

  /**
   * Implementation which is able to adapt a multimap materialization to a {@code Map<K,
   * Iterable<V>>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#multimapView}.
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
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
      // TODO: BEAM-3071 - fix this so that we aren't relying on Java equality and are
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
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
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
      // TODO: BEAM-3071 - fix this so that we aren't relying on Java equality and are
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
      if (windowingStrategy.getWindowFn() instanceof InvalidWindows) {
        throw new IllegalArgumentException("WindowFn of PCollectionView cannot be InvalidWindows");
      }
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

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return Collections.singletonMap(tag, pCollection);
    }
  }
}
