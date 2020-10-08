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
package org.apache.beam.sdk.extensions.sorter;

import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.sorter.MergeSortCombineFn.SortingAccumulator;
import org.apache.beam.sdk.extensions.sorter.MergeSortCombineFn.SortingAccumulator.SortingAccumulatorCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/**
 * An implementation of a distributed sorter that works by accumulating inputs while maintaining
 * their sorted order according to a sort key K. Accumulators are merged lazily to maintain sorted
 * order every time the input is reduced. The sort key K is extracted from each input V when it is
 * added to the accumulator.
 *
 * @param <K> the class of the sort key
 * @param <V> the class of the input PCollection elements
 */
public class MergeSortCombineFn<K, V> extends CombineFn<V, SortingAccumulator, Iterable<V>> {
  private final Coder<K> sortKeyCoder;
  private final Coder<V> valueCoder;
  private final SerializableFunction<V, K> sortKeyFn;

  public static <KeyT, ValueT> MergeSortCombineFn<KeyT, ValueT> of(
      Coder<ValueT> valueCoder,
      Coder<KeyT> sortKeyCoder,
      SerializableFunction<ValueT, KeyT> sortKeyFn) {
    return new MergeSortCombineFn<>(valueCoder, sortKeyCoder, sortKeyFn);
  }

  private MergeSortCombineFn(
      Coder<V> valueCoder, Coder<K> sortKeyCoder, SerializableFunction<V, K> sortKeyFn) {
    this.valueCoder = valueCoder;
    this.sortKeyCoder = sortKeyCoder;
    this.sortKeyFn = sortKeyFn;
  }

  public static <KeyT, SortingKeyT, ValueT> Combine.PerKey<KeyT, ValueT, Iterable<ValueT>> perKey(
      Coder<ValueT> valueCoder,
      Coder<SortingKeyT> sortKeyCoder,
      SerializableFunction<ValueT, SortingKeyT> sortKeyFn) {
    return Combine.perKey(MergeSortCombineFn.of(valueCoder, sortKeyCoder, sortKeyFn));
  }

  public static <KeyT, SortingKeyT, ValueT>
      Combine.GroupedValues<KeyT, ValueT, Iterable<ValueT>> groupedValues(
          Coder<ValueT> valueCoder,
          Coder<SortingKeyT> sortKeyCoder,
          SerializableFunction<ValueT, SortingKeyT> sortKeyFn) {
    return Combine.groupedValues(MergeSortCombineFn.of(valueCoder, sortKeyCoder, sortKeyFn));
  }

  @Override
  public Coder<SortingAccumulator> getAccumulatorCoder(
      CoderRegistry registry, Coder<V> inputCoder) {
    return SortingAccumulatorCoder.of();
  }

  @Override
  public SortingAccumulator createAccumulator() {
    return new SortingAccumulator();
  }

  @Override
  public SortingAccumulator addInput(SortingAccumulator mutableAccumulator, V input) {
    final K sortKey = sortKeyFn.apply(input);
    try {
      mutableAccumulator.add(
          KV.of(
              CoderUtils.encodeToByteArray(sortKeyCoder, sortKey),
              CoderUtils.encodeToByteArray(valueCoder, input)));
    } catch (CoderException e) {
      throw new RuntimeException("Caught exception encoding input", e);
    }
    return mutableAccumulator;
  }

  @Override
  public SortingAccumulator mergeAccumulators(Iterable<SortingAccumulator> accumulators) {
    return new SortingAccumulator(new MergeSortingIterable(accumulators));
  }

  // Lazily decodes each byte[] pair back to its original class V.
  @Override
  public Iterable<V> extractOutput(SortingAccumulator accumulator) {
    return () ->
        Iterators.transform(
            accumulator.iterator(),
            (KV<byte[], byte[]> item) -> {
              if (item == null) {
                throw new RuntimeException("Found unexpected null value in encoded output");
              }

              try {
                return CoderUtils.decodeFromByteArray(valueCoder, item.getValue());
              } catch (CoderException e) {
                throw new RuntimeException("Caught decoding exception", e);
              }
            });
  }

  // An iterator that maintains a byte[] ordering across its objects.
  static class MergeSortingIterable implements Iterable<KV<byte[], byte[]>> {
    private final List<Iterator<KV<byte[], byte[]>>> sources;

    MergeSortingIterable(Iterable<SortingAccumulator> accumulators) {
      sources = new ArrayList<>();
      accumulators.forEach(a -> sources.add(a.materialized().iterator()));
    }

    @Override
    public Iterator<KV<byte[], byte[]>> iterator() {
      return Iterators.mergeSorted(sources, SortingAccumulator.KV_COMPARATOR);
    }
  }

  /** Accumulator of KV<byte[], byte[]> pairs that maintains them in a lexicographical order. */
  static class SortingAccumulator implements Iterable<KV<byte[], byte[]>> {
    private Iterable<KV<byte[], byte[]>> sortedItems;
    private boolean isMaterialized;

    static final Comparator<KV<byte[], byte[]>> KV_COMPARATOR =
        (kv1, kv2) -> UnsignedBytes.lexicographicalComparator().compare(kv1.getKey(), kv2.getKey());

    SortingAccumulator() {
      this.sortedItems = new ArrayList<>();
      this.isMaterialized = true;
    }

    // Items are always sorted during ser/de and when accumulators merge, so we know this
    // Iterable is sorted
    SortingAccumulator(Iterable<KV<byte[], byte[]>> sortedItems) {
      this.sortedItems = sortedItems;
      this.isMaterialized = List.class.isAssignableFrom(sortedItems.getClass());
    }

    // Returns a possibly lazily evaluating iterator.
    @Override
    public Iterator<KV<byte[], byte[]>> iterator() {
      return sortedItems.iterator();
    }

    // Eagerly materializes items into a List.
    @SuppressWarnings(("unchecked"))
    List<KV<byte[], byte[]>> materialized() {
      if (!isMaterialized) {
        sortedItems = Lists.newArrayList(iterator());
        isMaterialized = true;
      }

      return (List<KV<byte[], byte[]>>) sortedItems;
    }

    // Merge a new item into the sorted list
    void add(KV<byte[], byte[]> item) {
      final int itemCount = materialized().size();
      boolean isAdded = false;
      for (int i = 0; i < itemCount; i++) {
        if (KV_COMPARATOR.compare(item, materialized().get(i)) <= 0) {
          materialized().add(i, item);
          isAdded = true;
          break;
        }
      }
      if (!isAdded) { // If we've reached end of list, append to tail
        materialized().add(item);
      }
    }

    static class SortingAccumulatorCoder extends AtomicCoder<SortingAccumulator> {
      private static final Coder<List<KV<byte[], byte[]>>> LIST_CODER =
          ListCoder.of(KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()));
      private static final Coder<Boolean> BOOLEAN_CODER = BooleanCoder.of();

      private static final SortingAccumulatorCoder INSTANCE = new SortingAccumulatorCoder();

      static SortingAccumulatorCoder of() {
        return INSTANCE;
      }

      @Override
      public void encode(SortingAccumulator value, OutputStream outStream) throws IOException {
        boolean isEmpty = (!value.sortedItems.iterator().hasNext());
        BOOLEAN_CODER.encode(isEmpty, outStream);

        // Always encode the materialized, sorted list
        if (!isEmpty) {
          LIST_CODER.encode(value.materialized(), outStream);
        }
      }

      @Override
      public SortingAccumulator decode(InputStream inStream) throws IOException {
        final boolean isEmpty = BOOLEAN_CODER.decode(inStream);

        if (!isEmpty) { // If items have already been added
          return new SortingAccumulator(LIST_CODER.decode(inStream));
        } else {
          return new SortingAccumulator();
        }
      }
    }
  }
}
