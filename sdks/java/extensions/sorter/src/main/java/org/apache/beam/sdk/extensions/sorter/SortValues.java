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

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code SortValues<PrimaryKeyT, SecondaryKeyT, ValueT>} takes a {@code PCollection<KV<PrimaryKeyT,
 * Iterable<KV<SecondaryKeyT, ValueT>>>>} with elements consisting of a primary key and iterables
 * over {@code <secondary key, value>} pairs, and returns a {@code PCollection<KV<PrimaryKeyT,
 * Iterable<KV<SecondaryKeyT, ValueT>>>} of the same elements but with values sorted by a secondary
 * key.
 *
 * <p>This transform ignores the primary key, there is no guarantee of any relationship between
 * elements with different primary keys. The primary key is explicit here only because this
 * transform is typically used on a result of a {@link GroupByKey} transform.
 *
 * <p>This transform sorts by lexicographic comparison of the byte representations of the secondary
 * keys and may write secondary key-value pairs to disk. In order to retrieve the byte
 * representations it requires the input PCollection to use a {@link KvCoder} for its input, an
 * {@link IterableCoder} for its input values and a {@link KvCoder} for its secondary key-value
 * pairs.
 */
public class SortValues<PrimaryKeyT, SecondaryKeyT, ValueT>
    extends PTransform<
        PCollection<KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>>,
        PCollection<KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>>> {

  private final BufferedExternalSorter.Options sorterOptions;

  private SortValues(BufferedExternalSorter.Options sorterOptions) {
    this.sorterOptions = sorterOptions;
  }

  /**
   * Returns a {@code SortValues<PrimaryKeyT, SecondaryKeyT, ValueT>} {@link PTransform}.
   *
   * @param <PrimaryKeyT> the type of the primary keys of the input and output {@code PCollection}s
   * @param <SecondaryKeyT> the type of the secondary (sort) keys of the input and output {@code
   *     PCollection}s
   * @param <ValueT> the type of the values of the input and output {@code PCollection}s
   */
  public static <PrimaryKeyT, SecondaryKeyT, ValueT>
      SortValues<PrimaryKeyT, SecondaryKeyT, ValueT> create(
          BufferedExternalSorter.Options sorterOptions) {
    return new SortValues<>(sorterOptions);
  }

  @Override
  public PCollection<KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>> expand(
      PCollection<KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>> input) {

    Coder<SecondaryKeyT> secondaryKeyCoder = getSecondaryKeyCoder(input.getCoder());
    try {
      secondaryKeyCoder.verifyDeterministic();
    } catch (Coder.NonDeterministicException e) {
      throw new IllegalStateException(
          "the secondary key coder of SortValues must be deterministic", e);
    }

    return input
        .apply(
            ParDo.of(
                new SortValuesDoFn<>(
                    sorterOptions, secondaryKeyCoder, getValueCoder(input.getCoder()))))
        .setCoder(input.getCoder());
  }

  /** Retrieves the {@link Coder} for the secondary key-value pairs. */
  @SuppressWarnings("unchecked")
  private static <PrimaryKeyT, SecondaryKeyT, ValueT>
      KvCoder<SecondaryKeyT, ValueT> getSecondaryKeyValueCoder(
          Coder<KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>> inputCoder) {
    if (!(inputCoder instanceof KvCoder)) {
      throw new IllegalStateException("SortValues requires its input to use KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>> kvCoder =
        (KvCoder<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>) inputCoder;

    if (!(kvCoder.getValueCoder() instanceof IterableCoder)) {
      throw new IllegalStateException(
          "SortValues requires the values be encoded with IterableCoder");
    }
    IterableCoder<KV<SecondaryKeyT, ValueT>> iterableCoder =
        (IterableCoder<KV<SecondaryKeyT, ValueT>>) kvCoder.getValueCoder();

    if (!(iterableCoder.getElemCoder() instanceof KvCoder)) {
      throw new IllegalStateException(
          "SortValues requires the secondary key-value pairs to use KvCoder");
    }
    return (KvCoder<SecondaryKeyT, ValueT>) iterableCoder.getElemCoder();
  }

  /** Retrieves the {@link Coder} for the secondary keys. */
  private static <PrimaryKeyT, SecondaryKeyT, ValueT> Coder<SecondaryKeyT> getSecondaryKeyCoder(
      Coder<KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>> inputCoder) {
    return getSecondaryKeyValueCoder(inputCoder).getKeyCoder();
  }

  /** Returns the {@code Coder} of the values associated with the secondary keys. */
  private static <PrimaryKeyT, SecondaryKeyT, ValueT> Coder<ValueT> getValueCoder(
      Coder<KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>> inputCoder) {
    return getSecondaryKeyValueCoder(inputCoder).getValueCoder();
  }

  private static <T> T elementOf(Coder<T> coder, byte[] bytes) throws CoderException {
    if (coder instanceof ByteArrayCoder) {
      return (T) bytes;
    }
    return CoderUtils.decodeFromByteArray(coder, bytes);
  }

  private static <T> byte[] bytesOf(Coder<T> coder, T element) throws CoderException {
    if (element instanceof byte[]) {
      return (byte[]) element;
    }
    return CoderUtils.encodeToByteArray(coder, element);
  }

  private static class SortValuesDoFn<PrimaryKeyT, SecondaryKeyT, ValueT>
      extends DoFn<
          KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>,
          KV<PrimaryKeyT, Iterable<KV<SecondaryKeyT, ValueT>>>> {
    private final BufferedExternalSorter.Options sorterOptions;
    private final Coder<SecondaryKeyT> keyCoder;
    private final Coder<ValueT> valueCoder;

    SortValuesDoFn(
        BufferedExternalSorter.Options sorterOptions,
        Coder<SecondaryKeyT> keyCoder,
        Coder<ValueT> valueCoder) {
      this.sorterOptions = sorterOptions;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<KV<SecondaryKeyT, ValueT>> records = c.element().getValue();

      try {
        Sorter sorter = BufferedExternalSorter.create(sorterOptions);
        for (KV<SecondaryKeyT, ValueT> record : records) {
          sorter.add(
              KV.of(bytesOf(keyCoder, record.getKey()), bytesOf(valueCoder, record.getValue())));
        }

        c.output(KV.of(c.element().getKey(), new DecodingIterable(sorter.sort())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private class DecodingIterable implements Iterable<KV<SecondaryKeyT, ValueT>> {
      final Iterable<KV<byte[], byte[]>> iterable;

      DecodingIterable(Iterable<KV<byte[], byte[]>> iterable) {
        this.iterable = iterable;
      }

      @Nonnull
      @Override
      public Iterator<KV<SecondaryKeyT, ValueT>> iterator() {
        return new DecodingIterator(iterable.iterator());
      }
    }

    private class DecodingIterator implements Iterator<KV<SecondaryKeyT, ValueT>> {
      final Iterator<KV<byte[], byte[]>> iterator;

      DecodingIterator(Iterator<KV<byte[], byte[]>> iterator) {
        this.iterator = iterator;
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public KV<SecondaryKeyT, ValueT> next() {
        KV<byte[], byte[]> next = iterator.next();
        try {
          SecondaryKeyT secondaryKey = elementOf(keyCoder, next.getKey());
          ValueT value = elementOf(valueCoder, next.getValue());
          return KV.of(secondaryKey, value);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Iterator does not support remove");
      }
    }
  }
}
