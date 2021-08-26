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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.util.Iterator;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link PTransform PTransforms} to count the elements in a {@link PCollection}.
 *
 * <p>{@link Count#perElement()} can be used to count the number of occurrences of each distinct
 * element in the PCollection, {@link Count#perKey()} can be used to count the number of values per
 * key, and {@link Count#globally()} can be used to count the total number of elements in a
 * PCollection.
 *
 * <p>{@link #combineFn} can also be used manually, in combination with state and with the {@link
 * Combine} transform.
 */
public class Count {
  private Count() {
    // do not instantiate
  }

  /** Returns a {@link CombineFn} that counts the number of its inputs. */
  public static <T> CombineFn<T, ?, Long> combineFn() {
    return new CountFn<>();
  }

  /**
   * Returns a {@link PTransform} that counts the number of elements in its input {@link
   * PCollection}.
   *
   * <p>Note: if the input collection uses a windowing strategy other than {@link GlobalWindows},
   * use {@code Combine.globally(Count.<T>combineFn()).withoutDefaults()} instead.
   */
  public static <T> PTransform<PCollection<T>, PCollection<Long>> globally() {
    return Combine.globally(new CountFn<T>());
  }

  /**
   * Returns a {@link PTransform} that counts the number of elements associated with each key of its
   * input {@link PCollection}.
   */
  public static <K, V> PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Long>>> perKey() {
    return Combine.perKey(new CountFn<V>());
  }

  /**
   * Returns a {@link PTransform} that counts the number of occurrences of each element in its input
   * {@link PCollection}.
   *
   * <p>The returned {@code PTransform} takes a {@code PCollection<T>} and returns a {@code
   * PCollection<KV<T, Long>>} representing a map from each distinct element of the input {@code
   * PCollection} to the number of times that element occurs in the input. Each key in the output
   * {@code PCollection} is unique.
   *
   * <p>The returned transform compares two values of type {@code T} by first encoding each element
   * using the input {@code PCollection}'s {@code Coder}, then comparing the encoded bytes. Because
   * of this, the input coder must be deterministic. (See {@link
   * org.apache.beam.sdk.coders.Coder#verifyDeterministic()} for more detail). Performing the
   * comparison in this manner admits efficient parallel evaluation.
   *
   * <p>By default, the {@code Coder} of the keys of the output {@code PCollection} is the same as
   * the {@code Coder} of the elements of the input {@code PCollection}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, Long>> wordCounts =
   *     words.apply(Count.<String>perElement());
   * }</pre>
   */
  public static <T> PTransform<PCollection<T>, PCollection<KV<T, Long>>> perElement() {
    return new PerElement<>();
  }

  /**
   * Private implementation of {@link #perElement()}.
   *
   * @param <T> the type of the elements of the input {@code PCollection}, and the type of the keys
   *     of the output {@code PCollection}
   */
  private static class PerElement<T> extends PTransform<PCollection<T>, PCollection<KV<T, Long>>> {

    private PerElement() {}

    @Override
    public PCollection<KV<T, Long>> expand(PCollection<T> input) {
      return input
          .apply(
              "Init",
              MapElements.via(
                  new SimpleFunction<T, KV<T, Void>>() {
                    @Override
                    public KV<T, Void> apply(T element) {
                      return KV.of(element, (Void) null);
                    }
                  }))
          .apply(Count.perKey());
    }
  }

  /** A {@link CombineFn} that counts elements. */
  private static class CountFn<T> extends CombineFn<T, long[], Long> {
    // Note that the long[] accumulator always has size 1, used as
    // a box for a mutable long.

    @Override
    public long[] createAccumulator() {
      return new long[] {0};
    }

    @Override
    public long[] addInput(long[] accumulator, T input) {
      accumulator[0] += 1;
      return accumulator;
    }

    @Override
    public long[] mergeAccumulators(Iterable<long[]> accumulators) {
      Iterator<long[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      }
      long[] running = iter.next();
      while (iter.hasNext()) {
        running[0] += iter.next()[0];
      }
      return running;
    }

    @Override
    public Long extractOutput(long[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<long[]> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return new AtomicCoder<long[]>() {
        @Override
        public void encode(long[] value, OutputStream outStream) throws IOException {
          VarInt.encode(value[0], outStream);
        }

        @Override
        public long[] decode(InputStream inStream) throws IOException, CoderException {
          try {
            return new long[] {VarInt.decodeLong(inStream)};
          } catch (EOFException | UTFDataFormatException exn) {
            throw new CoderException(exn);
          }
        }

        @Override
        public boolean isRegisterByteSizeObserverCheap(long[] value) {
          return true;
        }

        @Override
        protected long getEncodedElementByteSize(long[] value) {
          return VarInt.getLength(value[0]);
        }
      };
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other != null && getClass().equals(other.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }

    @Override
    public String getIncompatibleGlobalWindowErrorMessage() {
      return "If the input collection uses a windowing strategy other than GlobalWindows, "
          + "use Combine.globally(Count.<T>combineFn()).withoutDefaults() instead.";
    }
  }
}
