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

import java.util.Iterator;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link PTransform PTransforms} for converting a {@link PCollection PCollection&lt;?&gt;},
 * {@link PCollection PCollection&lt;KV&lt;?,?&gt;&gt;}, or
 * {@link PCollection PCollection&lt;Iterable&lt;?&gt;&gt;}
 * to a {@link PCollection PCollection&lt;String&gt;}.
 *
 * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your own
 * {@link SerializableFunction} using {@link MapElements#via(SerializableFunction)}
 */
public final class ToString {
  private ToString() {
    // do not instantiate
  }

  /**
   * Returns a {@code PTransform<PCollection, PCollection<String>>} which transforms each
   * element of the input {@link PCollection} to a {@link String} using the
   * {@link Object#toString} method.
   */
  public static PTransform<PCollection<?>, PCollection<String>> elements() {
    return new SimpleToString();
  }

  /**
   * Returns a {@code PTransform<PCollection<KV<?,?>, PCollection<String>>} which transforms each
   * element of the input {@link PCollection} to a {@link String} by using the
   * {@link Object#toString} on the key followed by a "," followed by the {@link Object#toString}
   * of the value.
   */
  public static PTransform<PCollection<? extends KV<?, ?>>, PCollection<String>> kv() {
    return kv(",");
  }

  /**
   * Returns a {@code PTransform<PCollection<KV<?,?>, PCollection<String>>} which transforms each
   * element of the input {@link PCollection} to a {@link String} by using the
   * {@link Object#toString} on the key followed by the specified delimeter followed by the
   * {@link Object#toString} of the value.
   * @param delimiter The delimiter to put between the key and value
   */
  public static PTransform<PCollection<? extends KV<?, ?>>,
          PCollection<String>> kv(String delimiter) {
    return new KVToString(delimiter);
  }

  /**
   * Returns a {@code PTransform<PCollection<Iterable<?>, PCollection<String>>} which
   * transforms each item in the iterable of the input {@link PCollection} to a {@link String}
   * using the {@link Object#toString} method followed by a "," until
   * the last element in the iterable. There is no trailing delimiter.
   */
  public static PTransform<PCollection<? extends Iterable<?>>, PCollection<String>> iterable() {
    return iterable(",");
  }

  /**
   * Returns a {@code PTransform<PCollection<Iterable<?>, PCollection<String>>} which
   * transforms each item in the iterable of the input {@link PCollection} to a {@link String}
   * using the {@link Object#toString} method followed by the specified delimiter until
   * the last element in the iterable. There is no trailing delimiter.
   * @param delimiter The delimiter to put between the items in the iterable.
   */
  public static PTransform<PCollection<? extends Iterable<?>>,
          PCollection<String>> iterable(String delimiter) {
    return new IterablesToString(delimiter);
  }

  /**
   * A {@link PTransform} that converts a {@code PCollection} to a {@code PCollection<String>}
   * using the {@link  Object#toString} method.
   *
   * <p>Example of use:
   * <pre>{@code
   * PCollection<Long> longs = ...;
   * PCollection<String> strings = longs.apply(ToString.elements());
   * }</pre>
   *
   *
   * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your own
   * {@link SerializableFunction} using {@link MapElements#via(SerializableFunction)}
   */
  private static final class SimpleToString extends
          PTransform<PCollection<?>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<?> input) {
      return input.apply(MapElements.via(new SimpleFunction<Object, String>() {
        @Override
        public String apply(Object input) {
          return input.toString();
        }
      }));
    }
  }

  /**
   * A {@link PTransform} that converts a {@code PCollection} of {@code KV} to a
   * {@code PCollection<String>} using the {@link  Object#toString} method for
   * the key and value and an optional delimiter.
   *
   * <p>Example of use:
   * <pre>{@code
   * PCollection<KV<String, Long>> nameToLong = ...;
   * PCollection<String> strings = nameToLong.apply(ToString.kv());
   * }</pre>
   *
   *
   * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your
   * own {@link SerializableFunction} using {@link MapElements#via(SerializableFunction)}
   */
  private static final class KVToString extends
          PTransform<PCollection<? extends KV<?, ?>>, PCollection<String>> {
    private final String delimiter;

    public KVToString(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public PCollection<String> expand(PCollection<? extends KV<?, ?>> input) {
      return input.apply(MapElements.via(new SimpleFunction<KV<?, ?>, String>() {
        @Override
        public String apply(KV<?, ?> input) {
          return input.getKey().toString() + delimiter + input.getValue().toString();
        }
      }));
    }
  }

  /**
   * A {@link PTransform} that converts a {@code PCollection} of {@link Iterable} to a
   * {@code PCollection<String>} using the {@link  Object#toString} method and
   * an optional delimiter.
   *
   * <p>Example of use:
   * <pre>{@code
   * PCollection<Iterable<Long>> longs = ...;
   * PCollection<String> strings = nameToLong.apply(ToString.iterable());
   * }</pre>
   *
   *
   * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your
   * own {@link SerializableFunction} using {@link MapElements#via(SerializableFunction)}
   */
  private static final class IterablesToString extends
          PTransform<PCollection<? extends Iterable<?>>, PCollection<String>> {
    private final String delimiter;

    public IterablesToString(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public PCollection<String> expand(PCollection<? extends Iterable<?>> input) {
      return input.apply(MapElements.via(new SimpleFunction<Iterable<?>, String>() {
        @Override
        public String apply(Iterable<?> input) {
          StringBuilder builder = new StringBuilder();
          Iterator iterator = input.iterator();

          while (iterator.hasNext()) {
            builder.append(iterator.next().toString());

            if (iterator.hasNext()) {
              builder.append(delimiter);
            }
          }

          return builder.toString();
        }
      }));
    }
  }
}
