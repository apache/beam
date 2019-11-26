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

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;

/**
 * {@link PTransform PTransforms} for converting a {@link PCollection PCollection&lt;?&gt;}, {@link
 * PCollection PCollection&lt;KV&lt;?,?&gt;&gt;}, or {@link PCollection
 * PCollection&lt;Iterable&lt;?&gt;&gt;} to a {@link PCollection PCollection&lt;String&gt;}.
 *
 * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your own
 * {@link ProcessFunction} using {@link MapElements#via(ProcessFunction)}
 */
public final class ToString {
  private ToString() {
    // do not instantiate
  }

  /**
   * Transforms each element of the input {@link PCollection} to a {@link String} using the {@link
   * Object#toString} method.
   */
  public static PTransform<PCollection<?>, PCollection<String>> elements() {
    return new Elements();
  }

  /**
   * Transforms each element of the input {@link PCollection} to a {@link String} by using the
   * {@link Object#toString} on the key followed by a "," followed by the {@link Object#toString} of
   * the value.
   */
  public static PTransform<PCollection<? extends KV<?, ?>>, PCollection<String>> kvs() {
    return kvs(",");
  }

  /**
   * Transforms each element of the input {@link PCollection} to a {@link String} by using the
   * {@link Object#toString} on the key followed by the specified delimiter followed by the {@link
   * Object#toString} of the value.
   *
   * @param delimiter The delimiter to put between the key and value
   */
  public static PTransform<PCollection<? extends KV<?, ?>>, PCollection<String>> kvs(
      String delimiter) {
    return new KVs(delimiter);
  }

  /**
   * Transforms each item in the iterable of the input {@link PCollection} to a {@link String} using
   * the {@link Object#toString} method followed by a "," until the last element in the iterable.
   * There is no trailing delimiter.
   */
  public static PTransform<PCollection<? extends Iterable<?>>, PCollection<String>> iterables() {
    return iterables(",");
  }

  /**
   * Transforms each item in the iterable of the input {@link PCollection} to a {@link String} using
   * the {@link Object#toString} method followed by the specified delimiter until the last element
   * in the iterable. There is no trailing delimiter.
   *
   * @param delimiter The delimiter to put between the items in the iterable.
   */
  public static PTransform<PCollection<? extends Iterable<?>>, PCollection<String>> iterables(
      String delimiter) {
    return new Iterables(delimiter);
  }

  /**
   * A {@link PTransform} that converts a {@code PCollection} to a {@code PCollection<String>} using
   * the {@link Object#toString} method.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Long> longs = ...;
   * PCollection<String> strings = longs.apply(ToString.elements());
   * }</pre>
   *
   * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your own
   * {@link ProcessFunction} using {@link MapElements#via(ProcessFunction)}
   */
  private static final class Elements extends PTransform<PCollection<?>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<?> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<Object, String>() {
                @Override
                public String apply(Object input) {
                  return input.toString();
                }
              }));
    }
  }

  /**
   * A {@link PTransform} that converts a {@code PCollection} of {@code KV} to a {@code
   * PCollection<String>} using the {@link Object#toString} method for the key and value and an
   * optional delimiter.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<String, Long>> nameToLong = ...;
   * PCollection<String> strings = nameToLong.apply(ToString.kv());
   * }</pre>
   *
   * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your own
   * {@link ProcessFunction} using {@link MapElements#via(ProcessFunction)}
   */
  private static final class KVs
      extends PTransform<PCollection<? extends KV<?, ?>>, PCollection<String>> {
    private final String delimiter;

    public KVs(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public PCollection<String> expand(PCollection<? extends KV<?, ?>> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<KV<?, ?>, String>() {
                @Override
                public String apply(KV<?, ?> input) {
                  return input.getKey().toString() + delimiter + input.getValue().toString();
                }
              }));
    }
  }

  /**
   * A {@link PTransform} that converts a {@code PCollection} of {@link Iterable} to a {@code
   * PCollection<String>} using the {@link Object#toString} method and an optional delimiter.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Iterable<Long>> longs = ...;
   * PCollection<String> strings = nameToLong.apply(ToString.iterable());
   * }</pre>
   *
   * <p><b>Note</b>: For any custom string conversion and formatting, we recommend applying your own
   * {@link ProcessFunction} using {@link MapElements#via(ProcessFunction)}
   */
  private static final class Iterables
      extends PTransform<PCollection<? extends Iterable<?>>, PCollection<String>> {
    private final String delimiter;

    public Iterables(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public PCollection<String> expand(PCollection<? extends Iterable<?>> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<Iterable<?>, String>() {
                @Override
                public String apply(Iterable<?> input) {
                  return Joiner.on(delimiter).join(input);
                }
              }));
    }
  }
}
