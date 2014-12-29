/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.coders;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.common.collect.Iterables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Properties for use in {@link Coder} tests. These are implemented with junit assertions
 * rather than as predicates for the sake of error messages.
 */
public class CoderProperties {

  /**
   * All the contexts, for use in test cases.
   */
   public static final List<Coder.Context> ALL_CONTEXTS = Arrays.asList(
       Coder.Context.OUTER, Coder.Context.NESTED);

  /**
   * Verifies that for the given {@link Coder<T>}, and values of
   * type {@code T}, if the values are equal then the encoded bytes are equal,
   * in any {@link Coder.Context}.
   */
  public static <T> void coderDeterministic(
      Coder<T> coder, T value1, T value2)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      coderDeterministicInContext(coder, context, value1, value2);
    }
  }

  /**
   * Verifies that for the given {@link Coder<T>}, {@link Coder.Context}, and values of
   * type {@code T}, if the values are equal then the encoded bytes are equal.
   */
  public static <T> void coderDeterministicInContext(
      Coder<T> coder, Coder.Context context, T value1, T value2)
      throws Exception {
    assumeThat(value1, equalTo(value2));
    assertThat(
        encode(coder, context, value1),
        equalTo(encode(coder, context, value2)));
  }

  /**
   * Verifies that for the given {@link Coder<T>},
   * and value of type {@code T}, encoding followed by decoding yields an
   * equal value of type {@code T}, in any {@link Coder.Context}.
   */
  public static <T> void coderDecodeEncodeEqual(
      Coder<T> coder, T value)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      coderDecodeEncodeEqualInContext(coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@link Coder<T>}, {@link Coder.Context},
   * and value of type {@code T}, encoding followed by decoding yields an
   * equal value of type {@code T}.
   */
  public static <T> void coderDecodeEncodeEqualInContext(
      Coder<T> coder, Coder.Context context, T value)
      throws Exception {
    assertThat(decodeEncode(coder, context, value), equalTo(value));
  }

  /**
   * Verifies that for the given {@link Coder<Collection<T>>},
   * and value of type {@code Collection<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in any {@link Coder.Context}.
   */
  public static <T, IT extends Collection<T>> void coderDecodeEncodeContentsEqual(
      Coder<IT> coder, IT value)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      coderDecodeEncodeContentsEqualInContext(coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@link Coder<Collection<T>>},
   * and value of type {@code Collection<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in the given {@link Coder.Context}.
   */
  @SuppressWarnings("unchecked")
  public static <T, CT extends Collection<T>> void coderDecodeEncodeContentsEqualInContext(
      Coder<CT> coder, Coder.Context context, CT value)
      throws Exception {
    // Matchers.containsInAnyOrder() requires at least one element
    Collection<T> result = decodeEncode(coder, context, value);
    if (value.isEmpty()) {
      assertThat(result, emptyIterable());
    } else {
      // This is the only Matchers.containInAnyOrder() overload that takes literal values
      assertThat(result, containsInAnyOrder((T[]) value.toArray()));
    }
  }

  /**
   * Verifies that for the given {@link Coder<Collection<T>>},
   * and value of type {@code Collection<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in any {@link Coder.Context}.
   */
  public static <T, IT extends Iterable<T>> void coderDecodeEncodeContentsInSameOrder(
      Coder<IT> coder, IT value)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.<T, IT>coderDecodeEncodeContentsInSameOrderInContext(
          coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@link Coder<Iterable<T>>},
   * and value of type {@code Iterable<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in the given {@link Coder.Context}.
   */
  @SuppressWarnings("unchecked")
  public static <T, IT extends Iterable<T>> void coderDecodeEncodeContentsInSameOrderInContext(
      Coder<IT> coder, Coder.Context context, IT value)
      throws Exception {
    Iterable<T> result = decodeEncode(coder, context, value);
    // Matchers.contains() requires at least one element
    if (Iterables.isEmpty(value)) {
      assertThat(result, emptyIterable());
    } else {
      // This is the only Matchers.contains() overload that takes literal values
      assertThat(result, contains((T[]) Iterables.toArray(value, Object.class)));
    }
  }

  public static <T> void coderSerializable(Coder<T> coder) {
    SerializableUtils.ensureSerializable(coder);
  }

  //////////////////////////////////////////////////////////////////////////

  private static <T> byte[] encode(
      Coder<T> coder, Coder.Context context, T value) throws CoderException, IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    coder.encode(value, os, context);
    return os.toByteArray();
  }

  private static <T> T decode(
      Coder<T> coder, Coder.Context context, byte[] bytes) throws CoderException, IOException {
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    return coder.decode(is, context);
  }

  private static <T> T decodeEncode(Coder<T> coder, Coder.Context context, T value)
      throws CoderException, IOException {
    return decode(coder, context, encode(coder, context, value));
  }
}
