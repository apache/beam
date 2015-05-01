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

package com.google.cloud.dataflow.sdk.testing;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.NonDeterministicException;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.Serializer;
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
 * <p>
 * We serialize and deserialize the coder to make sure that any state information required by the
 * coder is preserved. This causes tests written such that coders that lose information during
 * serialization or change state during encoding/decoding will fail.
 */
public class CoderProperties {

  /**
   * All the contexts, for use in test cases.
   */
   public static final List<Coder.Context> ALL_CONTEXTS = Arrays.asList(
       Coder.Context.OUTER, Coder.Context.NESTED);

  /**
   * Verifies that for the given {@link Coder Coder<T>}, and values of
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
   * Verifies that for the given {@link Coder Coder<T>}, {@link Coder.Context}, and values of
   * type {@code T}, if the values are equal then the encoded bytes are equal.
   */
  public static <T> void coderDeterministicInContext(
      Coder<T> coder, Coder.Context context, T value1, T value2)
      throws Exception {

    try {
      coder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      fail("Expected that the coder is deterministic");
    }
    assertThat("Expected that the passed in values are equal()", value1, equalTo(value2));
    assertThat(
        encode(coder, context, value1),
        equalTo(encode(coder, context, value2)));
  }

  /**
   * Verifies that for the given {@link Coder Coder<T>},
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
   * Verifies that for the given {@link Coder Coder<T>}, {@link Coder.Context},
   * and value of type {@code T}, encoding followed by decoding yields an
   * equal value of type {@code T}.
   */
  public static <T> void coderDecodeEncodeEqualInContext(
      Coder<T> coder, Coder.Context context, T value)
      throws Exception {
    assertThat(decodeEncode(coder, context, value), equalTo(value));
  }

  /**
   * Verifies that for the given {@link Coder Coder<Collection<T>>},
   * and value of type {@code Collection<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in any {@link Coder.Context}.
   */
  public static <T, CollectionT extends Collection<T>> void coderDecodeEncodeContentsEqual(
      Coder<CollectionT> coder, CollectionT value)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      coderDecodeEncodeContentsEqualInContext(coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@link Coder Coder<Collection<T>>},
   * and value of type {@code Collection<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in the given {@link Coder.Context}.
   */
  @SuppressWarnings("unchecked")
  public static <T, CollectionT extends Collection<T>> void coderDecodeEncodeContentsEqualInContext(
      Coder<CollectionT> coder, Coder.Context context, CollectionT value)
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
   * Verifies that for the given {@link Coder Coder<Collection<T>>},
   * and value of type {@code Collection<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in any {@link Coder.Context}.
   */
  public static <T, IterableT extends Iterable<T>> void coderDecodeEncodeContentsInSameOrder(
      Coder<IterableT> coder, IterableT value)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.<T, IterableT>coderDecodeEncodeContentsInSameOrderInContext(
          coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@link Coder Coder<Iterable<T>>},
   * and value of type {@code Iterable<T>}, encoding followed by decoding yields an
   * equal value of type {@code Collection<T>}, in the given {@link Coder.Context}.
   */
  @SuppressWarnings("unchecked")
  public static <T, IterableT extends Iterable<T>> void
      coderDecodeEncodeContentsInSameOrderInContext(
          Coder<IterableT> coder, Coder.Context context, IterableT value)
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

  public static <T> void coderConsistentWithEquals(
      Coder<T> coder, T value1, T value2)
      throws Exception {

    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.<T>coderConsistentWithEqualsInContext(coder, context, value1, value2);
    }
  }

  public static <T> void coderConsistentWithEqualsInContext(
      Coder<T> coder, Coder.Context context, T value1, T value2) throws Exception {

    assertEquals(
        value1.equals(value2),
        Arrays.equals(
            encode(coder, context, value1),
            encode(coder, context, value2)));
  }

  public static <T> void structuralValueConsistentWithEquals(
      Coder<T> coder, T value1, T value2)
      throws Exception {

    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.<T>structuralValueConsistentWithEqualsInContext(
          coder, context, value1, value2);
    }
  }

  public static <T> void structuralValueConsistentWithEqualsInContext(
      Coder<T> coder, Coder.Context context, T value1, T value2) throws Exception {

    assertEquals(
        coder.structuralValue(value1).equals(coder.structuralValue(value2)),
        Arrays.equals(
            encode(coder, context, value1),
            encode(coder, context, value2)));
  }

  //////////////////////////////////////////////////////////////////////////

  private static <T> byte[] encode(
      Coder<T> coder, Coder.Context context, T value) throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    Coder<T> deserializedCoder = Serializer.deserialize(coder.asCloudObject(), Coder.class);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    deserializedCoder.encode(value, os, context);
    return os.toByteArray();
  }

  private static <T> T decode(
      Coder<T> coder, Coder.Context context, byte[] bytes) throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    Coder<T> deserializedCoder = Serializer.deserialize(coder.asCloudObject(), Coder.class);

    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    return deserializedCoder.decode(is, context);
  }

  private static <T> T decodeEncode(Coder<T> coder, Coder.Context context, T value)
      throws CoderException, IOException {
    return decode(coder, context, encode(coder, context, value));
  }
}
