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
package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UnownedInputStream;
import org.apache.beam.sdk.util.UnownedOutputStream;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingInputStream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingOutputStream;

/**
 * Properties for use in {@link Coder} tests. These are implemented with junit assertions rather
 * than as predicates for the sake of error messages.
 *
 * <p>We serialize and deserialize the coder to make sure that any state information required by the
 * coder is preserved. This causes tests written such that coders that lose information during
 * serialization or change state during encoding/decoding will fail.
 */
public class CoderProperties {

  /** All the contexts, for use in test cases. */
  public static final List<Coder.Context> ALL_CONTEXTS =
      ImmutableList.of(Coder.Context.OUTER, Coder.Context.NESTED);

  /**
   * Verifies that for the given {@code Coder<T>}, and values of type {@code T}, if the values are
   * equal then the encoded bytes are equal, in any {@code Coder.Context}.
   */
  public static <T> void coderDeterministic(Coder<T> coder, T value1, T value2) throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      coderDeterministicInContext(coder, context, value1, value2);
    }
  }

  /**
   * Verifies that for the given {@code Coder<T>}, {@code Coder.Context}, and values of type {@code
   * T}, if the values are equal then the encoded bytes are equal.
   */
  public static <T> void coderDeterministicInContext(
      Coder<T> coder, Coder.Context context, T value1, T value2) throws Exception {
    try {
      coder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new AssertionError("Expected that the coder is deterministic", e);
    }
    assertThat("Expected that the passed in values are equal()", value1, equalTo(value2));
    assertThat(encode(coder, context, value1), equalTo(encode(coder, context, value2)));
  }

  /**
   * Verifies that for the given {@code Coder<T>}, and value of type {@code T}, encoding followed by
   * decoding yields an equal value of type {@code T}, in any {@code Coder.Context}.
   */
  public static <T> void coderDecodeEncodeEqual(Coder<T> coder, T value) throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      coderDecodeEncodeEqualInContext(coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@code Coder<T>}, {@code Coder.Context}, and value of type {@code
   * T}, encoding followed by decoding yields an equal value of type {@code T}.
   */
  public static <T> void coderDecodeEncodeEqualInContext(
      Coder<T> coder, Coder.Context context, T value) throws Exception {
    assertThat(decodeEncode(coder, context, value), equalTo(value));
  }

  /**
   * Verifies that for the given {@code Coder<T>}, {@code Coder.Context}, and value of type {@code
   * T}, encoding followed by decoding yields a value of type {@code T} and tests that the matcher
   * succeeds on the values.
   */
  public static <T> void coderDecodeEncodeInContext(
      Coder<T> coder, Coder.Context context, T value, org.hamcrest.Matcher<T> matcher)
      throws Exception {
    assertThat(decodeEncode(coder, context, value), matcher);
  }

  /**
   * Verifies that for the given {@code Coder<Collection<T>>}, and value of type {@code
   * Collection<T>}, encoding followed by decoding yields an equal value of type {@code
   * Collection<T>}, in any {@code Coder.Context}.
   */
  public static <T, CollectionT extends Collection<T>> void coderDecodeEncodeContentsEqual(
      Coder<CollectionT> coder, CollectionT value) throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      coderDecodeEncodeContentsEqualInContext(coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@code Coder<Collection<T>>}, and value of type {@code
   * Collection<T>}, encoding followed by decoding yields an equal value of type {@code
   * Collection<T>}, in the given {@code Coder.Context}.
   */
  @SuppressWarnings("unchecked")
  public static <T, CollectionT extends Collection<T>> void coderDecodeEncodeContentsEqualInContext(
      Coder<CollectionT> coder, Coder.Context context, CollectionT value) throws Exception {
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
   * Verifies that for the given {@code Coder<Collection<T>>}, and value of type {@code
   * Collection<T>}, encoding followed by decoding yields an equal value of type {@code
   * Collection<T>}, in any {@code Coder.Context}.
   */
  public static <T, IterableT extends Iterable<T>> void coderDecodeEncodeContentsInSameOrder(
      Coder<IterableT> coder, IterableT value) throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.coderDecodeEncodeContentsInSameOrderInContext(coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@code Coder<Iterable<T>>}, and value of type {@code Iterable<T>},
   * encoding followed by decoding yields an equal value of type {@code Collection<T>}, in the given
   * {@code Coder.Context}.
   */
  @SuppressWarnings("unchecked")
  public static <T, IterableT extends Iterable<T>>
      void coderDecodeEncodeContentsInSameOrderInContext(
          Coder<IterableT> coder, Coder.Context context, IterableT value) throws Exception {
    Iterable<T> result = decodeEncode(coder, context, value);
    // Matchers.contains() requires at least one element
    if (Iterables.isEmpty(value)) {
      assertThat(result, emptyIterable());
    } else {
      // This is the only Matchers.contains() overload that takes literal values
      assertThat(result, contains((T[]) Iterables.toArray(value, Object.class)));
    }
  }

  /** Verifies that the given {@code Coder<T>} can be correctly serialized and deserialized. */
  public static <T> void coderSerializable(Coder<T> coder) {
    SerializableUtils.ensureSerializable(coder);
  }

  /**
   * Verifies that for the given {@code Coder<T>} and values of type {@code T}, the values are equal
   * if and only if the encoded bytes are equal.
   */
  public static <T> void coderConsistentWithEquals(Coder<T> coder, T value1, T value2)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.coderConsistentWithEqualsInContext(coder, context, value1, value2);
    }
  }

  /**
   * Verifies that for the given {@code Coder<T>}, {@code Coder.Context}, and values of type {@code
   * T}, the values are equal if and only if the encoded bytes are equal, in any {@code
   * Coder.Context}.
   */
  public static <T> void coderConsistentWithEqualsInContext(
      Coder<T> coder, Coder.Context context, T value1, T value2) throws Exception {
    assertEquals(
        value1.equals(value2),
        Arrays.equals(encode(coder, context, value1), encode(coder, context, value2)));
  }

  /**
   * Verifies that for the given {@code Coder<T>} and values of type {@code T}, the structural
   * values are equal if and only if the encoded bytes are equal.
   */
  public static <T> void structuralValueConsistentWithEquals(Coder<T> coder, T value1, T value2)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.structuralValueConsistentWithEqualsInContext(coder, context, value1, value2);
    }
  }

  /**
   * Verifies that for the given {@code Coder<T>}, {@code Coder.Context}, and values of type {@code
   * T}, the structural values are equal if and only if the encoded bytes are equal, in any {@code
   * Coder.Context}.
   */
  public static <T> void structuralValueConsistentWithEqualsInContext(
      Coder<T> coder, Coder.Context context, T value1, T value2) throws Exception {
    assertEquals(
        coder.structuralValue(value1).equals(coder.structuralValue(value2)),
        Arrays.equals(encode(coder, context, value1), encode(coder, context, value2)));
  }

  /**
   * Verifies that for the given {@code Coder<T>} and value of type {@code T}, the structural value
   * is equal to the structural value yield by encoding and decoding the original value.
   *
   * <p>This is useful to test the correct implementation of a Coder structural equality with values
   * that don't implement the equals contract.
   */
  public static <T> void structuralValueDecodeEncodeEqual(Coder<T> coder, T value)
      throws Exception {
    for (Coder.Context context : ALL_CONTEXTS) {
      CoderProperties.structuralValueDecodeEncodeEqualInContext(coder, context, value);
    }
  }

  /**
   * Verifies that for the given {@code Coder<T>}, {@code Coder.Context}, and value of type {@code
   * T}, the structural value is equal to the structural value yield by encoding and decoding the
   * original value, in any {@code Coder.Context}.
   */
  public static <T> void structuralValueDecodeEncodeEqualInContext(
      Coder<T> coder, Coder.Context context, T value) throws Exception {
    assertEquals(
        coder.structuralValue(value), coder.structuralValue(decodeEncode(coder, context, value)));
  }

  private static final String DECODING_WIRE_FORMAT_MESSAGE =
      "Decoded value from known wire format does not match expected value."
          + " This probably means that this Coder no longer correctly decodes"
          + " a prior wire format. Changing the wire formats this Coder can read"
          + " should be avoided, as it is likely to cause breakage.";

  public static <T> void coderDecodesBase64(Coder<T> coder, String base64Encoding, T value)
      throws Exception {
    assertThat(
        DECODING_WIRE_FORMAT_MESSAGE,
        CoderUtils.decodeFromBase64(coder, base64Encoding),
        equalTo(value));
  }

  public static <T> void coderDecodesBase64(
      Coder<T> coder, List<String> base64Encodings, List<T> values) throws Exception {
    assertThat(
        "List of base64 encodings has different size than List of values",
        base64Encodings.size(),
        equalTo(values.size()));

    for (int i = 0; i < base64Encodings.size(); i++) {
      coderDecodesBase64(coder, base64Encodings.get(i), values.get(i));
    }
  }

  private static final String ENCODING_WIRE_FORMAT_MESSAGE =
      "Encoded value does not match expected wire format."
          + " Changing the wire format should be avoided, as it is likely to cause breakage."
          + " If you truly intend to change the wire format for this Coder,"
          + " See org.apache.beam.sdk.coders.PrintBase64Encoding for how to generate"
          + " new test data.";

  public static <T> void coderEncodesBase64(Coder<T> coder, T value, String base64Encoding)
      throws Exception {
    assertThat(
        ENCODING_WIRE_FORMAT_MESSAGE,
        CoderUtils.encodeToBase64(coder, value),
        equalTo(base64Encoding));
  }

  public static <T> void coderEncodesBase64(
      Coder<T> coder, List<T> values, List<String> base64Encodings) throws Exception {
    assertThat(
        "List of base64 encodings has different size than List of values",
        base64Encodings.size(),
        equalTo(values.size()));

    for (int i = 0; i < base64Encodings.size(); i++) {
      coderEncodesBase64(coder, values.get(i), base64Encodings.get(i));
    }
  }

  @SuppressWarnings("unchecked")
  public static <T, IterableT extends Iterable<T>> void coderDecodesBase64ContentsEqual(
      Coder<IterableT> coder, String base64Encoding, IterableT expected) throws Exception {

    IterableT result = CoderUtils.decodeFromBase64(coder, base64Encoding);
    if (Iterables.isEmpty(expected)) {
      assertThat(ENCODING_WIRE_FORMAT_MESSAGE, result, emptyIterable());
    } else {
      assertThat(
          ENCODING_WIRE_FORMAT_MESSAGE,
          result,
          containsInAnyOrder((T[]) Iterables.toArray(expected, Object.class)));
    }
  }

  public static <T, IterableT extends Iterable<T>> void coderDecodesBase64ContentsEqual(
      Coder<IterableT> coder, List<String> base64Encodings, List<IterableT> expected)
      throws Exception {
    assertThat(
        "List of base64 encodings has different size than List of values",
        base64Encodings.size(),
        equalTo(expected.size()));

    for (int i = 0; i < base64Encodings.size(); i++) {
      coderDecodesBase64ContentsEqual(coder, base64Encodings.get(i), expected.get(i));
    }
  }

  //////////////////////////////////////////////////////////////////////////

  @VisibleForTesting
  static <T> byte[] encode(Coder<T> coder, Coder.Context context, T value)
      throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    Coder<T> deserializedCoder = SerializableUtils.clone(coder);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    deserializedCoder.encode(value, new UnownedOutputStream(os), context);
    return os.toByteArray();
  }

  @VisibleForTesting
  static <T> T decode(Coder<T> coder, Coder.Context context, byte[] bytes)
      throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    Coder<T> deserializedCoder = SerializableUtils.clone(coder);

    byte[] buffer;
    if (Objects.equals(context, Coder.Context.NESTED)) {
      buffer = new byte[bytes.length + 1];
      System.arraycopy(bytes, 0, buffer, 0, bytes.length);
      buffer[bytes.length] = 1;
    } else {
      buffer = bytes;
    }

    CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(buffer));
    T value = deserializedCoder.decode(new UnownedInputStream(cis), context);
    assertThat(
        "consumed bytes equal to encoded bytes", cis.getCount(), equalTo((long) bytes.length));
    return value;
  }

  private static <T> T decodeEncode(Coder<T> coder, Coder.Context context, T value)
      throws CoderException, IOException {
    return decode(coder, context, encode(coder, context, value));
  }

  /**
   * A utility method that passes the given (unencoded) elements through coder's
   * registerByteSizeObserver() and encode() methods, and confirms they are mutually consistent.
   * This is useful for testing coder implementations.
   */
  public static <T> void testByteCount(Coder<T> coder, Coder.Context context, T[] elements)
      throws Exception {
    TestElementByteSizeObserver observer = new TestElementByteSizeObserver();

    try (CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream())) {
      for (T elem : elements) {
        coder.registerByteSizeObserver(elem, observer);
        coder.encode(elem, os, context);
        observer.advance();
      }
      long expectedLength = os.getCount();

      if (!context.isWholeStream) {
        assertEquals(expectedLength, observer.getSum());
      }
      assertEquals(elements.length, observer.getCount());
    }
  }

  /**
   * An {@link ElementByteSizeObserver} that records the observed element sizes for testing
   * purposes.
   */
  public static class TestElementByteSizeObserver extends ElementByteSizeObserver {

    private long currentSum = 0;
    private long count = 0;

    @Override
    protected void reportElementSize(long elementByteSize) {
      count++;
      currentSum += elementByteSize;
    }

    public double getMean() {
      return ((double) currentSum) / count;
    }

    public long getSum() {
      return currentSum;
    }

    public long getCount() {
      return count;
    }

    public void reset() {
      currentSum = 0;
      count = 0;
    }

    public long getSumAndReset() {
      long returnValue = currentSum;
      reset();
      return returnValue;
    }
  }
}
