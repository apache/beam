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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MutationDetectors}. */
@RunWith(JUnit4.class)
public class MutationDetectorsTest {
  /**
   * Solely used to test that immutability is enforced from the SDK's perspective and not from
   * Java's {@link Object#equals} method. Note that we do not expect users to create such an
   * implementation.
   */
  private static class ForSDKMutationDetectionTestCoder extends AtomicCoder<Object> {
    // Use a unique instance that is returned as the structural value making all structural
    // values of this coder equivalent to each other.
    private final Object uniqueInstance = new Object();

    @Override
    public void encode(Object value, OutputStream outStream) throws IOException {}

    @Override
    public Object decode(InputStream inStream) throws IOException {
      return new AtomicInteger();
    }

    @Override
    public Object structuralValue(Object value) {
      return uniqueInstance;
    }
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  /**
   * Tests that mutation detection is enforced from the SDK point of view (Based on the {@link
   * Coder#structuralValue}) and not from the Java's equals method.
   */
  @Test
  public void testMutationBasedOnStructuralValue() throws Exception {
    AtomicInteger value = new AtomicInteger();
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, new ForSDKMutationDetectionTestCoder());
    // Even though we modified the value, we are relying on the fact that the structural
    // value will be used to compare equality
    value.incrementAndGet();
    detector.verifyUnmodified();
  }

  @Test
  public void testMutationWithEqualEncodings() throws Exception {
    class EncodingBadStructuralValueCoder extends AtomicCoder<List<Object>> {
      @Override
      public void encode(List<Object> value, OutputStream outStream)
          throws CoderException, IOException {
        outStream.write(new byte[] {1, 2, -3, 45});
      }

      @Override
      public List<Object> decode(InputStream inStream) throws CoderException, IOException {
        // Consume the written bytes
        inStream.read(new byte[4]);
        return new ArrayList<>();
      }

      @Override
      public Object structuralValue(List<Object> value) {
        // Structural values are never equal to each other.
        return new Object();
      }
    }

    List<Object> ls = new ArrayList<>();
    ls.add(1);
    ls.add("foo");

    MutationDetector detector =
        MutationDetectors.forValueWithCoder(ls, new EncodingBadStructuralValueCoder());
    ls.add(new Byte[] {1, 2, -3, 45});

    // The structural values should be unequal, but the encoded bytes are equivalent, which is the
    // system definition of equality.
    detector.verifyUnmodified();
  }

  /** Tests that {@link MutationDetectors#forValueWithCoder} detects a mutation to a list. */
  @Test
  public void testMutatingList() throws Exception {
    List<Integer> value = Arrays.asList(1, 2, 3, 4);
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, ListCoder.of(VarIntCoder.of()));
    value.set(0, 37);

    thrown.expect(IllegalMutationException.class);
    detector.verifyUnmodified();
  }

  /**
   * Tests that {@link MutationDetectors#forValueWithCoder} does not false positive on a {@link
   * LinkedList} that will clone as an {@code ArrayList}.
   */
  @Test
  public void testUnmodifiedLinkedList() throws Exception {
    List<Integer> value = Lists.newLinkedList(Arrays.asList(1, 2, 3, 4));
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, ListCoder.of(VarIntCoder.of()));
    detector.verifyUnmodified();
  }

  /**
   * Tests that {@link MutationDetectors#forValueWithCoder} does not false positive on a {@link
   * LinkedList} coded as an {@link Iterable}.
   */
  @Test
  public void testImmutableList() throws Exception {
    List<Integer> value = Lists.newLinkedList(Arrays.asList(1, 2, 3, 4));
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, IterableCoder.of(VarIntCoder.of()));
    detector.verifyUnmodified();
  }

  /**
   * Tests that {@link MutationDetectors#forValueWithCoder} does not false positive on a {@link Set}
   * coded as an {@link Iterable}.
   */
  @Test
  public void testImmutableSet() throws Exception {
    Set<Integer> value = Sets.newHashSet(Arrays.asList(1, 2, 3, 4));
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, IterableCoder.of(VarIntCoder.of()));
    detector.verifyUnmodified();
  }

  /**
   * Tests that {@link MutationDetectors#forValueWithCoder} does not false positive on a {@link Set}
   * coded as an {@link Iterable}.
   */
  @Test
  public void testStructuralValue() throws Exception {
    Set<Integer> value = Sets.newHashSet(Arrays.asList(1, 2, 3, 4));
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, IterableCoder.of(VarIntCoder.of()));
    detector.verifyUnmodified();
  }

  /**
   * Tests that {@link MutationDetectors#forValueWithCoder} does not false positive on an {@link
   * Iterable} that is not known to be bounded; after coder-based cloning the bound will be known
   * and it will be a {@link List} so it will encode more compactly the second time around.
   */
  @Test
  public void testImmutableIterable() throws Exception {
    Iterable<Integer> value = FluentIterable.from(Arrays.asList(1, 2, 3, 4)).cycle().limit(50);
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, IterableCoder.of(VarIntCoder.of()));
    detector.verifyUnmodified();
  }

  /** Tests that {@link MutationDetectors#forValueWithCoder} detects a mutation to a byte array. */
  @Test
  public void testMutatingArray() throws Exception {
    byte[] value = new byte[] {0x1, 0x2, 0x3, 0x4};
    MutationDetector detector = MutationDetectors.forValueWithCoder(value, ByteArrayCoder.of());
    value[0] = 0xa;
    thrown.expect(IllegalMutationException.class);
    detector.verifyUnmodified();
  }

  /**
   * Tests that {@link MutationDetectors#forValueWithCoder} does not false positive on an array,
   * even though it will decode is another array which Java will not say is {@code equals}.
   */
  @Test
  public void testUnmodifiedArray() throws Exception {
    byte[] value = new byte[] {0x1, 0x2, 0x3, 0x4};
    MutationDetector detector = MutationDetectors.forValueWithCoder(value, ByteArrayCoder.of());
    detector.verifyUnmodified();
  }

  /**
   * Tests that {@link MutationDetectors#forValueWithCoder} does not false positive on an list of
   * arrays, even when some array is set to a deeply equal array that is not {@code equals}.
   */
  @Test
  public void testEquivalentListOfArrays() throws Exception {
    List<byte[]> value = Arrays.asList(new byte[] {0x1}, new byte[] {0x2, 0x3}, new byte[] {0x4});
    MutationDetector detector =
        MutationDetectors.forValueWithCoder(value, ListCoder.of(ByteArrayCoder.of()));
    value.set(0, new byte[] {0x1});
    detector.verifyUnmodified();
  }
}
