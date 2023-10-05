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
package org.apache.beam.sdk.coders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link DequeCoder}. */
@RunWith(JUnit4.class)
public class DequeCoderTest {

  private static final Coder<Deque<Integer>> TEST_CODER = DequeCoder.of(VarIntCoder.of());

  private static final List<Deque<Integer>> TEST_VALUES =
      ImmutableList.of(
          new ArrayDeque<>(),
          new ArrayDeque<>(Collections.singleton(13)),
          new ArrayDeque<>(ImmutableList.of(31, -5, 83)));

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(DequeCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testDecodeEncodeContentsEqual() throws Exception {
    for (Deque<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeContentsEqual(TEST_CODER, value);
    }
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      ImmutableList.of("AAAAAA", "AAAAAQ0", "AAAAAx_7____D1M");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Deque");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void structuralValueDecodeEncodeEqualIterable() throws Exception {
    DequeCoder<byte[]> coder = DequeCoder.of(ByteArrayCoder.of());
    Deque<byte[]> value = new ArrayDeque<>(Collections.singletonList(new byte[] {1, 2, 3, 4}));
    CoderProperties.structuralValueDecodeEncodeEqualIterable(coder, value);
  }

  @Test
  public void encodeDequeWithList() throws Exception {
    DequeCoder<List<Long>> listListLongCoder = DequeCoder.of(ListCoder.of(VarLongCoder.of()));

    CoderProperties.coderDecodeEncodeContentsEqual(
        listListLongCoder,
        new ArrayDeque<>(
            ImmutableList.of(
                ImmutableList.of(18L, 15L), ImmutableList.of(19L, 25L), ImmutableList.of(22L))));
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    TypeDescriptor<Deque<Integer>> typeDescriptor = new TypeDescriptor<Deque<Integer>>() {};
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(typeDescriptor));
  }
}
