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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ListCoder}. */
@RunWith(JUnit4.class)
public class ListCoderTest {

  private static final Coder<List<Integer>> TEST_CODER = ListCoder.of(VarIntCoder.of());

  private static final List<List<Integer>> TEST_VALUES = Arrays.<List<Integer>>asList(
      Collections.<Integer>emptyList(),
      Collections.singletonList(43),
      Arrays.asList(1, 2, 3, 4),
      new LinkedList<Integer>(Arrays.asList(7, 6, 5)));

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    for (List<Integer> value : TEST_VALUES) {
      CoderProperties.<Integer, List<Integer>>coderDecodeEncodeContentsInSameOrder(
          TEST_CODER, value);
    }
  }

  @Test
  public void testGetInstanceComponentsNonempty() throws Exception {
    List<Integer> list = Arrays.asList(21, 5, 3, 5);
    List<Object> components = ListCoder.getInstanceComponents(list);
    assertEquals(1, components.size());
    assertEquals(21, components.get(0));
  }

  @Test
  public void testGetInstanceComponentsEmpty() throws Exception {
    List<Integer> list = Arrays.asList();
    List<Object> components = ListCoder.getInstanceComponents(list);
    assertNull(components);
  }

  @Test
  public void testEmptyList() throws Exception {
    List<Integer> list = Collections.emptyList();
    Coder<List<Integer>> coder = ListCoder.of(VarIntCoder.of());
    CoderProperties.<List<Integer>>coderDecodeEncodeEqual(coder, list);
  }

  @Test
  public void testCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  // If this changes, it implies the binary format has changed.
  private static final String EXPECTED_ENCODING_ID = "";

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(TEST_CODER, EXPECTED_ENCODING_ID);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AAAAAA",
      "AAAAASs",
      "AAAABAECAwQ",
      "AAAAAwcGBQ");

  @Test
  public void testWireFormatEncode() throws Exception {
      CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null List");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testListWithNullsAndVarIntCoderThrowsException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Integer");

    List<Integer> list = Arrays.asList(1, 2, 3, null, 4);
    Coder<List<Integer>> coder = ListCoder.of(VarIntCoder.of());
    CoderProperties.<List<Integer>>coderDecodeEncodeEqual(coder, list);
  }

  @Test
  public void testListWithNullsAndSerializableCoder() throws Exception {
    List<Integer> list = Arrays.asList(1, 2, 3, null, 4);
    Coder<List<Integer>> coder = ListCoder.of(SerializableCoder.of(Integer.class));
    CoderProperties.<List<Integer>>coderDecodeEncodeEqual(coder, list);
  }

}
