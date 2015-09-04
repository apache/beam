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

package com.google.cloud.dataflow.sdk.coders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Unit tests for {@link MapCoder}. */
@RunWith(JUnit4.class)
public class MapCoderTest {

  private static final Coder<Map<Integer, String>> TEST_CODER =
      MapCoder.of(VarIntCoder.of(), StringUtf8Coder.of());

  private static final List<Map<Integer, String>> TEST_VALUES = Arrays.<Map<Integer, String>>asList(
      Collections.<Integer, String>emptyMap(),
      new TreeMap<Integer, String>(new ImmutableMap.Builder<Integer, String>()
          .put(1, "hello").put(-1, "foo").build()));

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    for (Map<Integer, String> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testGetInstanceComponentsNonempty() {
    Map<Integer, String> map = new HashMap<>();
    map.put(17, "foozle");
    List<Object> components = MapCoder.getInstanceComponents(map);
    assertEquals(2, components.size());
    assertEquals(17, components.get(0));
    assertEquals("foozle", components.get(1));
  }

  @Test
  public void testGetInstanceComponentsEmpty() {
    Map<Integer, String> map = new HashMap<>();
    List<Object> components = MapCoder.getInstanceComponents(map);
    assertNull(components);
  }

  // If this changes, it implies the binary format has changed!
  private static final String EXPECTED_ENCODING_ID = "";

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(TEST_CODER, EXPECTED_ENCODING_ID);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link com.google.cloud.dataflow.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AAAAAA",
      "AAAAAv____8PA2ZvbwEFaGVsbG8");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Map");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
