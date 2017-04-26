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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MapCoder}. */
@RunWith(JUnit4.class)
public class MapCoderTest {

  private static final Coder<Map<Integer, String>> TEST_CODER =
      MapCoder.of(VarIntCoder.of(), StringUtf8Coder.of());

  private static final List<Map<Integer, String>> TEST_VALUES = Arrays.<Map<Integer, String>>asList(
      Collections.<Integer, String>emptyMap(),
      new TreeMap<>(new ImmutableMap.Builder<Integer, String>()
          .put(1, "hello").put(-1, "foo").build()));

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    for (Map<Integer, String> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(
        MapCoder.of(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
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

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AAAAAA",
      "AAAAAv____8PA2ZvbwFoZWxsbw");

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

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    TypeDescriptor<Map<Integer, String>> typeDescriptor =
        new TypeDescriptor<Map<Integer, String>>() {};
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(typeDescriptor));
  }
}
