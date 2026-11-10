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

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueKind;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValueKindCoder}. */
@RunWith(JUnit4.class)
public class ValueKindCoderTest {

  private static final Coder<ValueKind> TEST_CODER = ValueKindCoder.of();

  private static final List<ValueKind> TEST_VALUES =
      Arrays.asList(
          ValueKind.INSERT, ValueKind.UPDATE_BEFORE, ValueKind.UPDATE_AFTER, ValueKind.DELETE);

  /** One byte per value, holding the proto enum number. */
  private static final List<String> TEST_ENCODINGS = Arrays.asList("AQ", "Ag", "Aw", "BA");

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (ValueKind value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  /** VALUE_KIND_UNSPECIFIED (0) means INSERT, for backwards compatibility. */
  @Test
  public void testDecodeUnspecified() throws Exception {
    assertEquals(ValueKind.INSERT, CoderUtils.decodeFromBase64(TEST_CODER, "AA"));
  }

  @Test
  public void testDecodeUnknownNumberThrows() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("Unknown ValueKind number: 42");

    CoderUtils.decodeFromBase64(TEST_CODER, "Kg");
  }

  @Test
  public void testCoderRegistryResolvesValueKind() throws Exception {
    assertEquals(
        ValueKindCoder.of(),
        CoderRegistry.createDefault().getCoder(TypeDescriptor.of(ValueKind.class)));
  }
}
