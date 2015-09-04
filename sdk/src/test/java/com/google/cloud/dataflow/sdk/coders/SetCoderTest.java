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

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Test case for {@link SetCoder}.
 */
@RunWith(JUnit4.class)
public class SetCoderTest {

  private static final Coder<Set<Integer>> TEST_CODER = SetCoder.of(VarIntCoder.of());

  private static final List<Set<Integer>> TEST_VALUES = Arrays.<Set<Integer>>asList(
      Collections.<Integer>emptySet(),
      Collections.singleton(13),
      new TreeSet<>(Arrays.asList(31, -5, 83)));

  @Test
  public void testDecodeEncodeContentsEqual() throws Exception {
    for (Set<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeContentsEqual(TEST_CODER, value);
    }
  }

  // If this changes, it implies the binary format has changed.
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
      "AAAAAQ0",
      "AAAAA_v___8PH1M");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Set");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
