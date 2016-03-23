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
import java.util.List;

/**
 * Test case for {@link StringUtf8Coder}.
 */
@RunWith(JUnit4.class)
public class StringUtf8CoderTest {

  private static final Coder<String> TEST_CODER = StringUtf8Coder.of();

  private static final List<String> TEST_VALUES = Arrays.asList(
      "", "a", "13", "hello",
      "a longer string with spaces and all that",
      "a string with a \n newline",
      "スタリング");

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (String value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link com.google.cloud.dataflow.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "",
      "YQ",
      "MTM",
      "aGVsbG8",
      "YSBsb25nZXIgc3RyaW5nIHdpdGggc3BhY2VzIGFuZCBhbGwgdGhhdA",
      "YSBzdHJpbmcgd2l0aCBhIAogbmV3bGluZQ",
      "44K544K_44Oq44Oz44Kw");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null String");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
