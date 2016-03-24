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
import com.google.common.collect.Lists;

import org.joda.time.Duration;
import org.joda.time.ReadableDuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link DurationCoder}. */
@RunWith(JUnit4.class)
public class DurationCoderTest {

  private static final DurationCoder TEST_CODER = DurationCoder.of();
  private static final List<Long> TEST_MILLIS =
      Lists.newArrayList(0L, 1L, -1L, -255L, 256L, Long.MIN_VALUE, Long.MAX_VALUE);

  private static final List<ReadableDuration> TEST_VALUES;

  static {
    TEST_VALUES = Lists.newArrayList();
    for (long millis : TEST_MILLIS) {
      TEST_VALUES.add(Duration.millis(millis));
    }
  }

  @Test
  public void testBasicEncoding() throws Exception {
    for (ReadableDuration value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link com.google.cloud.dataflow.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AA",
      "AQ",
      "____________AQ",
      "gf7_________AQ",
      "gAI",
      "gICAgICAgICAAQ",
      "__________9_");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null ReadableDuration");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
