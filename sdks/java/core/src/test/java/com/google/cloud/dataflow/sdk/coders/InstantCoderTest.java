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
import com.google.common.primitives.UnsignedBytes;

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Unit tests for {@link InstantCoder}. */
@RunWith(JUnit4.class)
public class InstantCoderTest {

  private static final InstantCoder TEST_CODER = InstantCoder.of();

  private static final List<Long> TEST_TIMESTAMPS =
      Arrays.asList(0L, 1L, -1L, -255L, 256L, Long.MIN_VALUE, Long.MAX_VALUE);

  private static final List<Instant> TEST_VALUES;

  static {
    TEST_VALUES = Lists.newArrayList();
    for (long timestamp : TEST_TIMESTAMPS) {
      TEST_VALUES.add(new Instant(timestamp));
    }
  }

  @Test
  public void testBasicEncoding() throws Exception {
    for (Instant value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testOrderedEncoding() throws Exception {
    List<Long> sortedTimestamps = new ArrayList<>(TEST_TIMESTAMPS);
    Collections.sort(sortedTimestamps);

    List<byte[]> encodings = new ArrayList<>(sortedTimestamps.size());
    for (long timestamp : sortedTimestamps) {
      encodings.add(CoderUtils.encodeToByteArray(TEST_CODER, new Instant(timestamp)));
    }

    // Verify that the encodings were already sorted, since they were generated
    // in the correct order.
    List<byte[]> sortedEncodings = new ArrayList<>(encodings);
    Collections.sort(sortedEncodings, UnsignedBytes.lexicographicalComparator());

    Assert.assertEquals(encodings, sortedEncodings);
  }

  // If this changes, it implies that the binary format has changed.
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
      "gAAAAAAAAAA",
      "gAAAAAAAAAE",
      "f_________8",
      "f________wE",
      "gAAAAAAAAQA",
      "AAAAAAAAAAA",
      "__________8");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Instant");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
