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
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList(
          "AA", "AQ", "____________AQ", "gf7_________AQ", "gAI", "gICAgICAgICAAQ", "__________9_");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null ReadableDuration");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(
        TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(ReadableDuration.class)));
  }

  @Test
  public void testStructuralValueReturnTheSameValue() {
    ReadableDuration expected = Duration.millis(3_000);
    Object actual = TEST_CODER.structuralValue(expected);
    assertEquals(expected, actual);
  }
}
