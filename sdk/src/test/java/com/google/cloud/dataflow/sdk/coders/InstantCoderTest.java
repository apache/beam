/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.common.primitives.UnsignedBytes;

import org.joda.time.Instant;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Unit tests for {@link InstantCoder}. */
@RunWith(JUnit4.class)
public class InstantCoderTest {

  private final InstantCoder coder = InstantCoder.of();
  private final List<Long> timestamps =
      Arrays.asList(0L, 1L, -1L, -255L, 256L, Long.MIN_VALUE, Long.MAX_VALUE);

  @Test
  public void testBasicEncoding() throws Exception {
    for (long timestamp : timestamps) {
      CoderProperties.coderDecodeEncodeEqual(coder, new Instant(timestamp));
    }
  }

  @Test
  public void testOrderedEncoding() throws Exception {
    List<Long> sortedTimestamps = new ArrayList<>(timestamps);
    Collections.sort(sortedTimestamps);

    List<byte[]> encodings = new ArrayList<>(sortedTimestamps.size());
    for (long timestamp : sortedTimestamps) {
      encodings.add(CoderUtils.encodeToByteArray(coder, new Instant(timestamp)));
    }

    // Verify that the encodings were already sorted, since they were generated
    // in the correct order.
    List<byte[]> sortedEncodings = new ArrayList<>(encodings);
    Collections.sort(sortedEncodings, UnsignedBytes.lexicographicalComparator());

    Assert.assertEquals(encodings, sortedEncodings);
  }
}
