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
package org.apache.beam.sdk.util;

import static org.apache.commons.lang3.StringUtils.countMatches;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for StringUtils. */
@RunWith(JUnit4.class)
public class StringUtilsTest {

  @Test
  public void testTranscodeEmptyByteArray() {
    byte[] bytes = {};
    String string = "";
    assertEquals(string, StringUtils.byteArrayToJsonString(bytes));
    assertArrayEquals(bytes, StringUtils.jsonStringToByteArray(string));
  }

  @Test
  public void testTranscodeMixedByteArray() {
    byte[] bytes = {0, 5, 12, 16, 31, 32, 65, 66, 126, 127, (byte) 128, (byte) 255, 67, 0};
    String string = "%00%05%0c%10%1f AB~%7f%80%ffC%00";
    assertEquals(string, StringUtils.byteArrayToJsonString(bytes));
    assertArrayEquals(bytes, StringUtils.jsonStringToByteArray(string));
  }

  @Test
  public void testLevenshteinDistance() {
    assertEquals(0, StringUtils.getLevenshteinDistance("", "")); // equal
    assertEquals(3, StringUtils.getLevenshteinDistance("", "abc")); // first empty
    assertEquals(3, StringUtils.getLevenshteinDistance("abc", "")); // second empty
    assertEquals(5, StringUtils.getLevenshteinDistance("abc", "12345")); // completely different
    assertEquals(1, StringUtils.getLevenshteinDistance("abc", "ac")); // deletion
    assertEquals(1, StringUtils.getLevenshteinDistance("abc", "ab1c")); // insertion
    assertEquals(1, StringUtils.getLevenshteinDistance("abc", "a1c")); // modification
  }

  @Test
  public void testArrayToNewlines() {
    Object[] uuids = IntStream.range(1, 10).mapToObj(unused -> UUID.randomUUID()).toArray();

    String r1 = StringUtils.arrayToNewlines(uuids, 6);
    assertTrue(r1.endsWith("...\n"));
    assertEquals(7, countMatches(r1, "\n"));
    String r2 = StringUtils.arrayToNewlines(uuids, 15);
    String r3 = StringUtils.arrayToNewlines(uuids, 10);
    assertEquals(r3, r2);
  }

  @Test
  public void testLeftTruncate() {
    assertEquals("", StringUtils.leftTruncate(null, 3));
    assertEquals("", StringUtils.leftTruncate("", 3));
    assertEquals("abc...", StringUtils.leftTruncate("abcd", 3));
  }
}
