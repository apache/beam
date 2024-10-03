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
package org.apache.beam.sdk.io;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TextSourceTest {

  @Test
  public void testSubstringByteArrayOutputStreamSuccessful() throws IOException {
    TextSource.SubstringByteArrayOutputStream output =
        new TextSource.SubstringByteArrayOutputStream();
    assertEquals("", output.toString(0, 0, StandardCharsets.UTF_8));

    output.write("ABC".getBytes(StandardCharsets.UTF_8));
    assertEquals("ABC", output.toString(0, 3, StandardCharsets.UTF_8));
    assertEquals("AB", output.toString(0, 2, StandardCharsets.UTF_8));
    assertEquals("BC", output.toString(1, 2, StandardCharsets.UTF_8));
    assertEquals("", output.toString(3, 0, StandardCharsets.UTF_8));

    output.write("DE".getBytes(StandardCharsets.UTF_8));
    assertEquals("ABCDE", output.toString(0, 5, StandardCharsets.UTF_8));
  }

  @Test
  public void testSubstringByteArrayIllegalArgumentException() throws IOException {
    TextSource.SubstringByteArrayOutputStream output =
        new TextSource.SubstringByteArrayOutputStream();
    output.write("ABC".getBytes(StandardCharsets.UTF_8));

    IllegalArgumentException exception;
    exception =
        assertThrows(
            IllegalArgumentException.class, () -> output.toString(-1, 2, StandardCharsets.UTF_8));
    assertThat(exception.getMessage(), containsString("offset is negative"));

    exception =
        assertThrows(
            IllegalArgumentException.class, () -> output.toString(4, 0, StandardCharsets.UTF_8));
    assertThat(exception.getMessage(), containsString("offset exceeds the buffer limit"));

    exception =
        assertThrows(
            IllegalArgumentException.class, () -> output.toString(0, -1, StandardCharsets.UTF_8));
    assertThat(exception.getMessage(), containsString("length is negative"));

    exception =
        assertThrows(
            IllegalArgumentException.class, () -> output.toString(2, 2, StandardCharsets.UTF_8));
    assertThat(exception.getMessage(), containsString("offset + length exceeds the buffer limit"));
  }

  @Test
  public void testDelimiterFinder() {
    // Simple pattern
    assertEquals(Arrays.asList("A", "C"), split("AB", "AABC"));
    assertEquals(Arrays.asList("A", "B", "C"), split("AB", "AABBABC"));

    // When mismatched at 2 (zero-indexed), the substring "AA" has subsequence "A"
    assertEquals(Arrays.asList("A", "C"), split("AAB", "AAABC"));
    assertEquals(Arrays.asList("ABAB"), split("AAB", "ABAB"));
    assertEquals(Arrays.asList("A", "A", "B"), split("AAB", "AAABAAABB"));

    // The last byte is the same as the first byte.
    assertEquals(Arrays.asList("A", "B"), split("AABA", "AAABAB"));
    assertEquals(Arrays.asList("AABBA"), split("AABA", "AABBA"));

    // "ABAB" has subsequence "AB".
    assertEquals(Arrays.asList("", "D"), split("ABABC", "ABABCD"));
    assertEquals(Arrays.asList("ABABAD"), split("ABABC", "ABABAD"));
    assertEquals(Arrays.asList("ABABBD"), split("ABABC", "ABABBD"));
    assertEquals(Arrays.asList("AB"), split("ABABC", "ABABABC"));

    // "ABCAB" has subsequence "AB".
    assertEquals(Arrays.asList(""), split("ABCABD", "ABCABD"));
    assertEquals(Arrays.asList("ABC"), split("ABCABD", "ABCABCABD"));

    // Repetition of 3 bytes pattern.
    assertEquals(Arrays.asList("AABAAB"), split("AABAAC", "AABAAB"));
    assertEquals(Arrays.asList(""), split("AABAAC", "AABAAC"));
    assertEquals(Arrays.asList("A", "D"), split("AABAAC", "AAABAACD"));
    assertEquals(Arrays.asList("AAB", "D"), split("AABAAC", "AABAABAACD"));
    assertEquals(Arrays.asList("AABA", "D"), split("AABAAC", "AABAAABAACD"));
    assertEquals(Arrays.asList("AABAA", "D"), split("AABAAC", "AABAAAABAACD"));

    // Same characters repeated 3 times.
    assertEquals(Arrays.asList("AAA"), split("AAAA", "AAA"));
    assertEquals(Arrays.asList(""), split("AAAA", "AAAA"));
    assertEquals(Arrays.asList("AAAB"), split("AAAA", "AAAB"));
    assertEquals(Arrays.asList("", "B"), split("AAAA", "AAAAB"));

    // 3 times repeated pattern followed by a different byte.
    assertEquals(Arrays.asList(""), split("AAAB", "AAAB"));
    assertEquals(Arrays.asList("A", "B"), split("AAAB", "AAAABB"));
    assertEquals(Arrays.asList("AA", "B"), split("AAAB", "AAAAABB"));
    assertEquals(Arrays.asList("AAA", "B"), split("AAAB", "AAAAAABB"));
    assertEquals(Arrays.asList("AAAA", "B"), split("AAAB", "AAAAAAABB"));

    // Multiple empty strings return.
    assertEquals(Arrays.asList("", "", ""), split("AA", "AAAAAA"));
    assertEquals(Arrays.asList("", "", ""), split("AB", "ABABAB"));
    assertEquals(Arrays.asList("", "", ""), split("AAB", "AABAABAAB"));
  }

  List<String> split(String delimiter, String text) {
    byte[] delimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    TextSource.KMPDelimiterFinder finder = new TextSource.KMPDelimiterFinder(delimiterBytes);

    byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);

    List<String> result = new ArrayList<>();
    int start = 0;
    for (int i = 0; i < textBytes.length; i++) {
      if (finder.feed(textBytes[i])) {
        int nextStart = i + 1;
        int end = nextStart - delimiterBytes.length;
        String s = new String(textBytes, start, end - start, StandardCharsets.UTF_8);
        result.add(s);
        start = nextStart;
      }
    }

    if (start != textBytes.length) {
      result.add(new String(textBytes, start, textBytes.length - start, StandardCharsets.UTF_8));
    }
    return result;
  }
}
