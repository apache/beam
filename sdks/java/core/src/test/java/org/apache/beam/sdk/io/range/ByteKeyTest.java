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
package org.apache.beam.sdk.io.range;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of {@link ByteKey}. */
@RunWith(JUnit4.class)
public class ByteKeyTest {
  /* A big list of byte[] keys, in ascending sorted order. */
  static final ByteKey[] TEST_KEYS =
      new ByteKey[] {
        ByteKey.EMPTY,
        ByteKey.of(0),
        ByteKey.of(0, 1),
        ByteKey.of(0, 1, 1),
        ByteKey.of(0, 1, 2),
        ByteKey.of(0, 1, 2, 0xfe),
        ByteKey.of(0, 1, 3, 0xfe),
        ByteKey.of(0, 0xfe, 0xfe, 0xfe),
        ByteKey.of(0, 0xfe, 0xfe, 0xff),
        ByteKey.of(0, 0xfe, 0xff, 0),
        ByteKey.of(0, 0xff, 0xff, 0),
        ByteKey.of(0, 0xff, 0xff, 1),
        ByteKey.of(0, 0xff, 0xff, 0xfe),
        ByteKey.of(0, 0xff, 0xff, 0xff),
        ByteKey.of(1),
        ByteKey.of(1, 2),
        ByteKey.of(1, 2, 3),
        ByteKey.of(3),
        ByteKey.of(0xdd),
        ByteKey.of(0xfe),
        ByteKey.of(0xfe, 0xfe),
        ByteKey.of(0xfe, 0xff),
        ByteKey.of(0xff),
        ByteKey.of(0xff, 0),
        ByteKey.of(0xff, 0xfe),
        ByteKey.of(0xff, 0xff),
        ByteKey.of(0xff, 0xff, 0xff),
        ByteKey.of(0xff, 0xff, 0xff, 0xff),
      };

  /**
   * Tests {@link ByteKey#compareTo(ByteKey)} using exhaustive testing within a large sorted list of
   * keys.
   */
  @Test
  public void testCompareToExhaustive() {
    // Verify that the comparison gives the correct result for all values in both directions.
    for (int i = 0; i < TEST_KEYS.length; ++i) {
      for (int j = 0; j < TEST_KEYS.length; ++j) {
        ByteKey left = TEST_KEYS[i];
        ByteKey right = TEST_KEYS[j];
        int cmp = left.compareTo(right);
        if (i < j && !(cmp < 0)) {
          fail(
              String.format(
                  "Expected that cmp(%s, %s) < 0, got %d [i=%d, j=%d]", left, right, cmp, i, j));
        } else if (i == j && !(cmp == 0)) {
          fail(
              String.format(
                  "Expected that cmp(%s, %s) == 0, got %d [i=%d, j=%d]", left, right, cmp, i, j));
        } else if (i > j && !(cmp > 0)) {
          fail(
              String.format(
                  "Expected that cmp(%s, %s) > 0, got %d [i=%d, j=%d]", left, right, cmp, i, j));
        }
      }
    }
  }

  /** Tests {@link ByteKey#equals}. */
  @Test
  public void testEquals() {
    // Verify that the comparison gives the correct result for all values in both directions.
    for (int i = 0; i < TEST_KEYS.length; ++i) {
      for (int j = 0; j < TEST_KEYS.length; ++j) {
        ByteKey left = TEST_KEYS[i];
        ByteKey right = TEST_KEYS[j];
        boolean eq = left.equals(right);
        if (i == j) {
          assertTrue(String.format("Expected that %s is equal to itself.", left), eq);
          assertTrue(
              String.format("Expected that %s is equal to a copy of itself.", left),
              left.equals(ByteKey.copyFrom(right.getValue())));
        } else {
          assertFalse(String.format("Expected that %s is not equal to %s", left, right), eq);
        }
      }
    }
  }

  /** Tests {@link ByteKey#hashCode}. */
  @Test
  public void testHashCode() {
    // Verify that the hashCode is equal when i==j, and usually not equal otherwise.
    int collisions = 0;
    for (int i = 0; i < TEST_KEYS.length; ++i) {
      int left = TEST_KEYS[i].hashCode();
      int leftClone = ByteKey.copyFrom(TEST_KEYS[i].getValue()).hashCode();
      assertEquals(
          String.format("Expected same hash code for %s and a copy of itself", TEST_KEYS[i]),
          left,
          leftClone);
      for (int j = i + 1; j < TEST_KEYS.length; ++j) {
        int right = TEST_KEYS[j].hashCode();
        if (left == right) {
          ++collisions;
        }
      }
    }
    int totalUnequalTests = TEST_KEYS.length * (TEST_KEYS.length - 1) / 2;
    assertThat("Too many hash collisions", collisions, lessThan(totalUnequalTests / 2));
  }

  /** Tests {@link ByteKey#toString}. */
  @Test
  public void testToString() {
    assertEquals("[]", ByteKey.EMPTY.toString());
    assertEquals("[00]", ByteKey.of(0).toString());
    assertEquals("[0000]", ByteKey.of(0x00, 0x00).toString());
    assertEquals(
        "[0123456789abcdef]",
        ByteKey.of(0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef).toString());
  }

  /** Tests {@link ByteKey#isEmpty}. */
  @Test
  public void testIsEmpty() {
    assertTrue("[] is empty", ByteKey.EMPTY.isEmpty());
    assertFalse("[00]", ByteKey.of(0).isEmpty());
  }

  /** Tests {@link ByteKey#getBytes}. */
  @Test
  public void testGetBytes() {
    assertArrayEquals("[] equal after getBytes", new byte[] {}, ByteKey.EMPTY.getBytes());
    assertArrayEquals("[00] equal after getBytes", new byte[] {0x00}, ByteKey.of(0x00).getBytes());
  }
}
