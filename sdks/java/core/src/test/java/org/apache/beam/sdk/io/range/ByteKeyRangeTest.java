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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ByteKeyRange}. */
@RunWith(JUnit4.class)
public class ByteKeyRangeTest {
  // A set of ranges for testing.
  private static final ByteKeyRange RANGE_1_10 = ByteKeyRange.of(ByteKey.of(1), ByteKey.of(10));
  private static final ByteKeyRange RANGE_5_10 = ByteKeyRange.of(ByteKey.of(5), ByteKey.of(10));
  private static final ByteKeyRange RANGE_5_50 = ByteKeyRange.of(ByteKey.of(5), ByteKey.of(50));
  private static final ByteKeyRange RANGE_10_50 = ByteKeyRange.of(ByteKey.of(10), ByteKey.of(50));
  private static final ByteKeyRange UP_TO_1 = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(1));
  private static final ByteKeyRange UP_TO_5 = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(5));
  private static final ByteKeyRange UP_TO_10 = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(10));
  private static final ByteKeyRange UP_TO_50 = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(50));
  private static final ByteKeyRange AFTER_1 = ByteKeyRange.of(ByteKey.of(1), ByteKey.EMPTY);
  private static final ByteKeyRange AFTER_5 = ByteKeyRange.of(ByteKey.of(5), ByteKey.EMPTY);
  private static final ByteKeyRange AFTER_10 = ByteKeyRange.of(ByteKey.of(10), ByteKey.EMPTY);
  private static final ByteKeyRange[] TEST_RANGES =
      new ByteKeyRange[] {
        ByteKeyRange.ALL_KEYS,
        RANGE_1_10,
        RANGE_5_10,
        RANGE_5_50,
        RANGE_10_50,
        UP_TO_1,
        UP_TO_5,
        UP_TO_10,
        UP_TO_50,
        AFTER_1,
        AFTER_5,
        AFTER_10,
      };

  static final ByteKey[] RANGE_TEST_KEYS =
      ImmutableList.<ByteKey>builder()
          .addAll(Arrays.asList(ByteKeyTest.TEST_KEYS))
          .add(ByteKey.EMPTY)
          .build()
          .toArray(ByteKeyTest.TEST_KEYS);

  /**
   * Tests that the two ranges do not overlap, passing each in as the first range in the comparison.
   */
  private static void bidirectionalNonOverlap(ByteKeyRange left, ByteKeyRange right) {
    bidirectionalOverlapHelper(left, right, false);
  }

  /** Tests that the two ranges overlap, passing each in as the first range in the comparison. */
  private static void bidirectionalOverlap(ByteKeyRange left, ByteKeyRange right) {
    bidirectionalOverlapHelper(left, right, true);
  }

  /** Helper function for tests with a good error message. */
  private static void bidirectionalOverlapHelper(
      ByteKeyRange left, ByteKeyRange right, boolean result) {
    assertEquals(String.format("%s overlaps %s", left, right), result, left.overlaps(right));
    assertEquals(String.format("%s overlaps %s", right, left), result, right.overlaps(left));
  }

  /** Tests of {@link ByteKeyRange#overlaps(ByteKeyRange)} with cases that should return true. */
  @Test
  public void testOverlappingRanges() {
    bidirectionalOverlap(ByteKeyRange.ALL_KEYS, ByteKeyRange.ALL_KEYS);
    bidirectionalOverlap(ByteKeyRange.ALL_KEYS, RANGE_1_10);
    bidirectionalOverlap(UP_TO_1, UP_TO_1);
    bidirectionalOverlap(UP_TO_1, UP_TO_5);
    bidirectionalOverlap(UP_TO_50, AFTER_10);
    bidirectionalOverlap(UP_TO_50, RANGE_1_10);
    bidirectionalOverlap(UP_TO_10, UP_TO_50);
    bidirectionalOverlap(RANGE_1_10, RANGE_5_50);
    bidirectionalOverlap(AFTER_1, AFTER_5);
    bidirectionalOverlap(RANGE_5_10, RANGE_1_10);
    bidirectionalOverlap(RANGE_5_10, RANGE_5_50);
  }

  /** Tests of {@link ByteKeyRange#overlaps(ByteKeyRange)} with cases that should return false. */
  @Test
  public void testNonOverlappingRanges() {
    bidirectionalNonOverlap(UP_TO_1, AFTER_1);
    bidirectionalNonOverlap(UP_TO_1, AFTER_5);
    bidirectionalNonOverlap(RANGE_5_10, RANGE_10_50);
  }

  /** Verifies that all keys in the given list are strictly ordered by size. */
  private static void ensureOrderedKeys(List<ByteKey> keys) {
    for (int i = 0; i < keys.size() - 1; ++i) {
      // This will throw if these two keys do not form a valid range.
      ByteKeyRange.of(keys.get(i), keys.get(i + 1));
      // Also, a key is only allowed empty if it is the first key.
      if (i > 0 && keys.get(i).isEmpty()) {
        fail(String.format("Intermediate key %s/%s may not be empty", i, keys.size()));
      }
    }
  }

  /** Tests for {@link ByteKeyRange#split(int)} with invalid inputs. */
  @Test
  public void testRejectsInvalidSplit() {
    try {
      fail(String.format("%s.split(0) should fail: %s", RANGE_1_10, RANGE_1_10.split(0)));
    } catch (IllegalArgumentException expected) {
      // pass
    }

    try {
      fail(String.format("%s.split(-3) should fail: %s", RANGE_1_10, RANGE_1_10.split(-3)));
    } catch (IllegalArgumentException expected) {
      // pass
    }
  }

  /** Tests for {@link ByteKeyRange#split(int)} with weird inputs. */
  @Test
  public void testSplitSpecialInputs() {
    // Range split by 1 returns list of its keys.
    assertEquals(
        "Split 1 should return input",
        ImmutableList.of(RANGE_1_10.getStartKey(), RANGE_1_10.getEndKey()),
        RANGE_1_10.split(1));

    // Unsplittable range returns list of its keys.
    ByteKeyRange unsplittable = ByteKeyRange.of(ByteKey.of(), ByteKey.of(0, 0, 0, 0));
    assertEquals(
        "Unsplittable should return input",
        ImmutableList.of(unsplittable.getStartKey(), unsplittable.getEndKey()),
        unsplittable.split(5));
  }

  /** Tests for {@link ByteKeyRange#split(int)}. */
  @Test
  public void testSplitKeysCombinatorial() {
    List<Integer> sizes = ImmutableList.of(1, 2, 5, 10, 25, 32, 64);
    for (int i = 0; i < RANGE_TEST_KEYS.length; ++i) {
      for (int j = i + 1; j < RANGE_TEST_KEYS.length; ++j) {
        ByteKeyRange range = ByteKeyRange.of(RANGE_TEST_KEYS[i], RANGE_TEST_KEYS[j]);
        for (int s : sizes) {
          List<ByteKey> splits = range.split(s);
          ensureOrderedKeys(splits);
          assertThat("At least two entries in splits", splits.size(), greaterThanOrEqualTo(2));
          assertEquals("First split equals start of range", splits.get(0), RANGE_TEST_KEYS[i]);
          assertEquals(
              "Last split equals end of range", splits.get(splits.size() - 1), RANGE_TEST_KEYS[j]);
        }
      }
    }
  }

  /** Manual tests for {@link ByteKeyRange#estimateFractionForKey}. */
  @Test
  public void testEstimateFractionForKey() {
    final double delta = 0.0000001;

    /* 0x80 is halfway between [] and [] */
    assertEquals(0.5, ByteKeyRange.ALL_KEYS.estimateFractionForKey(ByteKey.of(0x80)), delta);

    /* 0x80 is halfway between [00] and [] */
    ByteKeyRange after0 = ByteKeyRange.of(ByteKey.of(0), ByteKey.EMPTY);
    assertEquals(0.5, after0.estimateFractionForKey(ByteKey.of(0x80)), delta);

    /* 0x80 is halfway between [0000] and [] */
    ByteKeyRange after00 = ByteKeyRange.of(ByteKey.of(0, 0), ByteKey.EMPTY);
    assertEquals(0.5, after00.estimateFractionForKey(ByteKey.of(0x80)), delta);

    /* 0x7f is halfway between [] and [fe] */
    ByteKeyRange upToFE = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(0xfe));
    assertEquals(0.5, upToFE.estimateFractionForKey(ByteKey.of(0x7f)), delta);

    /* 0x40 is one-quarter of the way between [] and [] */
    assertEquals(0.25, ByteKeyRange.ALL_KEYS.estimateFractionForKey(ByteKey.of(0x40)), delta);

    /* 0x40 is one-half of the way between [] and [0x80] */
    ByteKeyRange upTo80 = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(0x80));
    assertEquals(0.50, upTo80.estimateFractionForKey(ByteKey.of(0x40)), delta);

    /* 0x40 is one-half of the way between [0x30] and [0x50] */
    ByteKeyRange range30to50 = ByteKeyRange.of(ByteKey.of(0x30), ByteKey.of(0x50));
    assertEquals(0.50, range30to50.estimateFractionForKey(ByteKey.of(0x40)), delta);

    /* 0x40 is one-half of the way between [0x30, 0, 1] and [0x4f, 0xff, 0xff, 0, 0] */
    ByteKeyRange range31to4f =
        ByteKeyRange.of(ByteKey.of(0x30, 0, 1), ByteKey.of(0x4f, 0xff, 0xff, 0, 0));
    assertEquals(0.50, range31to4f.estimateFractionForKey(ByteKey.of(0x40)), delta);

    /* Exact fractions from 0 to 47 for a prime range. */
    ByteKeyRange upTo47 = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(47));
    for (int i = 0; i <= 47; ++i) {
      assertEquals("i=" + i, i / 47.0, upTo47.estimateFractionForKey(ByteKey.of(i)), delta);
    }

    /* Exact fractions from 0 to 83 for a prime range. */
    ByteKeyRange rangeFDECtoFDEC83 =
        ByteKeyRange.of(ByteKey.of(0xfd, 0xec), ByteKey.of(0xfd, 0xec, 83));
    for (int i = 0; i <= 83; ++i) {
      assertEquals(
          "i=" + i,
          i / 83.0,
          rangeFDECtoFDEC83.estimateFractionForKey(ByteKey.of(0xfd, 0xec, i)),
          delta);
    }
  }

  /** Manual tests for {@link ByteKeyRange#interpolateKey}. */
  @Test
  public void testInterpolateKey() {
    /* 0x80 is halfway between [] and [] */
    assertEqualExceptPadding(ByteKey.of(0x80), ByteKeyRange.ALL_KEYS.interpolateKey(0.5));

    /* 0x80 is halfway between [00] and [] */
    ByteKeyRange after0 = ByteKeyRange.of(ByteKey.of(0), ByteKey.EMPTY);
    assertEqualExceptPadding(ByteKey.of(0x80), after0.interpolateKey(0.5));

    /* 0x80 is halfway between [0000] and [] -- padding to longest key */
    ByteKeyRange after00 = ByteKeyRange.of(ByteKey.of(0, 0), ByteKey.EMPTY);
    assertEqualExceptPadding(ByteKey.of(0x80), after00.interpolateKey(0.5));

    /* 0x7f is halfway between [] and [fe] */
    ByteKeyRange upToFE = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(0xfe));
    assertEqualExceptPadding(ByteKey.of(0x7f), upToFE.interpolateKey(0.5));

    /* 0x40 is one-quarter of the way between [] and [] */
    assertEqualExceptPadding(ByteKey.of(0x40), ByteKeyRange.ALL_KEYS.interpolateKey(0.25));

    /* 0x40 is halfway between [] and [0x80] */
    ByteKeyRange upTo80 = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(0x80));
    assertEqualExceptPadding(ByteKey.of(0x40), upTo80.interpolateKey(0.5));

    /* 0x40 is halfway between [0x30] and [0x50] */
    ByteKeyRange range30to50 = ByteKeyRange.of(ByteKey.of(0x30), ByteKey.of(0x50));
    assertEqualExceptPadding(ByteKey.of(0x40), range30to50.interpolateKey(0.5));

    /* 0x40 is halfway between [0x30, 0, 1] and [0x4f, 0xff, 0xff, 0, 0]  */
    ByteKeyRange range31to4f =
        ByteKeyRange.of(ByteKey.of(0x30, 0, 1), ByteKey.of(0x4f, 0xff, 0xff, 0, 0));
    assertEqualExceptPadding(ByteKey.of(0x40), range31to4f.interpolateKey(0.5));
  }

  /** Tests that {@link ByteKeyRange#interpolateKey} does not return the empty key. */
  @Test
  public void testInterpolateKeyIsNotEmpty() {
    String fmt = "Interpolating %s at fraction 0.0 should not return the empty key";
    for (ByteKeyRange range : TEST_RANGES) {
      range = ByteKeyRange.ALL_KEYS;
      assertFalse(String.format(fmt, range), range.interpolateKey(0.0).isEmpty());
    }
  }

  /** Test {@link ByteKeyRange} getters. */
  @Test
  public void testKeyGetters() {
    // [1,)
    assertEquals(AFTER_1.getStartKey(), ByteKey.of(1));
    assertEquals(AFTER_1.getEndKey(), ByteKey.EMPTY);
    // [1, 10)
    assertEquals(RANGE_1_10.getStartKey(), ByteKey.of(1));
    assertEquals(RANGE_1_10.getEndKey(), ByteKey.of(10));
    // [, 10)
    assertEquals(UP_TO_10.getStartKey(), ByteKey.EMPTY);
    assertEquals(UP_TO_10.getEndKey(), ByteKey.of(10));
  }

  /** Test {@link ByteKeyRange#toString}. */
  @Test
  public void testToString() {
    assertEquals("ByteKeyRange{startKey=[], endKey=[0a]}", UP_TO_10.toString());
  }

  /** Test {@link ByteKeyRange#equals}. */
  @Test
  public void testEquals() {
    // Verify that the comparison gives the correct result for all values in both directions.
    for (int i = 0; i < TEST_RANGES.length; ++i) {
      for (int j = 0; j < TEST_RANGES.length; ++j) {
        ByteKeyRange left = TEST_RANGES[i];
        ByteKeyRange right = TEST_RANGES[j];
        boolean eq = left.equals(right);
        if (i == j) {
          assertTrue(String.format("Expected that %s is equal to itself.", left), eq);
          assertTrue(
              String.format("Expected that %s is equal to a copy of itself.", left),
              left.equals(ByteKeyRange.of(right.getStartKey(), right.getEndKey())));
        } else {
          assertFalse(String.format("Expected that %s is not equal to %s", left, right), eq);
        }
      }
    }
  }

  /** Test that {@link ByteKeyRange#of} rejects invalid ranges. */
  @Test
  public void testRejectsInvalidRanges() {
    ByteKey[] testKeys = ByteKeyTest.TEST_KEYS;
    for (int i = 0; i < testKeys.length; ++i) {
      for (int j = i; j < testKeys.length; ++j) {
        if (testKeys[i].isEmpty() || testKeys[j].isEmpty() || testKeys[j].equals(testKeys[i])) {
          continue; // these are valid ranges.
        }
        try {
          ByteKeyRange range = ByteKeyRange.of(testKeys[j], testKeys[i]);
          fail(String.format("Expected failure constructing %s", range));
        } catch (IllegalArgumentException expected) {
          // pass
        }
      }
    }
  }

  /** Test {@link ByteKeyRange#hashCode}. */
  @Test
  public void testHashCode() {
    // Verify that the hashCode is equal when i==j, and usually not equal otherwise.
    int collisions = 0;
    for (int i = 0; i < TEST_RANGES.length; ++i) {
      ByteKeyRange current = TEST_RANGES[i];
      int left = current.hashCode();
      int leftClone = ByteKeyRange.of(current.getStartKey(), current.getEndKey()).hashCode();
      assertEquals(
          String.format("Expected same hash code for %s and a copy of itself", current),
          left,
          leftClone);
      for (int j = i + 1; j < TEST_RANGES.length; ++j) {
        int right = TEST_RANGES[j].hashCode();
        if (left == right) {
          ++collisions;
        }
      }
    }
    int totalUnequalTests = TEST_RANGES.length * (TEST_RANGES.length - 1) / 2;
    assertThat("Too many hash collisions", collisions, lessThan(totalUnequalTests / 2));
  }

  /**
   * Asserts the two keys are equal except trailing zeros. Note that this can only be used for
   * testing split logic. *
   */
  public static void assertEqualExceptPadding(ByteKey expected, ByteKey key) {
    ByteBuffer shortKey = expected.getValue();
    ByteBuffer longKey = key.getValue();
    if (shortKey.remaining() > longKey.remaining()) {
      shortKey = key.getValue();
      longKey = expected.getValue();
    }
    for (int i = 0; i < shortKey.remaining(); ++i) {
      if (shortKey.get(i) != longKey.get(i)) {
        fail(String.format("Expected %s (up to trailing zeros), got %s", expected, key));
      }
    }
    for (int j = shortKey.remaining(); j < longKey.remaining(); ++j) {
      if (longKey.get(j) != 0) {
        fail(String.format("Expected %s (up to trailing zeros), got %s", expected, key));
      }
    }
  }
}
