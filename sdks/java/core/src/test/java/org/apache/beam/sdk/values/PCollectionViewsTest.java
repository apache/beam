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
package org.apache.beam.sdk.values;

import static org.apache.beam.sdk.values.PCollectionViews.computeOverlappingRanges;
import static org.apache.beam.sdk.values.PCollectionViews.computePositionForIndex;
import static org.apache.beam.sdk.values.PCollectionViews.computeTotalNumElements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PCollectionViews}. */
@RunWith(JUnit4.class)
public class PCollectionViewsTest {
  @Test
  public void testEmpty() {
    Iterable<OffsetRange> ranges = Collections.emptyList();

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(nonOverlappingRangesToNumElementsPerPosition, Collections.emptyMap());
    assertEquals(0, computeTotalNumElements(nonOverlappingRangesToNumElementsPerPosition));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> computePositionForIndex(nonOverlappingRangesToNumElementsPerPosition, 0));
  }

  @Test
  public void testNoOverlapping() {
    Iterable<OffsetRange> ranges = Arrays.asList(range(0, 2), range(4, 6));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.of(range(0, 2), 1, range(4, 6), 1),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testOnlyTouchingRanges() {
    Iterable<OffsetRange> ranges = Arrays.asList(range(0, 4), range(4, 8), range(8, 12));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.of(range(0, 4), 1, range(4, 8), 1, range(8, 12), 1),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testRangesWithAtMostOneOverlap() {
    Iterable<OffsetRange> ranges = Arrays.asList(range(0, 6), range(4, 10), range(8, 12));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.builder()
            .put(range(0, 4), 1)
            .put(range(4, 6), 2)
            .put(range(6, 8), 1)
            .put(range(8, 10), 2)
            .put(range(10, 12), 1)
            .build(),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testOverlappingFroms() {
    Iterable<OffsetRange> ranges = Arrays.asList(range(0, 4), range(0, 8), range(0, 12));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.builder().put(range(0, 4), 3).put(range(4, 8), 2).put(range(8, 12), 1).build(),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testOverlappingTos() {
    Iterable<OffsetRange> ranges = Arrays.asList(range(0, 12), range(4, 12), range(8, 12));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.builder().put(range(0, 4), 1).put(range(4, 8), 2).put(range(8, 12), 3).build(),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testOverlappingFromsAndTos() {
    Iterable<OffsetRange> ranges = Arrays.asList(range(0, 4), range(0, 4), range(0, 4));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.builder().put(range(0, 4), 3).build(),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testMultipleOverlapsForTheSameRange() {
    Iterable<OffsetRange> ranges =
        Arrays.asList(
            range(0, 4),
            range(0, 8),
            range(0, 12),
            range(0, 12),
            range(4, 12),
            range(8, 12),
            range(0, 4),
            range(0, 8),
            range(0, 12),
            range(0, 12),
            range(4, 12),
            range(8, 12));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.builder().put(range(0, 4), 8).put(range(4, 8), 8).put(range(8, 12), 8).build(),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testIncreasingOverlaps() {
    Iterable<OffsetRange> ranges =
        Arrays.asList(range(0, 4), range(1, 5), range(2, 6), range(3, 7), range(4, 8), range(5, 9));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.builder()
            .put(range(0, 1), 1)
            .put(range(1, 2), 2)
            .put(range(2, 3), 3)
            .put(range(3, 4), 4)
            .put(range(4, 5), 4)
            .put(range(5, 6), 4)
            .put(range(6, 7), 3)
            .put(range(7, 8), 2)
            .put(range(8, 9), 1)
            .build(),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testNestedOverlaps() {
    Iterable<OffsetRange> ranges =
        Arrays.asList(range(0, 8), range(1, 7), range(2, 6), range(3, 5));

    Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
        computeOverlappingRanges(ranges);
    assertEquals(
        ImmutableMap.builder()
            .put(range(0, 1), 1)
            .put(range(1, 2), 2)
            .put(range(2, 3), 3)
            .put(range(3, 5), 4)
            .put(range(5, 6), 3)
            .put(range(6, 7), 2)
            .put(range(7, 8), 1)
            .build(),
        nonOverlappingRangesToNumElementsPerPosition);
    assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
  }

  @Test
  public void testRandomRanges() {
    Random random =
        new Random(123892154890L); // use an arbitrary seed to make this test deterministic
    for (int i = 0; i < 1000; ++i) {
      List<OffsetRange> ranges = new ArrayList<>();
      for (int j = 0; j < 20; ++j) {
        long start = random.nextInt(10);
        ranges.add(range(start, start + random.nextInt(10) + 1));
        Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition =
            computeOverlappingRanges(ranges);
        assertNonEmptyRangesAndPositions(ranges, nonOverlappingRangesToNumElementsPerPosition);
      }
    }
  }

  private static OffsetRange range(long from, long to) {
    return new OffsetRange(from, to);
  }

  private static void assertNonEmptyRangesAndPositions(
      Iterable<OffsetRange> ranges,
      Map<OffsetRange, Integer> nonOverlappingRangesToNumElementsPerPosition) {
    for (Map.Entry<OffsetRange, Integer> entry :
        nonOverlappingRangesToNumElementsPerPosition.entrySet()) {
      assertNotEquals(0, (int) entry.getValue());
    }

    ListMultimap<Long, Integer> positions = ArrayListMultimap.create();
    for (OffsetRange range : ranges) {
      for (long i = range.getFrom(); i < range.getTo(); ++i) {
        positions.put(i, 0);
      }
    }

    int position = 0;
    for (Long key : new TreeSet<>(positions.keySet())) {
      int size = positions.get(key).size();
      positions.replaceValues(
          key, Lists.newArrayList(IntStream.range(position, position + size).iterator()));
      position += size;
    }

    for (int i = 0; i < position; ++i) {
      KV<Long, Integer> computedPosition =
          computePositionForIndex(nonOverlappingRangesToNumElementsPerPosition, i);
      assertEquals(
          i, (int) positions.get(computedPosition.getKey()).get(computedPosition.getValue()));
    }

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> computePositionForIndex(nonOverlappingRangesToNumElementsPerPosition, -1));

    int totalNumberOfElements =
        computeTotalNumElements(nonOverlappingRangesToNumElementsPerPosition);
    assertEquals(position, totalNumberOfElements);
    assertThrows(
        IndexOutOfBoundsException.class,
        () ->
            computePositionForIndex(
                nonOverlappingRangesToNumElementsPerPosition, totalNumberOfElements));
  }
}
