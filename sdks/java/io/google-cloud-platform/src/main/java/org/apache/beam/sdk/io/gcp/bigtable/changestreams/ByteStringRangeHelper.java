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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** Helper functions to evaluate the completeness of collection of ByteStringRanges. */
@Internal
public class ByteStringRangeHelper {
  private static int compareStartKey(ByteString s1, ByteString s2) {
    return ByteString.unsignedLexicographicalComparator().compare(s1, s2);
  }

  private static int compareEndKey(ByteString e1, ByteString e2) {
    if (e1.isEmpty() && !e2.isEmpty()) {
      return 1;
    }
    if (e2.isEmpty() && !e1.isEmpty()) {
      return -1;
    }
    return ByteString.unsignedLexicographicalComparator().compare(e1, e2);
  }

  private static int compareStartEndKey(ByteString start, ByteString end) {
    if (end.isEmpty()) {
      return -1;
    }
    return ByteString.unsignedLexicographicalComparator().compare(start, end);
  }

  @VisibleForTesting
  static class PartitionComparator implements Comparator<ByteStringRange> {
    @Override
    // if first > second, it returns positive number
    // if first < second, it returns negative number
    // if first == second, it returns 0
    // First is greater than second if either of the following are true:
    // - Its start key comes after second's start key
    // - The start keys are equal and its end key comes after second's end key
    // An end key of "" represents the final end key, so it needs to be handled as a special case
    public int compare(ByteStringRange first, ByteStringRange second) {
      int compareStart = compareStartKey(first.getStart(), second.getStart());
      if (compareStart != 0) {
        return compareStart;
      }
      return compareEndKey(first.getEnd(), second.getEnd());
    }
  }

  /**
   * Returns true if parentPartitions form a proper superset of childPartition.
   *
   * <p>If ordered parentPartitions row ranges form a contiguous range without any overlaps, and
   * start key equals to childPartition's start key, and end key equals to childPartition's end key,
   * then parentPartitions form a proper superset of childPartition.
   *
   * <p>Returns false if there are overlaps or missing partitions in parentPartitions.
   *
   * @param parentPartitions list of partitions to determine if it forms an exact contiguous range
   * @param childPartition the partition to match
   * @return true if parentPartitions covers the same key space as childPartition, otherwise false.
   */
  public static boolean coverSameKeySpace(
      List<ByteStringRange> parentPartitions, ByteStringRange childPartition) {
    // Sort parentPartitions by starting key. Verify there's no gap or overlaps in between parent
    // partitions.
    // The first start key and last end key must be equal to child partition.
    if (parentPartitions.isEmpty()) {
      return false;
    }
    parentPartitions.sort(new PartitionComparator());
    if (!parentPartitions.get(0).getStart().equals(childPartition.getStart())) {
      return false;
    }
    if (!parentPartitions
        .get(parentPartitions.size() - 1)
        .getEnd()
        .equals(childPartition.getEnd())) {
      return false;
    }
    return isContinuous(parentPartitions);
  }

  /**
   * Convert partitions to a string for debugging.
   *
   * @param partitions to print
   * @return string representation of partitions
   */
  public static String partitionsToString(List<ByteStringRange> partitions) {
    return partitions.stream()
        .map(ByteStringRangeHelper::formatByteStringRange)
        .collect(Collectors.joining(", ", "{", "}"));
  }

  /**
   * Return missing partitions within partitions that are within start and end. Gaps in partitions
   * that are outside of start and end are ignored.
   *
   * @param partitions find missing partitions within
   * @param start start of the search range
   * @param end end of the search range
   * @return missing partitions within the range
   */
  public static List<ByteStringRange> getMissingPartitionsFrom(
      List<ByteStringRange> partitions, ByteString start, ByteString end) {
    if (partitions.isEmpty()) {
      return Collections.singletonList(ByteStringRange.create(start, end));
    }

    List<ByteStringRange> missingPartitions = new ArrayList<>();

    // sort partitions by start key
    // iterate through ensuring end key is lexicographically after next start key.
    partitions.sort(new PartitionComparator());

    // Check if the start is missing.
    if (compareStartKey(start, partitions.get(0).getStart()) < 0) {
      missingPartitions.add(ByteStringRange.create(start, partitions.get(0).getStart()));
    }
    ByteString currEnd = partitions.get(0).getEnd();
    if (compareStartKey(start, partitions.get(0).getEnd()) > 0) {
      currEnd = start;
    }
    for (int i = 1; i < partitions.size(); i++) {
      if (compareStartEndKey(partitions.get(i).getStart(), currEnd) > 0) {
        missingPartitions.add(ByteStringRange.create(currEnd, partitions.get(i).getStart()));
      }
      // It's possible that we have subset partitions that has a later start key but earlier end
      // key. So we only advance end key if it's further ahead. For example [A, D) and [B, C).
      // PrevEnd = D and processing [B, C) would push back end key to C instead of D.
      if (compareEndKey(currEnd, partitions.get(i).getEnd()) < 0) {
        currEnd = partitions.get(i).getEnd();
      }
      // Current end maybe before the start of the evaluation range.
      if (compareStartKey(start, currEnd) > 0) {
        currEnd = start;
      }
      // If it's past the end, we stop
      if (compareEndKey(currEnd, end) > 0) {
        break;
      }
    }
    // Check that the last partition ends with end, otherwise it's missing.
    if (compareEndKey(currEnd, end) < 0) {
      missingPartitions.add(ByteStringRange.create(currEnd, end));
    }
    return missingPartitions;
  }

  /**
   * Return missing partitions from the entire keyspace.
   *
   * @param partitions find missing partitions within
   * @return a list of missing partitions.
   */
  public static List<ByteStringRange> getMissingPartitionsFromEntireKeySpace(
      List<ByteStringRange> partitions) {
    return getMissingPartitionsFrom(partitions, ByteString.EMPTY, ByteString.EMPTY);
  }

  /**
   * Return a list of overlapping partitions. The partitions outputted are valid partitions i.e.
   * start < end. This represents 2 ordered partitions overlapping because the 2nd partition starts
   * before the 1st ended. If end == start, it's not considered overlap.
   *
   * @param partitions find overlaps within
   * @return a list of overlapping partitions.
   */
  public static List<ByteStringRange> getOverlappingPartitions(List<ByteStringRange> partitions) {
    if (partitions.isEmpty() || partitions.size() == 1) {
      return Collections.emptyList();
    }

    List<ByteStringRange> overlappingPartitions = new ArrayList<>();

    // sort partitions by start key
    // iterate through ensuring end key is lexicographically after next start key.
    partitions.sort(new PartitionComparator());

    // Determine where we start
    ByteString prevEnd = partitions.get(0).getEnd();
    for (int i = 1; i < partitions.size(); i++) {
      if (compareStartEndKey(partitions.get(i).getStart(), prevEnd) < 0) {
        overlappingPartitions.add(ByteStringRange.create(partitions.get(i).getStart(), prevEnd));
      }
      prevEnd = partitions.get(i).getEnd();
    }
    return overlappingPartitions;
  }

  /**
   * Returns formatted string of a partition for debugging.
   *
   * @param partition partition to format.
   * @return String representation of partition.
   */
  public static String formatByteStringRange(ByteStringRange partition) {
    return "['"
        + TextFormat.escapeBytes(partition.getStart())
        + "','"
        + TextFormat.escapeBytes(partition.getEnd())
        + "')";
  }

  // This assumes parentPartitions is sorted. If parentPartitions has not already been sorted
  // it will be incorrect
  private static boolean isContinuous(List<ByteStringRange> sortedParentPartitions) {
    for (int i = 1; i < sortedParentPartitions.size(); i++) {
      // Iterating through a sorted list, the start key should be the same as the end of the
      // previous.
      ByteString prevEndKey = sortedParentPartitions.get(i - 1).getEnd();
      if (!sortedParentPartitions.get(i).getStart().equals(prevEndKey)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if the two ByteStringRange overlaps, otherwise false. End = Start is not
   * considered overlapping because end is open and start is closed.
   *
   * <p>Assume the two ByteStringRange are valid such that the start <= end. There are 2 scenarios
   * that's considered NOT overlapping. Otherwise, they are overlapping.
   *
   * <ul>
   *   <li>The second's start key is same or after first's end key
   *   <li>The first's start key is same or after second's end key
   * </ul>
   *
   * @param first first ByteStringRange
   * @param second second ByteStringRange
   * @return true if the two ByteStringRange overlaps, otherwise false.
   */
  public static boolean doPartitionsOverlap(ByteStringRange first, ByteStringRange second) {
    if (ByteString.unsignedLexicographicalComparator().compare(second.getStart(), first.getEnd())
            >= 0
        && !first.getEnd().isEmpty()) {
      return false;
    } else if (ByteString.unsignedLexicographicalComparator()
                .compare(first.getStart(), second.getEnd())
            >= 0
        && !second.getEnd().isEmpty()) {
      return false;
    }
    return true;
  }

  /**
   * Checks if the partition's start key is before its end key.
   *
   * @param partition the partition to verify.
   * @return true if partition is valid, otherwise false.
   */
  public static boolean isValidPartition(ByteStringRange partition) {
    return ByteString.unsignedLexicographicalComparator()
                .compare(partition.getStart(), partition.getEnd())
            < 0
        || partition.getEnd().isEmpty();
  }

  /**
   * Return the overlapping parts of 2 partitions. Throw IllegalArgumentException if the 2
   * partitions don't overlap at all.
   *
   * @param p1 first partition
   * @param p2 second partition
   * @return the intersection of the 2 partitions
   * @throws IllegalArgumentException if the 2 partitions don't overlap at all
   */
  public static ByteStringRange getIntersectingPartition(ByteStringRange p1, ByteStringRange p2)
      throws IllegalArgumentException {
    if (!doPartitionsOverlap(p1, p2)) {
      throw new IllegalArgumentException(
          String.format(
              "The partitions %s and %s have no overlap",
              formatByteStringRange(p1), formatByteStringRange(p2)));
    }
    ByteString start = p1.getStart();
    ByteString end = p1.getEnd();
    if (compareStartKey(start, p2.getStart()) < 0) {
      start = p2.getStart();
    }
    if (compareEndKey(end, p2.getEnd()) > 0) {
      end = p2.getEnd();
    }
    return ByteStringRange.create(start, end);
  }
}
