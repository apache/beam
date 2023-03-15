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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

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
   * Returns true if parentPartitions is a superset of childPartition.
   *
   * <p>If ordered parentPartitions row ranges form a contiguous range, and start key is before or
   * at childPartition's start key, and end key is at or after childPartition's end key, then
   * parentPartitions is a superset of childPartition.
   *
   * <p>Overlaps from parents are valid because arbitrary partitions can merge and they may overlap.
   * They will form a valid new partition. However, if there are any missing parent partitions, then
   * merge cannot happen with missing row ranges.
   *
   * @param parentPartitions list of partitions to determine if it forms a large contiguous range
   * @param childPartition the smaller partition
   * @return true if parentPartitions is a superset of childPartition, otherwise false.
   */
  public static boolean isSuperset(
      List<ByteStringRange> parentPartitions, ByteStringRange childPartition) {
    // sort parentPartitions by starting key
    // iterate through, check open end key and close start key of each iteration to ensure no gaps.
    // first start key and last end key must be equal to or wider than child partition start and end
    // key.
    if (parentPartitions.isEmpty()) {
      return false;
    }
    parentPartitions.sort(new PartitionComparator());
    ByteString parentStartKey = parentPartitions.get(0).getStart();
    ByteString parentEndKey = parentPartitions.get(parentPartitions.size() - 1).getEnd();

    return !childStartsBeforeParent(parentStartKey, childPartition.getStart())
        && !childEndsAfterParent(parentEndKey, childPartition.getEnd())
        && !gapsInParentPartitions(parentPartitions);
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
   * Figure out if partitions cover the entire keyspace. If it doesn't, return a list of missing and
   * overlapping partitions.
   *
   * <p>partitions covers the entire key space if, when ordered, the end key is the same as the
   * start key of the next row range in the list, and the first start key is "" and the last end key
   * is "". There should be no overlap.
   *
   * @param partitions to determine if they cover entire keyspace
   * @return list of missing and overlapping partitions
   */
  public static List<ByteStringRange> getMissingAndOverlappingPartitionsFromKeySpace(
      List<ByteStringRange> partitions) {
    if (partitions.isEmpty()) {
      return Collections.singletonList(ByteStringRange.create("", ""));
    }

    List<ByteStringRange> missingPartitions = new ArrayList<>();

    // sort partitions by start key
    // iterate through ensuring end key is lexicographically after next start key.
    partitions.sort(new PartitionComparator());

    ByteString prevEnd = ByteString.EMPTY;
    for (ByteStringRange partition : partitions) {
      if (!partition.getStart().equals(prevEnd)) {
        ByteStringRange missingPartition = ByteStringRange.create(prevEnd, partition.getStart());
        missingPartitions.add(missingPartition);
      }
      prevEnd = partition.getEnd();
    }
    // Check that the last partition ends with "", otherwise it's missing.
    if (!prevEnd.equals(ByteString.EMPTY)) {
      ByteStringRange missingPartition = ByteStringRange.create(prevEnd, ByteString.EMPTY);
      missingPartitions.add(missingPartition);
    }
    return missingPartitions;
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

  private static boolean childStartsBeforeParent(
      ByteString parentStartKey, ByteString childStartKey) {
    // Check if the start key of the child partition comes before the start key of the entire
    // parentPartitions
    return compareStartKey(parentStartKey, childStartKey) > 0;
  }

  private static boolean childEndsAfterParent(ByteString parentEndKey, ByteString childEndKey) {
    return compareEndKey(parentEndKey, childEndKey) < 0;
  }

  // This assumes parentPartitions is sorted. If parentPartitions has not already been sorted
  // it will be incorrect
  private static boolean gapsInParentPartitions(List<ByteStringRange> sortedParentPartitions) {
    for (int i = 1; i < sortedParentPartitions.size(); i++) {
      // Iterating through a sorted list, the start key should be the same or before the end of the
      // previous. Handle "" end key as a special case.
      ByteString prevEndKey = sortedParentPartitions.get(i - 1).getEnd();
      if (ByteString.unsignedLexicographicalComparator()
                  .compare(sortedParentPartitions.get(i).getStart(), prevEndKey)
              > 0
          && !prevEndKey.isEmpty()) {
        return true;
      }
    }
    return false;
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
