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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** Helper functions to evaluate the completeness of collection of ByteStringRanges. */
@Internal
public class ByteStringRangeHelper {
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
      int compareStart =
          ByteString.unsignedLexicographicalComparator()
              .compare(first.getStart(), second.getStart());
      if (compareStart != 0) {
        return compareStart;
      }
      if (first.getEnd().isEmpty() && !second.getEnd().isEmpty()) {
        return 1;
      }
      if (second.getEnd().isEmpty() && !first.getEnd().isEmpty()) {
        return -1;
      }
      return ByteString.unsignedLexicographicalComparator()
          .compare(first.getEnd(), second.getEnd());
    }
  }

  private static boolean childStartsBeforeParent(
      ByteString parentStartKey, ByteString childStartKey) {
    // Check if the start key of the child partition comes before the start key of the entire
    // parentPartitions
    return ByteString.unsignedLexicographicalComparator().compare(parentStartKey, childStartKey)
        > 0;
  }

  private static boolean childEndsAfterParent(ByteString parentEndKey, ByteString childEndKey) {
    // A final end key is represented by "" but this evaluates to < all characters, so we need to
    // handle it as a special case.
    if (childEndKey.isEmpty() && !parentEndKey.isEmpty()) {
      return true;
    }

    // Check if the end key of the child partition comes after the end key of the entire
    // parentPartitions. "" Represents the final end key so we need to handle that as a
    // special case when it is the end key of the entire parentPartitions
    return ByteString.unsignedLexicographicalComparator().compare(parentEndKey, childEndKey) < 0
        && !parentEndKey.isEmpty();
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
}
