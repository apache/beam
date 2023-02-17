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
}
