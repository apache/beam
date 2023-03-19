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

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.formatByteStringRange;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.partitionsToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ByteStringRangeHelperTest {

  @Test
  public void testParentIsEntireKeySpaceIsSuperSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition = ByteStringRange.create("", "");
    parentPartitions.add(partition);

    ByteStringRange childPartition = ByteStringRange.create("A", "B");

    assertTrue(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testChildIsEntireKeySpaceParentIsLeftSubSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition = ByteStringRange.create("", "n");
    parentPartitions.add(partition);

    ByteStringRange childPartition = ByteStringRange.create("", "");
    assertFalse(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testChildIsEntireKeySpaceParentIsRightSubSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition = ByteStringRange.create("n", "");
    parentPartitions.add(partition);

    ByteStringRange childPartition = ByteStringRange.create("", "");
    assertFalse(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testChildIsEntireKeySpaceParentIsSuperSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition1 = ByteStringRange.create("", "n");
    ByteStringRange partition2 = ByteStringRange.create("n", "");
    parentPartitions.add(partition1);
    parentPartitions.add(partition2);

    ByteStringRange childPartition = ByteStringRange.create("", "");
    assertTrue(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testParentKeySpaceStartsBeforeAndEndAfterChildIsSuperSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition = ByteStringRange.create("A", "B");
    parentPartitions.add(partition);

    ByteStringRange childPartition = ByteStringRange.create("AA", "AB");

    assertTrue(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testParentStartKeyIsAfterChildStartKeyIsNotSuperSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition = ByteStringRange.create("AA", "B");
    parentPartitions.add(partition);

    ByteStringRange childPartition = ByteStringRange.create("A", "AB");

    assertFalse(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testParentEndKeyIsBeforeChildEndKeyIsNotSuperSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition = ByteStringRange.create("A", "B");
    parentPartitions.add(partition);

    ByteStringRange childPartition = ByteStringRange.create("AA", "BA");

    assertFalse(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testParentIsSameAsChildIsSuperSet() {
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    ByteStringRange partition = ByteStringRange.create("A", "B");
    parentPartitions.add(partition);

    ByteStringRange childPartition = ByteStringRange.create("A", "B");

    assertTrue(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testParentIsMissingPartitionIsNotSuperSet() {
    ByteStringRange partition1 = ByteStringRange.create("A", "B");
    ByteStringRange partition2 = ByteStringRange.create("C", "Z");
    List<ByteStringRange> parentPartitions = Arrays.asList(partition1, partition2);

    ByteStringRange childPartition = ByteStringRange.create("A", "Z");

    assertFalse(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testParentHasOverlapIsSuperSet() {
    ByteStringRange partition1 = ByteStringRange.create("A", "C");
    ByteStringRange partition2 = ByteStringRange.create("B", "Z");
    List<ByteStringRange> parentPartitions = Arrays.asList(partition1, partition2);

    ByteStringRange childPartition = ByteStringRange.create("A", "Z");

    assertTrue(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testEmptyParentsIsNotSuperset() {
    List<ByteStringRange> parentPartitions = Collections.emptyList();
    ByteStringRange childPartition = ByteStringRange.create("", "");

    assertFalse(ByteStringRangeHelper.isSuperset(parentPartitions, childPartition));
  }

  @Test
  public void testPartitionsToString() {
    ByteStringRange partition1 = ByteStringRange.create("", "A");
    ByteStringRange partition2 = ByteStringRange.create("A", "B");
    ByteStringRange partition3 = ByteStringRange.create("B", "");
    List<ByteStringRange> partitions = Arrays.asList(partition1, partition2, partition3);
    String partitionsString = partitionsToString(partitions);
    assertEquals(
        String.format(
            "{%s, %s, %s}",
            formatByteStringRange(partition1),
            formatByteStringRange(partition2),
            formatByteStringRange(partition3)),
        partitionsString);
  }

  @Test
  public void testPartitionsToStringEmptyPartition() {
    List<ByteStringRange> partitions = new ArrayList<>();
    String partitionsString = partitionsToString(partitions);
    assertEquals("{}", partitionsString);
  }

  @Test
  public void testPartitionComparator() {
    ByteStringRange partition1 = ByteStringRange.create("", "a");
    ByteStringRange partition2 = ByteStringRange.create("", "");
    ByteStringRange partition3 = ByteStringRange.create("a", "z");
    ByteStringRange partition4 = ByteStringRange.create("a", "");
    List<ByteStringRange> unsorted = Arrays.asList(partition3, partition4, partition2, partition1);
    List<ByteStringRange> sorted = Arrays.asList(partition1, partition2, partition3, partition4);
    unsorted.sort(new ByteStringRangeHelper.PartitionComparator());
    assertEquals(unsorted, sorted);
  }
}
