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

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.doPartitionsOverlap;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.formatByteStringRange;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.getIntersectingPartition;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.getMissingAndOverlappingPartitionsFromKeySpace;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.isValidPartition;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.partitionsToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
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
  public void testGetMissingAndOverlappingPartitionFromKeySpaceEmptyPartition() {
    List<ByteStringRange> partitions = new ArrayList<>();
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(
        Collections.singletonList(ByteStringRange.create("", "")), missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionFromKeySpaceSinglePartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "");
    List<ByteStringRange> partitions = Collections.singletonList(partition1);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(Collections.emptyList(), missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionFromKeySpaceNoMissingPartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "A");
    ByteStringRange partition2 = ByteStringRange.create("A", "B");
    ByteStringRange partition3 = ByteStringRange.create("B", "");
    List<ByteStringRange> partitions = Arrays.asList(partition1, partition2, partition3);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(Collections.emptyList(), missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionFromKeySpaceMissingStartPartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "A");
    ByteStringRange partition2 = ByteStringRange.create("A", "B");
    ByteStringRange partition3 = ByteStringRange.create("B", "");
    List<ByteStringRange> partitions = Arrays.asList(partition2, partition3);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(Collections.singletonList(partition1), missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionFromKeySpaceMissingEndPartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "A");
    ByteStringRange partition2 = ByteStringRange.create("A", "B");
    ByteStringRange partition3 = ByteStringRange.create("B", "");
    List<ByteStringRange> partitions = Arrays.asList(partition1, partition2);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(Collections.singletonList(partition3), missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionFromKeySpaceMissingMiddlePartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "A");
    ByteStringRange partition2 = ByteStringRange.create("A", "B");
    ByteStringRange partition3 = ByteStringRange.create("B", "");
    List<ByteStringRange> partitions = Arrays.asList(partition1, partition3);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(Collections.singletonList(partition2), missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionFromKeySpaceOverlapPartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "B");
    ByteStringRange partition2 = ByteStringRange.create("A", "");
    List<ByteStringRange> partitions = Arrays.asList(partition1, partition2);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(
        Collections.singletonList(ByteStringRange.create("B", "A")),
        missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionFromKeySpaceOverlapAndMissingPartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "B");
    ByteStringRange partition2 = ByteStringRange.create("C", "D");
    ByteStringRange partition3 = ByteStringRange.create("A", "C");
    ByteStringRange partition4 = ByteStringRange.create("E", "");
    ByteStringRange partition5 = ByteStringRange.create("C", "E");
    List<ByteStringRange> partitions =
        Arrays.asList(partition1, partition2, partition3, partition4, partition5);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(
        Arrays.asList(ByteStringRange.create("B", "A"), ByteStringRange.create("D", "C")),
        missingAndOverlappingPartitions);
  }

  @Test
  public void testGetMissingAndOverlappingPartitionsFromKeySpaceOverlapWithOpenEndKey() {
    ByteStringRange fullKeySpace = ByteStringRange.create("", "");
    ByteStringRange partialKeySpace = ByteStringRange.create("n", "");
    List<ByteStringRange> partitions = Arrays.asList(fullKeySpace, partialKeySpace);
    // TODO come up with a better way to differentiate missing with start key "" and overlapping
    // with end key ""
    ByteStringRange overlappingPartition = ByteStringRange.create("", "n");
    List<ByteStringRange> expectedOverlapping = Collections.singletonList(overlappingPartition);
    List<ByteStringRange> missingAndOverlappingPartitions =
        getMissingAndOverlappingPartitionsFromKeySpace(partitions);
    assertEquals(expectedOverlapping, missingAndOverlappingPartitions);
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

  @Test
  public void testOverlappingByteStringRange() {
    ByteStringRange partition1 = ByteStringRange.create("", "b");
    ByteStringRange partition2 = ByteStringRange.create("a", "");
    assertTrue(doPartitionsOverlap(partition1, partition2));
    assertTrue(doPartitionsOverlap(partition2, partition1));
  }

  @Test
  public void testNonOverlappingByteStringRange() {
    ByteStringRange partition1 = ByteStringRange.create("", "a");
    ByteStringRange partition2 = ByteStringRange.create("a", "");
    assertFalse(doPartitionsOverlap(partition1, partition2));
    assertFalse(doPartitionsOverlap(partition2, partition1));
  }

  @Test
  public void testOverlappingByteStringRangeWithEmptyEndKey() {
    ByteStringRange partition1 = ByteStringRange.create("a", "");
    ByteStringRange partition2 = ByteStringRange.create("b", "");
    ByteStringRange partition3 = ByteStringRange.create("b", "c");
    ByteStringRange partition4 = ByteStringRange.create("", "b");
    assertTrue(doPartitionsOverlap(partition1, partition2));
    assertTrue(doPartitionsOverlap(partition2, partition1));
    assertTrue(doPartitionsOverlap(partition1, partition3));
    assertTrue(doPartitionsOverlap(partition3, partition1));
    assertTrue(doPartitionsOverlap(partition1, partition4));
    assertTrue(doPartitionsOverlap(partition4, partition1));
    assertTrue(doPartitionsOverlap(partition2, partition3));
    assertTrue(doPartitionsOverlap(partition3, partition2));
    assertFalse(doPartitionsOverlap(partition2, partition4));
    assertFalse(doPartitionsOverlap(partition4, partition2));
    assertFalse(doPartitionsOverlap(partition3, partition4));
    assertFalse(doPartitionsOverlap(partition4, partition3));
  }

  @Test
  public void testIsValidPartition() {
    ByteStringRange validPartition1 = ByteStringRange.create("a", "");
    ByteStringRange validPartition2 = ByteStringRange.create("", "");
    ByteStringRange validPartition3 = ByteStringRange.create("", "z");
    ByteStringRange validPartition4 = ByteStringRange.create("a", "b");
    ByteStringRange invalidPartition1 = ByteStringRange.create("b", "a");
    assertTrue(isValidPartition(validPartition1));
    assertTrue(isValidPartition(validPartition2));
    assertTrue(isValidPartition(validPartition3));
    assertTrue(isValidPartition(validPartition4));
    assertFalse(isValidPartition(invalidPartition1));
  }

  @Test
  public void testGetIntersectingPartition() {
    ByteStringRange partition1 = ByteStringRange.create("", "b");
    ByteStringRange partition2 = ByteStringRange.create("", "a");
    ByteStringRange partition3 = ByteStringRange.create("a", "c");
    ByteStringRange partition4 = ByteStringRange.create("b", "d");
    ByteStringRange partition5 = ByteStringRange.create("c", "d");
    ByteStringRange partition6 = ByteStringRange.create("a", "");
    ByteStringRange partition7 = ByteStringRange.create("b", "");
    assertEquals(ByteStringRange.create("", "a"), getIntersectingPartition(partition1, partition2));
    assertEquals(
        ByteStringRange.create("a", "b"), getIntersectingPartition(partition1, partition3));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition1, partition4));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition1, partition5));
    assertEquals(
        ByteStringRange.create("a", "b"), getIntersectingPartition(partition1, partition6));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition1, partition7));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition2, partition3));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition2, partition4));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition2, partition5));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition2, partition6));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition2, partition7));
    assertEquals(
        ByteStringRange.create("b", "c"), getIntersectingPartition(partition3, partition4));
    assertThrows(
        IllegalArgumentException.class, () -> getIntersectingPartition(partition3, partition5));
    assertEquals(
        ByteStringRange.create("a", "c"), getIntersectingPartition(partition3, partition6));
    assertEquals(
        ByteStringRange.create("b", "c"), getIntersectingPartition(partition3, partition7));
    assertEquals(
        ByteStringRange.create("c", "d"), getIntersectingPartition(partition4, partition5));
    assertEquals(
        ByteStringRange.create("b", "d"), getIntersectingPartition(partition4, partition6));
    assertEquals(
        ByteStringRange.create("b", "d"), getIntersectingPartition(partition4, partition7));
    assertEquals(
        ByteStringRange.create("c", "d"), getIntersectingPartition(partition5, partition6));
    assertEquals(
        ByteStringRange.create("c", "d"), getIntersectingPartition(partition5, partition7));
    assertEquals(ByteStringRange.create("b", ""), getIntersectingPartition(partition6, partition7));
  }
}
