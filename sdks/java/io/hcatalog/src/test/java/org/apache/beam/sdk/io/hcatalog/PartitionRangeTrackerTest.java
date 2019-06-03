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
package org.apache.beam.sdk.io.hcatalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link PartitionRangeTracker}. */
@RunWith(JUnit4.class)
public class PartitionRangeTrackerTest {

  public static final String TEST_DATABASE = "default";
  public static final String TEST_TABLE = "mytable";
  private ImmutableList<Partition> partitions;
  private final PartitionCreateTimeComparator partitionCreateTimeComparator =
      new PartitionCreateTimeComparator();
  private final PartitionRange emptyPartitions =
      new PartitionRange(ImmutableList.of(), partitionCreateTimeComparator, null);

  @Rule public final ExpectedException expected = ExpectedException.none();

  @Rule
  public final transient TestRule testDataSetupRule =
      new TestWatcher() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          return new Statement() {
            @Override
            public void evaluate() throws Throwable {
              if (description.getAnnotation(PartitionRangeTrackerTest.NeedsTestData.class)
                  != null) {
                prepareTestData();
              }
              base.evaluate();
            }
          };
        }
      };

  /** Use this annotation to setup partition data to be used in all tests. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  private @interface NeedsTestData {}

  @Test
  @NeedsTestData
  public void testClaimingPartition() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertEquals(3, tracker.currentRestriction().getPartitions().size());
    assertEquals(partitions, tracker.currentRestriction().getPartitions());
    // Claim partition p1
    assertTrue(tracker.tryClaim(partitions.get(0)));
    // Claiming the same partition again, should not be allowed.
    assertFalse(tracker.tryClaim(partitions.get(0)));
  }

  @Test
  @NeedsTestData
  public void testClaimingPartitionInWrongOrder() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertTrue(tracker.tryClaim(partitions.get(1))); // createTime 14
    assertFalse(tracker.tryClaim(partitions.get(0))); // createTime 10
    assertFalse(tracker.tryClaim(partitions.get(2))); // createTime 13
  }

  @Test
  @NeedsTestData
  public void testLastClaimedPartition() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertTrue(tracker.tryClaim(partitions.get(0)));
    tracker.checkpoint();
    assertEquals(partitions.get(0), tracker.currentRestriction().getLastCompletedPartition());
  }

  @Test
  @NeedsTestData
  public void testCheckpointUnstarted() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    final PartitionRange checkpoint = tracker.checkpoint();
    // We expect to get the original range back and that the current restriction
    // is effectively made empty.
    assertEquals(range, checkpoint);
    assertEquals(emptyPartitions, tracker.currentRestriction());
  }

  @Test
  @NeedsTestData
  public void testCheckpointRegular() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertTrue(tracker.tryClaim(partitions.get(0))); // createTime 10
    assertTrue(tracker.tryClaim(partitions.get(2))); // createTime 13
    final PartitionRange checkpoint = tracker.checkpoint();

    PartitionRange currentRestriction =
        new PartitionRange(
            ImmutableList.of(partitions.get(0), partitions.get(2)),
            partitionCreateTimeComparator,
            partitions.get(2));
    PartitionRange remainingPartitions =
        new PartitionRange(
            ImmutableList.of(partitions.get(1)), partitionCreateTimeComparator, partitions.get(2));
    assertEquals(currentRestriction, tracker.currentRestriction());
    assertEquals(remainingPartitions, checkpoint);
  }

  @Test
  @NeedsTestData
  public void testCheckpointAtLast() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertTrue(tracker.tryClaim(partitions.get(0))); // createTime 10
    assertTrue(tracker.tryClaim(partitions.get(2))); // createTime 13
    assertTrue(tracker.tryClaim(partitions.get(1))); // createTime 14;
    final PartitionRange checkpoint = tracker.checkpoint();
    PartitionRange currentRestriction =
        new PartitionRange(
            ImmutableList.of(partitions.get(0), partitions.get(2), partitions.get(1)),
            partitionCreateTimeComparator,
            partitions.get(1));
    assertEquals(currentRestriction, tracker.currentRestriction());
    assertEquals(emptyPartitions, checkpoint);
  }

  @Test
  @NeedsTestData
  public void testBacklogWithEmptyRange() {
    PartitionRange range =
        new PartitionRange(ImmutableList.of(), partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertEquals(BigDecimal.ZERO, tracker.getBacklog().backlog());
  }

  @Test
  @NeedsTestData
  public void testBacklogUnstarted() {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertEquals(BigDecimal.valueOf(3), tracker.getBacklog().backlog());
  }

  @Test
  @NeedsTestData
  public void testBacklogPartiallyCompleted() {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    tracker.tryClaim(partitions.get(0));
    assertEquals(BigDecimal.valueOf(2), tracker.getBacklog().backlog());

    tracker.tryClaim(partitions.get(2));
    tracker.tryClaim(partitions.get(1));
    // We have claimed all partitions. There is no backlog remaining.
    assertEquals(BigDecimal.ZERO, tracker.getBacklog().backlog());
  }

  @Test
  @NeedsTestData
  public void testCheckDoneAfterTryClaimAtEndOfRange() {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);

    assertTrue(tracker.tryClaim(partitions.get(0)));
    assertTrue(tracker.tryClaim(partitions.get(2)));
    assertTrue(tracker.tryClaim(partitions.get(1)));
    tracker.checkDone();
  }

  @Test
  @NeedsTestData
  public void testCheckDoneForEmptyRange() {
    PartitionRange range =
        new PartitionRange(ImmutableList.of(), partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    tracker.checkDone();
  }

  @Test
  @NeedsTestData
  public void testCheckDoneWhenNotDone() {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);

    assertTrue(tracker.tryClaim(partitions.get(0)));
    assertTrue(tracker.tryClaim(partitions.get(2)));
    expected.expectMessage(
        "Last attempted partition was at index 1, claiming work from index [2] to [3] was not attempted");
    tracker.checkDone();
  }

  @Test
  @NeedsTestData
  public void testCheckDoneUnstarted() {
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.of(0x10), ByteKey.of(0xc0)));
    expected.expect(IllegalStateException.class);
    tracker.checkDone();
  }

  private void prepareTestData() {
    Partition p1 = createPartition(new ArrayList<>(Arrays.asList("10", "20")), 10, 10);
    Partition p2 = createPartition(new ArrayList<>(Arrays.asList("10", "20")), 14, 14);
    Partition p3 = createPartition(new ArrayList<>(Arrays.asList("10", "20")), 13, 13);
    this.partitions = ImmutableList.of(p1, p2, p3);
  }

  private Partition createPartition(List<String> values, int createTime, int lastAccessTime) {
    return new Partition(values, TEST_DATABASE, TEST_TABLE, createTime, lastAccessTime, null, null);
  }

  private static class PartitionCreateTimeComparator implements SerializableComparator<Partition> {
    @Override
    public int compare(Partition o1, Partition o2) {
      if (o1.getCreateTime() > o2.getCreateTime()) {
        return 1;
      } else if (o1.getCreateTime() < o2.getCreateTime()) {
        return -1;
      }
      return 0;
    }
  }
}
