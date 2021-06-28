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
package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;
import static org.junit.Assert.*;

import com.google.cloud.Timestamp;
import org.junit.Test;

public class PartitionRestrictionTest {

  @Test
  public void testQueryChangeStreamRestriction() {
    assertEquals(
        PartitionRestriction.queryChangeStream(Timestamp.MIN_VALUE, Timestamp.MAX_VALUE),
        new PartitionRestriction(
            Timestamp.MIN_VALUE, Timestamp.MAX_VALUE, QUERY_CHANGE_STREAM, null));
  }

  @Test
  public void testWaitForChildPartitionsRestriction() {
    assertEquals(
        PartitionRestriction.waitForChildPartitions(10L),
        new PartitionRestriction(null, null, WAIT_FOR_CHILD_PARTITIONS, 10L));
  }

  @Test
  public void testFinishPartitionRestriction() {
    assertEquals(
        PartitionRestriction.finishPartition(),
        new PartitionRestriction(null, null, FINISH_PARTITION, null));
  }

  @Test
  public void testWaitForParentPartitionsRestriction() {
    assertEquals(
        PartitionRestriction.waitForParentPartitions(),
        new PartitionRestriction(null, null, WAIT_FOR_PARENT_PARTITIONS, null));
  }

  @Test
  public void testDeletePartitionRestriction() {
    assertEquals(
        PartitionRestriction.deletePartition(),
        new PartitionRestriction(null, null, DELETE_PARTITION, null));
  }

  @Test
  public void testDoneRestriction() {
    assertEquals(PartitionRestriction.done(), new PartitionRestriction(null, null, DONE, null));
  }

  @Test
  public void testStopRestriction() {
    assertEquals(PartitionRestriction.stop(), new PartitionRestriction(null, null, STOP, null));
  }
}
