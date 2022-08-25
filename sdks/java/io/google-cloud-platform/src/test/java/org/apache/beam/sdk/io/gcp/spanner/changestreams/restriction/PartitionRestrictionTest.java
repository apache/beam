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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.UPDATE_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import org.junit.Before;
import org.junit.Test;

public class PartitionRestrictionTest {

  private Timestamp startTimestamp;
  private Timestamp endTimestamp;

  @Before
  public void setUp() throws Exception {
    startTimestamp = Timestamp.MIN_VALUE;
    endTimestamp = Timestamp.MAX_VALUE;
  }

  @Test
  public void testUpdateStateRestriction() {
    assertEquals(
        PartitionRestriction.updateState(startTimestamp, endTimestamp),
        new PartitionRestriction(startTimestamp, endTimestamp, UPDATE_STATE, null));
  }

  @Test
  public void testQueryChangeStreamRestriction() {
    assertEquals(
        PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp),
        new PartitionRestriction(startTimestamp, endTimestamp, QUERY_CHANGE_STREAM, null));
  }

  @Test
  public void testWaitForChildPartitionsRestriction() {
    assertEquals(
        PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp),
        new PartitionRestriction(startTimestamp, endTimestamp, WAIT_FOR_CHILD_PARTITIONS, null));
  }

  @Test
  public void testDoneRestriction() {
    assertEquals(
        PartitionRestriction.done(startTimestamp, endTimestamp),
        new PartitionRestriction(startTimestamp, endTimestamp, DONE, null));
  }

  @Test
  public void testStopRestriction() {
    final PartitionRestriction restriction =
        PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp);
    assertEquals(
        PartitionRestriction.stop(restriction),
        new PartitionRestriction(startTimestamp, endTimestamp, STOP, QUERY_CHANGE_STREAM));
  }
}
