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
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class PartitionPositionTest {

  private Timestamp timestamp;

  @Before
  public void setUp() {
    timestamp = Timestamp.now();
  }

  @Test
  public void testPositionUpdateState() {
    assertEquals(
        new PartitionPosition(Optional.empty(), UPDATE_STATE), PartitionPosition.updateState());
  }

  @Test
  public void testPositionQueryChangeStream() {
    assertEquals(
        new PartitionPosition(Optional.of(timestamp), QUERY_CHANGE_STREAM),
        PartitionPosition.queryChangeStream(timestamp));
  }

  @Test
  public void testPositionWaitForChildPartitions() {
    assertEquals(
        new PartitionPosition(Optional.empty(), WAIT_FOR_CHILD_PARTITIONS),
        PartitionPosition.waitForChildPartitions());
  }

  @Test
  public void testPositionDone() {
    assertEquals(new PartitionPosition(Optional.empty(), DONE), PartitionPosition.done());
  }

  @Test
  public void testPositionStop() {
    assertEquals(new PartitionPosition(Optional.empty(), STOP), PartitionPosition.stop());
  }
}
