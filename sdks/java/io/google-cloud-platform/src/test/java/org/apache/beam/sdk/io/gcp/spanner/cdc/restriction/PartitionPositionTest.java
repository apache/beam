package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;
import static org.junit.Assert.*;

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
  public void testPositionQueryChangeStream() {
    assertEquals(
        new PartitionPosition(Optional.of(timestamp), QUERY_CHANGE_STREAM, Optional.empty()),
        PartitionPosition.queryChangeStream(timestamp)
    );
  }

  @Test
  public void testPositionWaitForChildPartitions() {
    assertEquals(
        new PartitionPosition(Optional.empty(), WAIT_FOR_CHILD_PARTITIONS, Optional.of(20L)),
        PartitionPosition.waitForChildPartitions(20L)
    );
  }

  @Test
  public void testPositionFinishPartition() {
    assertEquals(
        new PartitionPosition(Optional.empty(), FINISH_PARTITION, Optional.empty()),
        PartitionPosition.finishPartition()
    );
  }

  @Test
  public void testPositionWaitForParentPartitions() {
    assertEquals(
        new PartitionPosition(Optional.empty(), WAIT_FOR_PARENT_PARTITIONS, Optional.empty()),
        PartitionPosition.waitForParentPartitions()
    );
  }

  @Test
  public void testPositionDeletePartition() {
    assertEquals(
        new PartitionPosition(Optional.empty(), DELETE_PARTITION, Optional.empty()),
        PartitionPosition.deletePartition()
    );
  }

  @Test
  public void testPositionDone() {
    assertEquals(
        new PartitionPosition(Optional.empty(), DONE, Optional.empty()),
        PartitionPosition.done()
    );
  }
}
