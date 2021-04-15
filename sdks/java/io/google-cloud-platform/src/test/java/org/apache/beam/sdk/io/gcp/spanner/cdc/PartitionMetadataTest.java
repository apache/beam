package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.io.gcp.spanner.cdc.PartitionMetadata.State;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionMetadataTest {

  public static final Timestamp A_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(1618361869, 0);
  public static final String A_PARTITION_TOKEN = "partitionToken123";

  @Test
  public void testBuilderDefaultsToInclusiveStartAndExclusiveEnd() {
    PartitionMetadata expectedPartitionMetadata = new PartitionMetadata(
        A_PARTITION_TOKEN,
        A_PARTITION_TOKEN,
        A_TIMESTAMP,
        true,
        A_TIMESTAMP,
        false,
        10,
        State.CREATED,
        A_TIMESTAMP,
        A_TIMESTAMP);
    PartitionMetadata actualPartitionMetadata = PartitionMetadata.newBuilder()
        .setPartitionToken(A_PARTITION_TOKEN)
        .setParentToken(A_PARTITION_TOKEN)
        .setStartTimestamp(A_TIMESTAMP)
        .setEndTimestamp(A_TIMESTAMP)
        .setHeartbeatSeconds(10)
        .setState(State.CREATED)
        .setCreatedAt(A_TIMESTAMP)
        .setUpdatedAt(A_TIMESTAMP)
        .build();
    assertEquals(expectedPartitionMetadata, actualPartitionMetadata);
  }

  @Test
  public void testBuilderThrowsExceptionWhenPartitionTokenMissing() {
    assertThrows("partitionToken", IllegalStateException.class, () ->
        PartitionMetadata.newBuilder()
            .setParentToken(A_PARTITION_TOKEN)
            .setStartTimestamp(A_TIMESTAMP)
            .setEndTimestamp(A_TIMESTAMP)
            .setHeartbeatSeconds(10)
            .setState(State.CREATED)
            .setCreatedAt(A_TIMESTAMP)
            .setUpdatedAt(A_TIMESTAMP)
            .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenParentTokenMissing() {
    assertThrows("parentToken", IllegalStateException.class, () ->
        PartitionMetadata.newBuilder()
            .setPartitionToken(A_PARTITION_TOKEN)
            .setStartTimestamp(A_TIMESTAMP)
            .setEndTimestamp(A_TIMESTAMP)
            .setHeartbeatSeconds(10)
            .setState(State.CREATED)
            .setCreatedAt(A_TIMESTAMP)
            .setUpdatedAt(A_TIMESTAMP)
            .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenStartTimestampMissing() {
    assertThrows("startTimestamp", IllegalStateException.class, () ->
        PartitionMetadata.newBuilder()
            .setPartitionToken(A_PARTITION_TOKEN)
            .setParentToken(A_PARTITION_TOKEN)
            .setEndTimestamp(A_TIMESTAMP)
            .setHeartbeatSeconds(10)
            .setState(State.CREATED)
            .setCreatedAt(A_TIMESTAMP)
            .setUpdatedAt(A_TIMESTAMP)
            .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenHeartbeatSecondsMissing() {
    assertThrows("heartbeatSeconds", IllegalStateException.class, () ->
        PartitionMetadata.newBuilder()
            .setPartitionToken(A_PARTITION_TOKEN)
            .setParentToken(A_PARTITION_TOKEN)
            .setStartTimestamp(A_TIMESTAMP)
            .setEndTimestamp(A_TIMESTAMP)
            .setState(State.CREATED)
            .setCreatedAt(A_TIMESTAMP)
            .setUpdatedAt(A_TIMESTAMP)
            .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenStateMissing() {
    assertThrows("state", IllegalStateException.class, () ->
        PartitionMetadata.newBuilder()
            .setPartitionToken(A_PARTITION_TOKEN)
            .setParentToken(A_PARTITION_TOKEN)
            .setStartTimestamp(A_TIMESTAMP)
            .setEndTimestamp(A_TIMESTAMP)
            .setHeartbeatSeconds(10)
            .setCreatedAt(A_TIMESTAMP)
            .setUpdatedAt(A_TIMESTAMP)
            .build());
  }
}
