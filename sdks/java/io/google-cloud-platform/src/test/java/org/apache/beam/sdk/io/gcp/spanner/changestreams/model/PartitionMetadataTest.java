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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionMetadataTest {

  private static final String PARTITION_TOKEN = "partitionToken123";
  private static final String PARENT_TOKEN = "parentToken123";
  private static final Timestamp START_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(1, 1);
  private static final Timestamp END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(2, 2);
  private static final Timestamp WATERMARK = Timestamp.ofTimeSecondsAndNanos(3, 3);
  private static final Timestamp CREATED_AT = Timestamp.ofTimeSecondsAndNanos(4, 4);
  private static final Timestamp SCHEDULED_AT = Timestamp.ofTimeSecondsAndNanos(5, 5);
  private static final Timestamp RUNNING_AT = Timestamp.ofTimeSecondsAndNanos(6, 6);
  private static final Timestamp FINISHED_AT = Timestamp.ofTimeSecondsAndNanos(7, 7);

  @Test
  public void testBuilderDefaultsToInclusiveStartAndExclusiveEnd() {
    PartitionMetadata expectedPartitionMetadata =
        new PartitionMetadata(
            PARTITION_TOKEN,
            Sets.newHashSet(PARENT_TOKEN),
            START_TIMESTAMP,
            END_TIMESTAMP,
            10,
            State.RUNNING,
            WATERMARK,
            CREATED_AT,
            SCHEDULED_AT,
            RUNNING_AT,
            FINISHED_AT);
    PartitionMetadata actualPartitionMetadata =
        PartitionMetadata.newBuilder()
            .setPartitionToken(PARTITION_TOKEN)
            .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
            .setStartTimestamp(START_TIMESTAMP)
            .setEndTimestamp(END_TIMESTAMP)
            .setHeartbeatMillis(10)
            .setState(State.RUNNING)
            .setWatermark(WATERMARK)
            .setCreatedAt(CREATED_AT)
            .setScheduledAt(SCHEDULED_AT)
            .setRunningAt(RUNNING_AT)
            .setFinishedAt(FINISHED_AT)
            .build();

    assertEquals(expectedPartitionMetadata.hashCode(), actualPartitionMetadata.hashCode());
    assertEquals(expectedPartitionMetadata.toString(), actualPartitionMetadata.toString());
    assertEquals(expectedPartitionMetadata, actualPartitionMetadata);
  }

  @Test
  public void testBuilderDefaultsToCommitTimestampWhenCreatedAtIsNotGiven() {
    PartitionMetadata expectedPartitionMetadata =
        new PartitionMetadata(
            PARTITION_TOKEN,
            Sets.newHashSet(PARENT_TOKEN),
            START_TIMESTAMP,
            END_TIMESTAMP,
            10,
            State.RUNNING,
            WATERMARK,
            Value.COMMIT_TIMESTAMP,
            SCHEDULED_AT,
            RUNNING_AT,
            FINISHED_AT);
    PartitionMetadata actualPartitionMetadata =
        PartitionMetadata.newBuilder()
            .setPartitionToken(PARTITION_TOKEN)
            .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
            .setStartTimestamp(START_TIMESTAMP)
            .setEndTimestamp(END_TIMESTAMP)
            .setHeartbeatMillis(10)
            .setState(State.RUNNING)
            .setWatermark(WATERMARK)
            .setScheduledAt(SCHEDULED_AT)
            .setRunningAt(RUNNING_AT)
            .setFinishedAt(FINISHED_AT)
            .build();
    assertEquals(expectedPartitionMetadata, actualPartitionMetadata);
  }

  @Test
  public void testBuilderThrowsExceptionWhenPartitionTokenMissing() {
    assertThrows(
        "partitionToken",
        IllegalStateException.class,
        () ->
            PartitionMetadata.newBuilder()
                .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
                .setStartTimestamp(START_TIMESTAMP)
                .setEndTimestamp(END_TIMESTAMP)
                .setHeartbeatMillis(10)
                .setState(State.CREATED)
                .setWatermark(WATERMARK)
                .setCreatedAt(CREATED_AT)
                .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenParentTokenMissing() {
    assertThrows(
        "parentToken",
        IllegalStateException.class,
        () ->
            PartitionMetadata.newBuilder()
                .setPartitionToken(PARTITION_TOKEN)
                .setStartTimestamp(START_TIMESTAMP)
                .setEndTimestamp(END_TIMESTAMP)
                .setHeartbeatMillis(10)
                .setState(State.CREATED)
                .setWatermark(WATERMARK)
                .setCreatedAt(CREATED_AT)
                .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenStartTimestampMissing() {
    assertThrows(
        "startTimestamp",
        IllegalStateException.class,
        () ->
            PartitionMetadata.newBuilder()
                .setPartitionToken(PARTITION_TOKEN)
                .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
                .setEndTimestamp(END_TIMESTAMP)
                .setHeartbeatMillis(10)
                .setState(State.CREATED)
                .setWatermark(WATERMARK)
                .setCreatedAt(CREATED_AT)
                .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenHeartbeatMillisMissing() {
    assertThrows(
        "heartbeatMillis",
        IllegalStateException.class,
        () ->
            PartitionMetadata.newBuilder()
                .setPartitionToken(PARTITION_TOKEN)
                .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
                .setStartTimestamp(START_TIMESTAMP)
                .setEndTimestamp(END_TIMESTAMP)
                .setState(State.CREATED)
                .setWatermark(WATERMARK)
                .setCreatedAt(CREATED_AT)
                .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenStateMissing() {
    assertThrows(
        "state",
        IllegalStateException.class,
        () ->
            PartitionMetadata.newBuilder()
                .setPartitionToken(PARTITION_TOKEN)
                .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
                .setStartTimestamp(START_TIMESTAMP)
                .setEndTimestamp(END_TIMESTAMP)
                .setHeartbeatMillis(10)
                .setCreatedAt(CREATED_AT)
                .setWatermark(WATERMARK)
                .build());
  }

  @Test
  public void testBuilderThrowsExceptionWhenWatermarkMissing() {
    assertThrows(
        "state",
        IllegalStateException.class,
        () ->
            PartitionMetadata.newBuilder()
                .setPartitionToken(PARTITION_TOKEN)
                .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
                .setStartTimestamp(START_TIMESTAMP)
                .setEndTimestamp(END_TIMESTAMP)
                .setHeartbeatMillis(10)
                .setState(State.CREATED)
                .setCreatedAt(CREATED_AT)
                .build());
  }

  @Test
  public void testGetters() {
    PartitionMetadata partitionMetadata =
        new PartitionMetadata(
            PARTITION_TOKEN,
            Sets.newHashSet(PARENT_TOKEN),
            START_TIMESTAMP,
            END_TIMESTAMP,
            10,
            State.RUNNING,
            WATERMARK,
            CREATED_AT,
            SCHEDULED_AT,
            RUNNING_AT,
            FINISHED_AT);

    assertEquals(PARTITION_TOKEN, partitionMetadata.getPartitionToken());
    assertEquals(1, partitionMetadata.getParentTokens().size());
    assertEquals(PARENT_TOKEN, partitionMetadata.getParentTokens().iterator().next());
    assertEquals(START_TIMESTAMP, partitionMetadata.getStartTimestamp());
    assertEquals(END_TIMESTAMP, partitionMetadata.getEndTimestamp());
    assertEquals(10, partitionMetadata.getHeartbeatMillis());
    assertEquals(State.RUNNING, partitionMetadata.getState());
    assertEquals(WATERMARK, partitionMetadata.getWatermark());
    assertEquals(CREATED_AT, partitionMetadata.getCreatedAt());
    assertEquals(SCHEDULED_AT, partitionMetadata.getScheduledAt());
    assertEquals(RUNNING_AT, partitionMetadata.getRunningAt());
    assertEquals(FINISHED_AT, partitionMetadata.getFinishedAt());
  }
}
