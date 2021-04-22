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
    PartitionMetadata expectedPartitionMetadata =
        new PartitionMetadata(
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
    PartitionMetadata actualPartitionMetadata =
        PartitionMetadata.newBuilder()
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
    assertThrows(
        "partitionToken",
        IllegalStateException.class,
        () ->
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
    assertThrows(
        "parentToken",
        IllegalStateException.class,
        () ->
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
    assertThrows(
        "startTimestamp",
        IllegalStateException.class,
        () ->
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
    assertThrows(
        "heartbeatSeconds",
        IllegalStateException.class,
        () ->
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
    assertThrows(
        "state",
        IllegalStateException.class,
        () ->
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
