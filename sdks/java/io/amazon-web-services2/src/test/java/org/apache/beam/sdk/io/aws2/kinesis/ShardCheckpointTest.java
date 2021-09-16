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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static software.amazon.kinesis.common.InitialPositionInStream.LATEST;
import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

import java.io.IOException;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/** */
@RunWith(MockitoJUnitRunner.class)
public class ShardCheckpointTest {

  private static final String AT_SEQUENCE_SHARD_IT = "AT_SEQUENCE_SHARD_IT";
  private static final String AFTER_SEQUENCE_SHARD_IT = "AFTER_SEQUENCE_SHARD_IT";
  private static final String STREAM_NAME = "STREAM";
  private static final String SHARD_ID = "SHARD_ID";
  @Mock private SimplifiedKinesisClient client;

  @Before
  public void setUp() throws IOException, TransientKinesisException {
    when(client.getShardIterator(
            eq(STREAM_NAME),
            eq(SHARD_ID),
            eq(AT_SEQUENCE_NUMBER),
            anyString(),
            isNull(Instant.class)))
        .thenReturn(AT_SEQUENCE_SHARD_IT);
    when(client.getShardIterator(
            eq(STREAM_NAME),
            eq(SHARD_ID),
            eq(AFTER_SEQUENCE_NUMBER),
            anyString(),
            isNull(Instant.class)))
        .thenReturn(AFTER_SEQUENCE_SHARD_IT);
  }

  @Test
  public void testProvidingShardIterator() throws IOException, TransientKinesisException {
    assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", null).getShardIterator(client))
        .isEqualTo(AT_SEQUENCE_SHARD_IT);
    assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", null).getShardIterator(client))
        .isEqualTo(AFTER_SEQUENCE_SHARD_IT);
    assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", 10L).getShardIterator(client))
        .isEqualTo(AT_SEQUENCE_SHARD_IT);
    assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", 10L).getShardIterator(client))
        .isEqualTo(AT_SEQUENCE_SHARD_IT);
  }

  @Test
  public void testComparisonWithExtendedSequenceNumber() {
    assertThat(
            new ShardCheckpoint("", "", new StartingPoint(LATEST))
                .isBeforeOrAt(recordWith(new ExtendedSequenceNumber("100", 0L))))
        .isTrue();

    assertThat(
            new ShardCheckpoint("", "", new StartingPoint(TRIM_HORIZON))
                .isBeforeOrAt(recordWith(new ExtendedSequenceNumber("100", 0L))))
        .isTrue();

    assertThat(
            checkpoint(AFTER_SEQUENCE_NUMBER, "10", 1L)
                .isBeforeOrAt(recordWith(new ExtendedSequenceNumber("100", 0L))))
        .isTrue();

    assertThat(
            checkpoint(AT_SEQUENCE_NUMBER, "100", 0L)
                .isBeforeOrAt(recordWith(new ExtendedSequenceNumber("100", 0L))))
        .isTrue();

    assertThat(
            checkpoint(AFTER_SEQUENCE_NUMBER, "100", 0L)
                .isBeforeOrAt(recordWith(new ExtendedSequenceNumber("100", 0L))))
        .isFalse();

    assertThat(
            checkpoint(AT_SEQUENCE_NUMBER, "100", 1L)
                .isBeforeOrAt(recordWith(new ExtendedSequenceNumber("100", 0L))))
        .isFalse();

    assertThat(
            checkpoint(AFTER_SEQUENCE_NUMBER, "100", 0L)
                .isBeforeOrAt(recordWith(new ExtendedSequenceNumber("99", 1L))))
        .isFalse();
  }

  @Test
  public void testComparisonWithTimestamp() {
    DateTime referenceTimestamp = DateTime.now();

    assertThat(
            checkpoint(AT_TIMESTAMP, referenceTimestamp.toInstant())
                .isBeforeOrAt(recordWith(referenceTimestamp.minusMillis(10).toInstant())))
        .isFalse();

    assertThat(
            checkpoint(AT_TIMESTAMP, referenceTimestamp.toInstant())
                .isBeforeOrAt(recordWith(referenceTimestamp.toInstant())))
        .isTrue();

    assertThat(
            checkpoint(AT_TIMESTAMP, referenceTimestamp.toInstant())
                .isBeforeOrAt(recordWith(referenceTimestamp.plusMillis(10).toInstant())))
        .isTrue();
  }

  private KinesisRecord recordWith(ExtendedSequenceNumber extendedSequenceNumber) {
    KinesisRecord record = mock(KinesisRecord.class);
    when(record.getExtendedSequenceNumber()).thenReturn(extendedSequenceNumber);
    return record;
  }

  private ShardCheckpoint checkpoint(
      ShardIteratorType iteratorType, String sequenceNumber, Long subSequenceNumber) {
    return new ShardCheckpoint(
        STREAM_NAME, SHARD_ID, iteratorType, sequenceNumber, subSequenceNumber);
  }

  private KinesisRecord recordWith(Instant approximateArrivalTimestamp) {
    KinesisRecord record = mock(KinesisRecord.class);
    when(record.getApproximateArrivalTimestamp()).thenReturn(approximateArrivalTimestamp);
    return record;
  }

  private ShardCheckpoint checkpoint(ShardIteratorType iteratorType, Instant timestamp) {
    return new ShardCheckpoint(STREAM_NAME, SHARD_ID, iteratorType, timestamp);
  }
}
