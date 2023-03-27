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

import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.mockRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.Helpers.mockShardIterators;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.createAggregatedRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.createRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.common.InitialPositionInStream;

/** Tests {@link ShardReadersPool} with less mocks. */
@RunWith(MockitoJUnitRunner.class)
public class ShardReadersPoolExtendedTest {
  private static final String STREAM = "stream-0";
  private static final String SHARD_0 = "0";
  private static final int GET_RECORDS_LIMIT = 100;

  @Mock private KinesisClient kinesis;
  @Mock private CloudWatchClient cloudWatch;
  private ShardReadersPool shardReadersPool;
  private SimplifiedKinesisClient simplifiedKinesisClient;

  @Before
  public void setUp() {
    simplifiedKinesisClient =
        new SimplifiedKinesisClient(() -> kinesis, () -> cloudWatch, GET_RECORDS_LIMIT);
  }

  @Test
  public void testNextRecordReturnsRecords() throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        initCheckpoint(ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 0L);
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = createRecords(1, 3);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 3);

    shardReadersPool.start();

    // before fetching anything:
    assertThat(shardReadersPool.getCheckpointMark())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 0L));

    // record with seq num = 0 is skipped
    consumeAndCheckNonAggregatedRecords(1, 3);
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @Test
  public void testNextRecordReturnsNonAggregatedRecordsIfSubSeqNumIsPositive()
      throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        initCheckpoint(ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 125L);
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = createRecords(1, 3);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 3);
    shardReadersPool.start();

    // record with seq num = 0 is skipped
    consumeAndCheckNonAggregatedRecords(1, 3);
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @Test
  public void testNextRecordReturnsRecordsWhenStartedAtTrimHorizon()
      throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        initCheckpoint(ShardIteratorType.TRIM_HORIZON, null, null);
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = createRecords(1, 3);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 3);

    shardReadersPool.start();

    // before fetching anything:
    assertThat(shardReadersPool.getCheckpointMark())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                STREAM, SHARD_0, new StartingPoint(InitialPositionInStream.TRIM_HORIZON)));

    consumeAndCheckNonAggregatedRecords(0, 3);
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @Test
  public void testNextRecordReturnsRecordsWhenStartedAtLatest() throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        initCheckpoint(ShardIteratorType.LATEST, null, null);
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = createRecords(1, 3);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 3);

    shardReadersPool.start();

    // before fetching anything:
    assertThat(shardReadersPool.getCheckpointMark())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.LATEST, null, null));

    consumeAndCheckNonAggregatedRecords(0, 3);
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @Test
  public void testNextRecordReturnsDeAggregatedRecords() throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, SHARD_0, new StartingPoint(InitialPositionInStream.LATEST))));
    shardReadersPool = initPool(initialCheckpoint);

    List<List<Record>> records = createAggregatedRecords(1, 6);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 1);

    shardReadersPool.start();

    // before fetching anything:
    assertThat(shardReadersPool.getCheckpointMark())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                STREAM, SHARD_0, new StartingPoint(InitialPositionInStream.LATEST)));

    consumerAndCheckAggregatedRecords(0L, 3L);

    // re-initialize pool from the previous checkpoint
    KinesisReaderCheckpoint intermediateCheckpoint = shardReadersPool.getCheckpointMark();
    shardReadersPool.stop();
    shardReadersPool = initPool(intermediateCheckpoint);
    shardReadersPool.start();

    // consume the rest of records
    consumerAndCheckAggregatedRecords(3L, 6L);
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @Test
  public void testNextRecordReturnsDeAggregatedRecordsWhenStartedAfterSeqNum()
      throws TransientKinesisException {
    KinesisReaderCheckpoint initialCheckpoint =
        initCheckpoint(ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 2L);
    List<List<Record>> records = createAggregatedRecords(1, 6);
    mockShardIterators(kinesis, records);
    mockRecords(kinesis, records, 1);
    shardReadersPool = initPool(initialCheckpoint);
    shardReadersPool.start();

    // before fetching anything:
    assertThat(shardReadersPool.getCheckpointMark())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", 2L));

    // consume the rest of records - filter drops the one with subSeqNum = 2L
    consumerAndCheckAggregatedRecords(3L, 6L);
    assertThat(shardReadersPool.nextRecord().isPresent()).isFalse();
  }

  @After
  public void clean() throws Exception {
    shardReadersPool.stop();
    simplifiedKinesisClient.close();
    verify(kinesis).close();
    verifyNoInteractions(cloudWatch);
  }

  private static KinesisIO.Read spec() {
    return KinesisIO.read().withStreamName(STREAM);
  }

  private ShardReadersPool initPool(KinesisReaderCheckpoint initialCheckpoint) {
    return new ShardReadersPool(spec(), simplifiedKinesisClient, initialCheckpoint);
  }

  private KinesisReaderCheckpoint initCheckpoint(
      ShardIteratorType type, String seqNum, Long subSeqNum) {
    return new KinesisReaderCheckpoint(
        ImmutableList.of(new ShardCheckpoint(STREAM, SHARD_0, type, seqNum, subSeqNum)));
  }

  private void consumeAndCheckNonAggregatedRecords(int startSeqNum, int endSeqNum) {
    for (int i = startSeqNum; i < endSeqNum; i++) {
      KinesisRecord kinesisRecord = shardReadersPool.nextRecord().get();
      assertThat(kinesisRecord.getSequenceNumber()).isEqualTo(String.valueOf(i));
      assertThat(kinesisRecord.getSubSequenceNumber()).isEqualTo(0L);
      assertThat(shardReadersPool.getCheckpointMark())
          .containsExactlyInAnyOrder(
              new ShardCheckpoint(
                  STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, String.valueOf(i), 0L));
    }
  }

  private void consumerAndCheckAggregatedRecords(long startSubSeqNum, long endSubSeqNum) {
    for (long i = startSubSeqNum; i < endSubSeqNum; i++) {
      KinesisRecord kinesisRecord = shardReadersPool.nextRecord().get();
      assertThat(kinesisRecord.getSequenceNumber()).isEqualTo("0");
      assertThat(kinesisRecord.getSubSequenceNumber()).isEqualTo(i);
      assertThat(shardReadersPool.getCheckpointMark())
          .containsExactlyInAnyOrder(
              new ShardCheckpoint(
                  STREAM, SHARD_0, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "0", i));
    }
  }
}
