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

import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.createReadSpec;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.eventWithAggRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.eventWithRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.eventsWithRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.eventsWithoutRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.reShardEvent;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.reShardEventWithRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.recordWithMinutesAgo;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.subscribeAfterSeqNumber;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.subscribeAtSeqNumber;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.subscribeAtTs;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.subscribeLatest;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.subscribeTrimHorizon;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import io.netty.handler.timeout.ReadTimeoutException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.common.InitialPositionInStream;

/** This must follow same checkpoint semantics as in {@link ShardReadersPoolExtendedTest}. */
public class EFOShardSubscribersPoolTest {
  private static final String STREAM = "stream-01";
  private static final String CONSUMER = "consumer-01";

  private KinesisIO.Read readSpec;
  private String consumerArn;
  private EFOStubbedKinesisAsyncClient kinesis;
  private EFOShardSubscribersPool pool;

  @Before
  public void setUp() {
    readSpec = createReadSpec();
    consumerArn = CONSUMER;
  }

  @After
  public void tearDown() {
    kinesis.close();
    pool.stop();
  }

  @Test
  public void poolReSubscribesAndReadsRecords() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(10, 3));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3, 5));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(8, 3));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 0, 10))
                .withLastCheckpointSequenceNumber(12),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 0, 8))
                .withLastCheckpointSequenceNumber(10));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "2"),
            subscribeAfterSeqNumber("shard-000", "9"),
            subscribeAfterSeqNumber("shard-001", "7"),
            subscribeAfterSeqNumber("shard-000", "12"),
            subscribeAfterSeqNumber("shard-001", "10"));
  }

  @Test
  public void poolReSubscribesAndReadsRecordsAfterCheckPoint() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(11, 3));

    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                afterCheckpoint("shard-000", "0"), afterCheckpoint("shard-001", "11")));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 1, 2))
                .withLastCheckpointSequenceNumber(2),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 12, 2))
                .withLastCheckpointSequenceNumber(13));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeAtSeqNumber("shard-000", "0"),
            subscribeAtSeqNumber("shard-001", "11"),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "13"));
  }

  @Test
  public void poolReSubscribesAndReadsRecordsWithTrimHorizon() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(11, 3));

    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                trimHorizonCheckpoint("shard-000"), trimHorizonCheckpoint("shard-001")));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 0, 3))
                .withLastCheckpointSequenceNumber(2),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 11, 3))
                .withLastCheckpointSequenceNumber(13));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeTrimHorizon("shard-000"),
            subscribeTrimHorizon("shard-001"),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "13"));
  }

  @Test
  public void poolReSubscribesAndReadsRecordsWithAtTimestamp() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(11, 3));

    Instant shard000ts = Instant.now().minus(Duration.standardHours(1));
    Instant shard001ts = Instant.now().minus(Duration.standardHours(1));

    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                tsCheckpoint("shard-000", shard000ts), tsCheckpoint("shard-001", shard001ts)));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 0, 3))
                .withLastCheckpointSequenceNumber(2),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 11, 3))
                .withLastCheckpointSequenceNumber(13));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeAtTs("shard-000", shard000ts),
            subscribeAtTs("shard-001", shard001ts),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "13"));
  }

  @Test
  public void poolReSubscribesAndSkipsAllRecordsWithAtTimestampGreaterThanRecords()
      throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(11, 3));

    Instant shard000ts = Instant.now().plus(Duration.standardHours(1));
    Instant shard001ts = Instant.now().plus(Duration.standardHours(1));

    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                tsCheckpoint("shard-000", shard000ts), tsCheckpoint("shard-001", shard001ts)));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000").gives().withLastCheckpointSequenceNumber(2),
            ShardAssertion.shard("shard-001").gives().withLastCheckpointSequenceNumber(13));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeAtTs("shard-000", shard000ts),
            subscribeAtTs("shard-001", shard001ts),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "13"));
  }

  @Test
  public void poolReSubscribesAndReadsRecordsAfterCheckPointWithPositiveSubSeqNumber()
      throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(11, 3));

    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                afterCheckpoint("shard-000", "0", 1000L),
                afterCheckpoint("shard-001", "11", Long.MAX_VALUE)));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 1, 2))
                .withLastCheckpointSequenceNumber(2),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 12, 2))
                .withLastCheckpointSequenceNumber(13));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeAtSeqNumber("shard-000", "0"),
            subscribeAtSeqNumber("shard-001", "11"),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "13"));
  }

  /**
   * Potentially can catch issues with back-pressure logic.
   *
   * @throws Exception
   */
  @Test
  public void poolReSubscribesAndReadsManyEvents() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(1);
    kinesis.stubSubscribeToShard("shard-000", eventsWithRecords(18, 300));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(318, 3));

    kinesis.stubSubscribeToShard("shard-001", eventsWithRecords(75, 200));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(275, 3));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 18, 300))
                .withLastCheckpointSequenceNumber(320),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 75, 200))
                .withLastCheckpointSequenceNumber(277));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "317"),
            subscribeAfterSeqNumber("shard-001", "274"),
            subscribeAfterSeqNumber("shard-000", "320"),
            subscribeAfterSeqNumber("shard-001", "277"));
  }

  @Test
  public void handlesAggregatedRecords() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithAggRecords(12, 2));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(13, 1));
    kinesis.stubSubscribeToShard("shard-001", eventWithAggRecords(55, 3));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(56, 1));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generateAggregated("shard-000", 12, 2))
                .withLastCheckpointSequenceNumber(13),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generateAggregated("shard-001", 55, 3))
                .withLastCheckpointSequenceNumber(56));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "12"),
            subscribeAfterSeqNumber("shard-001", "55"),
            subscribeAfterSeqNumber("shard-000", "13"),
            subscribeAfterSeqNumber("shard-001", "56"));
  }

  @Test
  public void doesNotIntroduceDuplicatesWithAggregatedRecordsCheckpoints() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    SubscribeToShardEvent eventWithAggRecords = eventWithAggRecords(12, 6);
    kinesis.stubSubscribeToShard("shard-000", eventWithAggRecords);

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000"));
    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    validateAggregatedRecords("12", 0, 3);
    KinesisReaderCheckpoint checkpoint = pool.getCheckpointMark();
    assertThat(checkpoint.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                STREAM, "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "12", 3L));
    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            // no-op subscriber
            subscribeAfterSeqNumber("shard-000", "12"));
    pool.stop();
    kinesis.close();

    kinesis = new EFOStubbedKinesisAsyncClient(10);
    // simulate re-consuming same content again
    kinesis.stubSubscribeToShard("shard-000", eventWithAggRecords);

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(checkpoint);
    validateAggregatedRecords("12", 4, 5);
    assertThat(waitForRecords(pool, 3).size()).isEqualTo(0);
    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeAtSeqNumber("shard-000", "12"),
            // no-op subscriber
            subscribeAfterSeqNumber("shard-000", "12"));
  }

  private void validateAggregatedRecords(
      String expectedSeqNum, int startSubSeqNum, int endSubSeqNum) throws Exception {
    for (int i = startSubSeqNum; i < endSubSeqNum + 1; i++) {
      KinesisRecord r = waitForRecords(pool, 1).get(0);
      assertThat(r.getSequenceNumber()).isEqualTo(expectedSeqNum);
      assertThat(r.getSubSequenceNumber()).isEqualTo(i);
      assertThat(pool.getCheckpointMark().iterator())
          .containsExactlyInAnyOrder(
              new ShardCheckpoint(
                  STREAM,
                  "shard-000",
                  ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                  expectedSeqNum,
                  (long) i));
    }
  }

  @Test
  public void skipsEntireAggregatedBatch() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    SubscribeToShardEvent eventWithAggRecords = eventWithAggRecords(12, 6);
    kinesis.stubSubscribeToShard("shard-000", eventWithAggRecords);

    KinesisReaderCheckpoint initialCheckpoint =
        new KinesisReaderCheckpoint(
            ImmutableList.of(
                new ShardCheckpoint(
                    STREAM, "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "12", 5L)));
    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    List<KinesisRecord> actualRecords = waitForRecords(pool, 10);
    assertThat(actualRecords.size()).isEqualTo(0);
    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeAtSeqNumber("shard-000", "12"),
            // to no-op subscriber
            subscribeAfterSeqNumber("shard-000", "12"));
  }

  @Test
  public void poolReSubscribesWhenNoRecordsCome() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(31, 3));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(8, 3));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000").gives().withLastCheckpointSequenceNumber(33),
            ShardAssertion.shard("shard-001").gives().withLastCheckpointSequenceNumber(10));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "33"),
            subscribeAfterSeqNumber("shard-001", "10"));
  }

  @Test
  public void poolReSubscribesWhenRecoverableErrorOccurs() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(3))
        .failWith(new ReadTimeoutException());

    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(10, 10));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis
        .stubSubscribeToShard("shard-001", eventWithRecords(3, 5))
        .failWith(SdkClientException.create("this is recoverable", new ReadTimeoutException()));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(8, 8));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis, 1);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 0, 10))
                .withLastCheckpointSequenceNumber(19),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 0, 8))
                .withLastCheckpointSequenceNumber(15));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "2"),
            subscribeAfterSeqNumber("shard-000", "9"),
            subscribeAfterSeqNumber("shard-001", "7"),
            subscribeAfterSeqNumber("shard-000", "19"),
            subscribeAfterSeqNumber("shard-001", "15"));
  }

  @Test
  public void poolReSubscribesWhenManyRecoverableErrorsOccur() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(1);

    for (int i = 0; i < 250; i++) {
      kinesis
          .stubSubscribeToShard("shard-000", eventsWithRecords(i, 1))
          .failWith(new ReadTimeoutException());
    }

    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(250, 3));

    kinesis
        .stubSubscribeToShard("shard-001", eventsWithRecords(333, 250))
        .failWith(SdkClientException.create("this is recoverable", new ReadTimeoutException()));

    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(583, 3));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis, 1);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 0, 250))
                .withLastCheckpointSequenceNumber(252),
            ShardAssertion.shard("shard-001")
                .gives(KinesisRecordView.generate("shard-001", 333, 250))
                .withLastCheckpointSequenceNumber(585));

    assertThat(kinesis.subscribeRequestsSeen().size()).isEqualTo(255);
  }

  @Test
  public void poolReSubscribesFromInitialWhenRecoverableErrorOccursImmediately() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000").failWith(new ReadTimeoutException());
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(550, 3));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(553, 1));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000"));
    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis, 1);
    pool.start(initialCheckpoint);

    PoolAssertion.assertPool(pool)
        .givesCheckPointedRecords(
            ShardAssertion.shard("shard-000")
                .gives(KinesisRecordView.generate("shard-000", 550, 3))
                .withLastCheckpointSequenceNumber(553));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-000"),
            subscribeAfterSeqNumber("shard-000", "552"),
            subscribeAfterSeqNumber("shard-000", "553"));
  }

  @Test
  public void poolFailsWhenNonRecoverableErrorOccurs() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(7));

    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(7, 3))
        .failWith(new RuntimeException("Oh..."));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 10));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 8));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);
    Throwable exception = assertThrows(IOException.class, () -> waitForRecords(pool, 20));
    assertThat(exception.getMessage()).isEqualTo("java.lang.RuntimeException: Oh...");
    assertThat(exception.getCause()).isInstanceOf(RuntimeException.class);

    // Depending on the moment of shard-000 error catch, recorded sequence numbers
    // of events from shard-001 may differ. However, both shards' checkpoints
    // should never be LATEST
    assertThat(pool.getCheckpointMark())
        .contains(
            new ShardCheckpoint(
                STREAM, "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "9", 0L))
        .doesNotContain(
            new ShardCheckpoint(
                STREAM, "shard-001", new StartingPoint(InitialPositionInStream.LATEST)));
  }

  @Test
  public void poolFailsWhenConsumerDoesNotExist() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(3))
        .failWith(
            new CompletionException(
                "Err ...",
                ResourceNotFoundException.builder()
                    .cause(null)
                    .awsErrorDetails(
                        AwsErrorDetails.builder()
                            .serviceName("Kinesis")
                            .errorCode("ResourceNotFoundException")
                            .errorMessage("Consumer consumer-01 not found.")
                            .build())
                    .build()));

    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 10));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 8));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);
    Throwable exception = assertThrows(IOException.class, () -> waitForRecords(pool, 10));
    assertThat(exception.getMessage())
        .isEqualTo("java.util.concurrent.CompletionException: Err ...");
    Throwable cause = exception.getCause().getCause();
    assertThat(cause).isInstanceOf(ResourceNotFoundException.class);
    assertThat(cause.getMessage())
        .isEqualTo(
            "Consumer consumer-01 not found. (Service: Kinesis, Status Code: 0, Request ID: null)");
  }

  @Test
  public void poolHandlesShardUp() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(1, 10));

    kinesis.stubSubscribeToShard(
        "shard-000",
        reShardEvent(ImmutableList.of("shard-000"), ImmutableList.of("shard-002", "shard-003")));
    kinesis.stubSubscribeToShard("shard-002", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventsWithoutRecords(3, 5));

    kinesis.stubSubscribeToShard("shard-003", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-003", eventsWithoutRecords(3, 6));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3, 5));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(1, 8));

    kinesis.stubSubscribeToShard(
        "shard-001", reShardEvent(ImmutableList.of("shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-004", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-004", eventsWithoutRecords(2, 5));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 35);
    assertThat(actualRecords.size()).isEqualTo(34);

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "2"),
            subscribeAfterSeqNumber("shard-000", "9"),
            subscribeAfterSeqNumber("shard-001", "7"),
            subscribeAfterSeqNumber("shard-000", "10"),
            subscribeAfterSeqNumber("shard-001", "8"),
            subscribeTrimHorizon("shard-002"),
            subscribeTrimHorizon("shard-003"),
            subscribeTrimHorizon("shard-004"),
            subscribeAfterSeqNumber("shard-002", "4"),
            subscribeAfterSeqNumber("shard-003", "5"),
            subscribeAfterSeqNumber("shard-004", "4"),
            subscribeAfterSeqNumber("shard-002", "7"),
            subscribeAfterSeqNumber("shard-003", "8"),
            subscribeAfterSeqNumber("shard-004", "6"));

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                STREAM, "shard-002", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "7", 0L),
            new ShardCheckpoint(
                STREAM, "shard-003", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L),
            new ShardCheckpoint(
                STREAM, "shard-004", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "6", 0L));
  }

  @Test
  public void poolHandlesShardUpWithRecords() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(1);
    kinesis.stubSubscribeToShard(
        "shard-000",
        reShardEventWithRecords(
            11, 1, ImmutableList.of("shard-000"), ImmutableList.of("shard-001", "shard-002")));

    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(101, 1));
    kinesis.stubSubscribeToShard("shard-002", eventsWithoutRecords(102, 1));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 1);
    assertThat(actualRecords.size()).isEqualTo(1);

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(STREAM, "shard-001", ShardIteratorType.TRIM_HORIZON, null, null),
            new ShardCheckpoint(STREAM, "shard-002", ShardIteratorType.TRIM_HORIZON, null, null));

    assertThat(waitForRecords(pool, 1).size()).isEqualTo(0);
    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                STREAM, "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "101", 0L),
            new ShardCheckpoint(
                STREAM, "shard-002", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "102", 0L));

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeTrimHorizon("shard-001"),
            subscribeTrimHorizon("shard-002"),
            subscribeAfterSeqNumber("shard-001", "101"),
            subscribeAfterSeqNumber("shard-002", "102"));
  }

  @Test
  public void poolHandlesShardDown() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(1, 10));
    kinesis.stubSubscribeToShard(
        "shard-000",
        reShardEvent(ImmutableList.of("shard-000", "shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(2, 5));

    kinesis.stubSubscribeToShard(
        "shard-001",
        reShardEvent(ImmutableList.of("shard-000", "shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-002", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventsWithoutRecords(2, 5));
    kinesis.stubSubscribeToShard(
        "shard-002",
        reShardEvent(ImmutableList.of("shard-002", "shard-003"), ImmutableList.of("shard-005")));

    kinesis.stubSubscribeToShard("shard-003", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-003", eventsWithoutRecords(2, 5));
    kinesis.stubSubscribeToShard(
        "shard-003",
        reShardEvent(ImmutableList.of("shard-002", "shard-003"), ImmutableList.of("shard-005")));

    kinesis.stubSubscribeToShard("shard-004", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-004", eventsWithoutRecords(3, 6));
    kinesis.stubSubscribeToShard("shard-005", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-005", eventsWithoutRecords(3, 6));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(
            ImmutableList.of("shard-000", "shard-001", "shard-002", "shard-003"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 38);
    assertThat(actualRecords.size()).isEqualTo(37);

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeLatest("shard-002"),
            subscribeLatest("shard-003"),
            subscribeAfterSeqNumber("shard-000", "2"),
            subscribeAfterSeqNumber("shard-001", "4"),
            subscribeAfterSeqNumber("shard-002", "4"),
            subscribeAfterSeqNumber("shard-003", "4"),
            subscribeAfterSeqNumber("shard-000", "9"),
            subscribeAfterSeqNumber("shard-001", "6"),
            subscribeAfterSeqNumber("shard-002", "6"),
            subscribeAfterSeqNumber("shard-003", "6"),
            subscribeAfterSeqNumber("shard-000", "10"),
            subscribeTrimHorizon("shard-004"),
            subscribeTrimHorizon("shard-005"),
            subscribeAfterSeqNumber("shard-004", "5"),
            subscribeAfterSeqNumber("shard-005", "5"),
            subscribeAfterSeqNumber("shard-004", "8"),
            subscribeAfterSeqNumber("shard-005", "8"));

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                STREAM, "shard-005", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L),
            new ShardCheckpoint(
                STREAM, "shard-004", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L));
  }

  @Test
  public void checkpointEqualsToInitStateIfNothingIsConsumed() {
    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(1));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(1));

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);
    ShardCheckpoint[] expectedShardsCheckpoints = {
      new ShardCheckpoint(STREAM, "shard-000", new StartingPoint(InitialPositionInStream.LATEST)),
      new ShardCheckpoint(STREAM, "shard-001", new StartingPoint(InitialPositionInStream.LATEST))
    };
    assertThat(initialCheckpoint.iterator()).containsExactlyInAnyOrder(expectedShardsCheckpoints);
    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(expectedShardsCheckpoints);
  }

  @Test
  public void poolWatermarkReturnsTsOfOldestAcknowledgedRecord() throws Exception {
    List<Record> shard000records0 = recordWithMinutesAgo(5);
    List<Record> shard001records0 = recordWithMinutesAgo(4);
    List<Record> shard000records1 = recordWithMinutesAgo(3);

    kinesis = new EFOStubbedKinesisAsyncClient(10);
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(shard000records0));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(shard001records0));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(shard000records1));

    Instant ts0 = TimeUtil.toJoda(shard000records0.get(0).approximateArrivalTimestamp());
    Instant ts1 = TimeUtil.toJoda(shard001records0.get(0).approximateArrivalTimestamp());

    KinesisReaderCheckpoint initialCheckpoint =
        initialLatestCheckpoint(ImmutableList.of("shard-000", "shard-001"));

    pool = new EFOShardSubscribersPool(readSpec, consumerArn, kinesis);
    pool.start(initialCheckpoint);

    // nothing was ack-ed yet
    assertThat(pool.getWatermark()).isEqualTo(BoundedWindow.TIMESTAMP_MIN_VALUE);

    // one of the shards had nothing ack-ed yet
    assertThat(waitForRecords(pool, 1).size()).isEqualTo(1);
    assertThat(pool.getWatermark()).isEqualTo(BoundedWindow.TIMESTAMP_MIN_VALUE);

    assertThat(waitForRecords(pool, 1).size()).isEqualTo(1);
    assertThat(pool.getWatermark()).isEqualTo(ts0);

    assertThat(waitForRecords(pool, 1).size()).isEqualTo(1);
    assertThat(pool.getWatermark()).isEqualTo(ts1);
  }

  static List<KinesisRecord> waitForRecords(EFOShardSubscribersPool pool, int recordsToWaitFor)
      throws Exception {
    List<KinesisRecord> records = new ArrayList<>(recordsToWaitFor);
    int attempts = 0; // max attempts per record
    while (records.size() < recordsToWaitFor && attempts < 5) {
      KinesisRecord r = pool.getNextRecord();
      if (r != null) {
        records.add(r);
        attempts = 0;
      } else {
        attempts++;
        Thread.sleep(50);
      }
    }
    return records;
  }

  private KinesisReaderCheckpoint initialLatestCheckpoint(List<String> shardIds) {
    List<ShardCheckpoint> shardCheckpoints = new ArrayList<>();
    for (String shardId : shardIds) {
      shardCheckpoints.add(
          new ShardCheckpoint(STREAM, shardId, new StartingPoint(InitialPositionInStream.LATEST)));
    }
    return new KinesisReaderCheckpoint(shardCheckpoints);
  }

  private ShardCheckpoint afterCheckpoint(String shardId, String seqNum) {
    return afterCheckpoint(shardId, seqNum, 0L);
  }

  private ShardCheckpoint afterCheckpoint(String shardId, String seqNum, long subSeqNum) {
    return new ShardCheckpoint(
        STREAM, shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER, seqNum, subSeqNum);
  }

  private ShardCheckpoint trimHorizonCheckpoint(String shardId) {
    return new ShardCheckpoint(STREAM, shardId, ShardIteratorType.TRIM_HORIZON, null);
  }

  private ShardCheckpoint tsCheckpoint(String shardId, Instant ts) {
    return new ShardCheckpoint(STREAM, shardId, ShardIteratorType.AT_TIMESTAMP, ts);
  }

  private static class KinesisRecordView {
    private final String shardId;
    private final String sequenceNumber;
    private final long subSequenceNumber;

    KinesisRecordView(String shardId, String sequenceNumber, long subSequenceNumber) {
      this.shardId = shardId;
      this.sequenceNumber = sequenceNumber;
      this.subSequenceNumber = subSequenceNumber;
    }

    static KinesisRecordView fromKinesisRecord(KinesisRecord r) {
      return new KinesisRecordView(r.getShardId(), r.getSequenceNumber(), r.getSubSequenceNumber());
    }

    static KinesisRecordView[] generate(String shardId, int startingSeqNum, int cnt) {
      List<KinesisRecordView> result = new ArrayList<>();
      for (int i = startingSeqNum; i < startingSeqNum + cnt; i++) {
        result.add(new KinesisRecordView(shardId, String.valueOf(i), 0L));
      }
      return result.toArray(new KinesisRecordView[0]);
    }

    static KinesisRecordView[] generateAggregated(String shardId, int seqNum, int cnt) {
      List<KinesisRecordView> result = new ArrayList<>();
      for (long i = 0L; i < cnt; i++) {
        result.add(new KinesisRecordView(shardId, String.valueOf(seqNum), i));
      }
      return result.toArray(new KinesisRecordView[0]);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KinesisRecordView that = (KinesisRecordView) o;
      return subSequenceNumber == that.subSequenceNumber
          && shardId.equals(that.shardId)
          && sequenceNumber.equals(that.sequenceNumber);
    }

    @Override
    public int hashCode() {
      return Objects.hash(shardId, sequenceNumber, subSequenceNumber);
    }

    @Override
    public String toString() {
      return "KinesisRecordView{"
          + "shardId='"
          + shardId
          + '\''
          + ", sequenceNumber='"
          + sequenceNumber
          + '\''
          + ", subSequenceNumber="
          + subSequenceNumber
          + '}';
    }
  }

  private static void nothingElseShouldBeReceived(EFOShardSubscribersPool pool) throws Exception {
    assertThat(waitForRecords(pool, 5)).hasSize(0);
  }

  private static class PoolAssertion {
    private final EFOShardSubscribersPool pool;

    PoolAssertion(EFOShardSubscribersPool pool) {
      this.pool = pool;
    }

    static PoolAssertion assertPool(EFOShardSubscribersPool pool) {
      return new PoolAssertion(pool);
    }

    /**
     * Validates records' shard ID, sequence number and sub-sequence number.
     *
     * <p>Fetches records from the pool one-by-one and validates how the checkpoint advances.
     */
    void givesCheckPointedRecords(ShardAssertion... shardAssertions) throws Exception {
      List<KinesisRecordView> allExpectedRecords = new ArrayList<>();
      List<ShardCheckpoint> allExpectedFinalCheckPoints = new ArrayList<>();

      for (ShardAssertion sa : shardAssertions) {
        allExpectedRecords.addAll(sa.expectedRecords);
        if (sa.expectedRecords.size() > 0) {
          KinesisRecordView lastRecordView = sa.expectedRecords.get(sa.expectedRecords.size() - 1);
          allExpectedFinalCheckPoints.add(
              new ShardCheckpoint(
                  STREAM,
                  sa.shardId,
                  ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                  sa.expectedLastSequenceNumber,
                  lastRecordView.subSequenceNumber));
        } else {
          allExpectedFinalCheckPoints.add(
              new ShardCheckpoint(
                  STREAM,
                  sa.shardId,
                  ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                  sa.expectedLastSequenceNumber,
                  0L));
        }
      }

      List<KinesisRecordView> actualRecords = new ArrayList<>();

      for (int i = 0; i < allExpectedRecords.size(); i++) {
        List<KinesisRecord> kinesisRecords = waitForRecords(this.pool, 1);
        if (kinesisRecords.size() == 0) {
          String msg = String.format("Unable to fetch %s th record", i);
          throw new RuntimeException(msg);
        }
        KinesisRecord kinesisRecord = kinesisRecords.get(0);
        actualRecords.add(KinesisRecordView.fromKinesisRecord(kinesisRecord));
        KinesisReaderCheckpoint checkpoint = pool.getCheckpointMark();

        assertThat(checkpoint)
            .contains(
                new ShardCheckpoint(
                    STREAM,
                    kinesisRecord.getShardId(),
                    ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                    kinesisRecord.getSequenceNumber(),
                    kinesisRecord.getSubSequenceNumber()));
      }

      nothingElseShouldBeReceived(pool);

      assertThat(actualRecords)
          .containsExactlyInAnyOrder(allExpectedRecords.toArray(new KinesisRecordView[0]));
      assertThat(pool.getCheckpointMark())
          .containsExactlyInAnyOrder(allExpectedFinalCheckPoints.toArray(new ShardCheckpoint[0]));
    }
  }

  private static class ShardAssertion {
    final String shardId;
    final List<KinesisRecordView> expectedRecords = new ArrayList<>();
    String expectedLastSequenceNumber;

    ShardAssertion(String shardId) {
      this.shardId = shardId;
    }

    static ShardAssertion shard(String shardId) {
      return new ShardAssertion(shardId);
    }

    ShardAssertion gives(KinesisRecordView... records) {
      this.expectedRecords.addAll(Arrays.asList(records));
      return this;
    }

    ShardAssertion withLastCheckpointSequenceNumber(int expectedLastSequenceNumber) {
      this.expectedLastSequenceNumber = String.valueOf(expectedLastSequenceNumber);
      return this;
    }
  }
}
