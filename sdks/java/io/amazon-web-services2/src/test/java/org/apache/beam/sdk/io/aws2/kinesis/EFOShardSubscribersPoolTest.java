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

import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertTrue;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.createReadSpec;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.eventsWithoutRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.listLatest;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.subscribeAfterSeqNumber;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.subscribeLatest;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOHelpers.subscribeTrimHorizon;
import static org.apache.beam.sdk.io.aws2.kinesis.EFORecordsGenerators.eventWithAggRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.EFORecordsGenerators.eventWithRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.EFORecordsGenerators.eventsWithRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.EFORecordsGenerators.reshardEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.netty.handler.timeout.ReadTimeoutException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.common.InitialPositionInStream;

public class EFOShardSubscribersPoolTest {
  private KinesisIO.Read readSpec;
  private EFOStubbedKinesisAsyncClient kinesis;
  private EFOShardSubscribersPool pool;

  @Before
  public void setUp() {
    readSpec = createReadSpec();
  }

  @After
  public void tearDown() {
    kinesis.close();
    pool.stop();
  }

  @Test
  public void poolReSubscribesAndReadsRecords() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 10));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3, 5));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 8));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 18);
    validateRecords(actualRecords);
    assertThat(waitForRecords(pool, 3).size()).isEqualTo(0); // nothing more is received

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());

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

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "12", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "10", 0L));
  }

  /**
   * Potentially can catch issues with back-pressure logic.
   *
   * @throws Exception
   */
  @Test
  public void poolReSubscribesAndReadsManyEvents() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(1, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventsWithRecords(18, 300));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 318));

    kinesis.stubSubscribeToShard("shard-001", eventsWithRecords(75, 200));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 275));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    assertThat(waitForRecords(pool, 500).size()).isEqualTo(500);
    assertThat(waitForRecords(pool, 100).size()).isEqualTo(0); // nothing more is received

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "317"),
            subscribeAfterSeqNumber("shard-001", "274"),
            subscribeAfterSeqNumber("shard-000", "320"),
            subscribeAfterSeqNumber("shard-001", "277"));

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "320", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "277", 0L));
  }

  @Test
  public void handlesAggregatedRecords() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventWithAggRecords(12, 2));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(1, 13));
    kinesis.stubSubscribeToShard("shard-001", eventWithAggRecords(55, 3));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(1, 56));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 5);
    assertThat(waitForRecords(pool, 10).size()).isEqualTo(0); // nothing more is received

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());
    assertThat(actualRecords.size()).isEqualTo(5);

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "12"),
            subscribeAfterSeqNumber("shard-001", "55"),
            subscribeAfterSeqNumber("shard-000", "13"),
            subscribeAfterSeqNumber("shard-001", "56"));

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "13", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "56", 0L));
  }

  @Test
  public void doesNotIntroduceDuplicatesWithAggregatedRecordsCheckpoints() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000"));
    kinesis.stubSubscribeToShard("shard-000", eventWithAggRecords(12, 6));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 4);
    validateRecords(
        actualRecords, 4, new String[] {"12", "12", "12", "12"}, new Long[] {0L, 1L, 2L, 3L});
    KinesisReaderCheckpoint checkpoint = pool.getCheckpointMark();
    assertThat(checkpoint.iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AT_SEQUENCE_NUMBER, "12", 3L));
    pool.stop();
    kinesis.close();

    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000"));
    // simulate re-consuming same content again
    kinesis.stubSubscribeToShard("shard-000", eventWithAggRecords(12, 6));

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(checkpoint);
    List<KinesisRecord> recordsAfterReStart = waitForRecords(pool, 3);

    // FIXME: Check ShardCheckpoint#isBeforeOrAt() semantic!
    validateRecords(
        recordsAfterReStart,
        3,
        new String[] {"12", "12", "12"},
        new Long[] {3L, 4L, 5L} // 0L, 1L, 2L, 3L were consumed and checkpoint-ed before
        );
    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AT_SEQUENCE_NUMBER, "12", 5L));

    assertThat(waitForRecords(pool, 3).size()).isEqualTo(0);
    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "12", 0L));
  }

  @Test
  public void poolReSubscribesWhenNoRecordsCome() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 31));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 8));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 1);
    assertEquals(0, actualRecords.size());

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());
    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-001"),
            subscribeAfterSeqNumber("shard-000", "33"),
            subscribeAfterSeqNumber("shard-001", "10"));

    List<ShardCheckpoint> expectedCheckPoint =
        ImmutableList.of(
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "10", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "33", 0L));
    KinesisReaderCheckpoint actualCheckPoint = pool.getCheckpointMark();
    assertEquals(expectedCheckPoint, ImmutableList.copyOf(actualCheckPoint.iterator()));
  }

  @Test
  public void poolReSubscribesWhenRecoverableErrorOccurs() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(3))
        .failWith(new ReadTimeoutException());

    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 10));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis
        .stubSubscribeToShard("shard-001", eventWithRecords(3, 5))
        .failWith(SdkClientException.create("this is recoverable", new ReadTimeoutException()));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 8));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis, 1);
    pool.start(initialCheckpoint);

    assertThat(waitForRecords(pool, 25)).hasSize(18);
    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());

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

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "12", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "10", 0L));

    assertThat(waitForRecords(pool, 1)).isEmpty(); // no more records

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "12", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "10", 0L));
  }

  @Test
  public void poolReSubscribesWhenManyRecoverableErrorsOccur() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(1, ImmutableList.of("shard-000", "shard-001"));

    for (int i = 0; i < 250; i++) {
      kinesis
          .stubSubscribeToShard("shard-000", eventsWithRecords(i, 1))
          .failWith(new ReadTimeoutException());
    }

    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 250));

    kinesis
        .stubSubscribeToShard("shard-001", eventsWithRecords(333, 250))
        .failWith(SdkClientException.create("this is recoverable", new ReadTimeoutException()));

    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 583));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis, 1);
    pool.start(initialCheckpoint);

    assertThat(waitForRecords(pool, 500)).hasSize(500);
    assertThat(waitForRecords(pool, 50)).hasSize(0); // nothing else comes
    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());
    assertThat(kinesis.subscribeRequestsSeen().size()).isEqualTo(255);

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "252", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "585", 0L));
  }

  @Test
  public void poolReSubscribesFromInitialWhenRecoverableErrorOccursImmediately() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000"));
    kinesis.stubSubscribeToShard("shard-000").failWith(new ReadTimeoutException());
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(550, 3));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(1, 553));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis, 1);
    pool.start(initialCheckpoint);

    assertThat(waitForRecords(pool, 3)).hasSize(3);
    assertThat(waitForRecords(pool, 1)).isEmpty(); // no more records
    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());

    assertThat(kinesis.subscribeRequestsSeen())
        .containsExactlyInAnyOrder(
            subscribeLatest("shard-000"),
            subscribeLatest("shard-000"),
            subscribeAfterSeqNumber("shard-000", "552"),
            subscribeAfterSeqNumber("shard-000", "553"));

    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "553", 0L));
  }

  @Test
  public void poolFailsWhenNonRecoverableErrorOccurs() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(7));

    kinesis
        .stubSubscribeToShard("shard-000", eventWithRecords(7, 3))
        .failWith(new RuntimeException("Oh..."));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(3, 10));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(3, 8));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    Throwable exception = assertThrows(IOException.class, () -> waitForRecords(pool, 20));
    assertEquals("java.lang.RuntimeException: Oh...", exception.getMessage());
    assertTrue(exception.getCause() instanceof RuntimeException);

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());

    // Depending on the moment of shard-000 error catch, recorded sequence numbers
    // of events from shard-001 may differ. However, both shards' checkpoints
    // should never be LATEST
    assertThat(pool.getCheckpointMark())
        .contains(
            new ShardCheckpoint(
                "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "9", 0L))
        .doesNotContain(
            new ShardCheckpoint(
                "stream-01", "shard-001", new StartingPoint(InitialPositionInStream.LATEST)));
  }

  @Test
  public void poolFailsWhenConsumerDoesNotExist() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
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
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    Throwable exception = assertThrows(IOException.class, () -> waitForRecords(pool, 10));
    assertEquals("java.util.concurrent.CompletionException: Err ...", exception.getMessage());
    Throwable cause = exception.getCause().getCause();
    assertTrue(cause instanceof ResourceNotFoundException);
    assertEquals(
        "Consumer consumer-01 not found. (Service: Kinesis, Status Code: 0, Request ID: null)",
        cause.getMessage());

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());
  }

  @Test
  public void poolHandlesShardUp() throws Exception {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(1, 10));

    kinesis.stubSubscribeToShard(
        "shard-000",
        reshardEvent(ImmutableList.of("shard-000"), ImmutableList.of("shard-002", "shard-003")));
    kinesis.stubSubscribeToShard("shard-002", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventsWithoutRecords(3, 5));

    kinesis.stubSubscribeToShard("shard-003", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-003", eventsWithoutRecords(3, 6));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(3, 5));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(1, 8));

    kinesis.stubSubscribeToShard(
        "shard-001", reshardEvent(ImmutableList.of("shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-004", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-004", eventsWithoutRecords(2, 5));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 35);
    assertEquals(34, actualRecords.size());

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());
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
                "stream-01", "shard-002", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "7", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-003", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-004", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "6", 0L));
  }

  @Test
  public void poolHandlesShardDown() throws Exception {
    kinesis =
        new EFOStubbedKinesisAsyncClient(
            10, ImmutableList.of("shard-000", "shard-001", "shard-002", "shard-003"));

    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(3, 7));
    kinesis.stubSubscribeToShard("shard-000", eventsWithoutRecords(1, 10));
    kinesis.stubSubscribeToShard(
        "shard-000",
        reshardEvent(ImmutableList.of("shard-000", "shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-001", eventsWithoutRecords(2, 5));

    kinesis.stubSubscribeToShard(
        "shard-001",
        reshardEvent(ImmutableList.of("shard-000", "shard-001"), ImmutableList.of("shard-004")));

    kinesis.stubSubscribeToShard("shard-002", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-002", eventsWithoutRecords(2, 5));
    kinesis.stubSubscribeToShard(
        "shard-002",
        reshardEvent(ImmutableList.of("shard-002", "shard-003"), ImmutableList.of("shard-005")));

    kinesis.stubSubscribeToShard("shard-003", eventWithRecords(5));
    kinesis.stubSubscribeToShard("shard-003", eventsWithoutRecords(2, 5));
    kinesis.stubSubscribeToShard(
        "shard-003",
        reshardEvent(ImmutableList.of("shard-002", "shard-003"), ImmutableList.of("shard-005")));

    kinesis.stubSubscribeToShard("shard-004", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-004", eventsWithoutRecords(3, 6));
    kinesis.stubSubscribeToShard("shard-005", eventWithRecords(6));
    kinesis.stubSubscribeToShard("shard-005", eventsWithoutRecords(3, 6));

    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 38);
    assertEquals(37, actualRecords.size());

    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());
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
                "stream-01", "shard-005", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L),
            new ShardCheckpoint(
                "stream-01", "shard-004", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "8", 0L));
  }

  @Test
  public void checkpointEqualsToInitStateIfNothingIsConsumed() throws TransientKinesisException {
    kinesis = new EFOStubbedKinesisAsyncClient(10, ImmutableList.of("shard-000", "shard-001"));
    kinesis.stubSubscribeToShard("shard-000", eventWithRecords(1));
    kinesis.stubSubscribeToShard("shard-001", eventWithRecords(1));
    KinesisReaderCheckpoint initialCheckpoint =
        new EFOFromScratchCheckpointGenerator(readSpec).generate(kinesis);

    pool = new EFOShardSubscribersPool(readSpec, kinesis);
    pool.start(initialCheckpoint);
    ShardCheckpoint[] expectedShardsCheckpoints = {
      new ShardCheckpoint(
          "stream-01", "shard-000", new StartingPoint(InitialPositionInStream.LATEST)),
      new ShardCheckpoint(
          "stream-01", "shard-001", new StartingPoint(InitialPositionInStream.LATEST))
    };
    assertThat(initialCheckpoint.iterator()).containsExactlyInAnyOrder(expectedShardsCheckpoints);
    assertThat(pool.getCheckpointMark().iterator())
        .containsExactlyInAnyOrder(expectedShardsCheckpoints);
    assertThat(kinesis.listRequestsSeen()).containsExactlyInAnyOrder(listLatest());
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

  private void validateRecords(List<KinesisRecord> records) {
    List<RecordDataToCheck> internalRecords =
        records.stream().map(RecordDataToCheck::fromKinesisRecord).collect(Collectors.toList());

    assertThat(internalRecords)
        .hasSize(18)
        .containsExactlyInAnyOrder(
            new RecordDataToCheck("shard-000", "0"),
            new RecordDataToCheck("shard-000", "1"),
            new RecordDataToCheck("shard-000", "2"),
            new RecordDataToCheck("shard-000", "3"),
            new RecordDataToCheck("shard-000", "4"),
            new RecordDataToCheck("shard-000", "5"),
            new RecordDataToCheck("shard-000", "6"),
            new RecordDataToCheck("shard-000", "7"),
            new RecordDataToCheck("shard-000", "8"),
            new RecordDataToCheck("shard-000", "9"),
            new RecordDataToCheck("shard-001", "0"),
            new RecordDataToCheck("shard-001", "1"),
            new RecordDataToCheck("shard-001", "2"),
            new RecordDataToCheck("shard-001", "3"),
            new RecordDataToCheck("shard-001", "4"),
            new RecordDataToCheck("shard-001", "5"),
            new RecordDataToCheck("shard-001", "6"),
            new RecordDataToCheck("shard-001", "7"));
  }

  private void validateRecords(
      List<KinesisRecord> records,
      int expectedCnt,
      String[] expectedSequenceNumbers,
      Long[] expectedSubSequenceNumbers) {

    List<String> sequenceNumbers =
        records.stream().map(KinesisRecord::getSequenceNumber).collect(Collectors.toList());
    List<Long> subSequenceNumbers =
        records.stream().map(KinesisRecord::getSubSequenceNumber).collect(Collectors.toList());

    assertThat(sequenceNumbers)
        .hasSize(expectedCnt)
        .containsExactlyInAnyOrder(expectedSequenceNumbers);
    assertThat(subSequenceNumbers)
        .hasSize(expectedCnt)
        .containsExactlyInAnyOrder(expectedSubSequenceNumbers);
  }

  private static ByteBuffer fromStr(String str) {
    return ByteBuffer.wrap(str.getBytes(UTF_8));
  }

  private static class RecordDataToCheck {
    private final String shardId;
    private final String sequenceNumber;
    private final long subSequenceNumber;
    private final ByteBuffer data;

    static RecordDataToCheck fromKinesisRecord(KinesisRecord kinesisRecord) {
      return new RecordDataToCheck(
          kinesisRecord.getShardId(),
          kinesisRecord.getSequenceNumber(),
          kinesisRecord.getSubSequenceNumber(),
          kinesisRecord.getData());
    }

    public RecordDataToCheck(
        String shardId, String sequenceNumber, long subSequenceNumber, ByteBuffer data) {
      this.shardId = shardId;
      this.sequenceNumber = sequenceNumber;
      this.subSequenceNumber = subSequenceNumber;
      this.data = data;
    }

    public RecordDataToCheck(String shardId, String sequenceNumber, ByteBuffer data) {
      this.shardId = shardId;
      this.sequenceNumber = sequenceNumber;
      this.subSequenceNumber = 0L;
      this.data = data;
    }

    public RecordDataToCheck(String shardId, String sequenceNumber) {
      this.shardId = shardId;
      this.sequenceNumber = sequenceNumber;
      this.subSequenceNumber = 0L;
      this.data = fromStr(sequenceNumber);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RecordDataToCheck that = (RecordDataToCheck) o;
      return subSequenceNumber == that.subSequenceNumber
          && shardId.equals(that.shardId)
          && sequenceNumber.equals(that.sequenceNumber)
          && data.equals(that.data);
    }

    @Override
    public int hashCode() {
      return Objects.hash(shardId, sequenceNumber, subSequenceNumber, data);
    }

    @Override
    public String toString() {
      return "{"
          + "shardId='"
          + shardId
          + '\''
          + ", sequenceNumber='"
          + sequenceNumber
          + '\''
          + ", subSequenceNumber="
          + subSequenceNumber
          + ", data=<...>"
          + '}';
    }
  }
}
