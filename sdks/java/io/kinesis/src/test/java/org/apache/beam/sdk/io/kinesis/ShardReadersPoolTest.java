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
package org.apache.beam.sdk.io.kinesis;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

/** Tests {@link ShardReadersPool}. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class ShardReadersPoolTest {

  private static final int TIMEOUT_IN_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);

  @Mock private ShardRecordsIterator firstIterator, secondIterator, thirdIterator, fourthIterator;
  @Mock private ShardCheckpoint firstCheckpoint, secondCheckpoint;
  @Mock private SimplifiedKinesisClient kinesis;
  @Mock private KinesisRecord a, b, c, d;
  @Mock private WatermarkPolicyFactory watermarkPolicyFactory;
  @Mock private RateLimitPolicyFactory rateLimitPolicyFactory;
  @Mock private RateLimitPolicy customRateLimitPolicy;

  private ShardReadersPool shardReadersPool;
  private final Instant now = Instant.now();

  @Before
  public void setUp() throws TransientKinesisException {
    when(a.getShardId()).thenReturn("shard1");
    when(b.getShardId()).thenReturn("shard1");
    when(c.getShardId()).thenReturn("shard2");
    when(d.getShardId()).thenReturn("shard2");
    when(firstCheckpoint.getShardId()).thenReturn("shard1");
    when(secondCheckpoint.getShardId()).thenReturn("shard2");
    when(firstIterator.getShardId()).thenReturn("shard1");
    when(firstIterator.getCheckpoint()).thenReturn(firstCheckpoint);
    when(secondIterator.getShardId()).thenReturn("shard2");
    when(secondIterator.getCheckpoint()).thenReturn(secondCheckpoint);
    when(thirdIterator.getShardId()).thenReturn("shard3");
    when(fourthIterator.getShardId()).thenReturn("shard4");

    WatermarkPolicy watermarkPolicy =
        WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();
    RateLimitPolicy rateLimitPolicy = RateLimitPolicyFactory.withoutLimiter().getRateLimitPolicy();

    KinesisReaderCheckpoint checkpoint =
        new KinesisReaderCheckpoint(ImmutableList.of(firstCheckpoint, secondCheckpoint));
    shardReadersPool =
        Mockito.spy(
            new ShardReadersPool(
                kinesis, checkpoint, watermarkPolicyFactory, rateLimitPolicyFactory, 100));

    when(watermarkPolicyFactory.createWatermarkPolicy()).thenReturn(watermarkPolicy);
    when(rateLimitPolicyFactory.getRateLimitPolicy()).thenReturn(rateLimitPolicy);

    doReturn(firstIterator).when(shardReadersPool).createShardIterator(kinesis, firstCheckpoint);
    doReturn(secondIterator).when(shardReadersPool).createShardIterator(kinesis, secondCheckpoint);
  }

  @After
  public void clean() {
    shardReadersPool.stop();
  }

  @Test
  public void shouldReturnAllRecords()
      throws TransientKinesisException, KinesisShardClosedException {
    when(firstIterator.readNextBatch())
        .thenReturn(Collections.emptyList())
        .thenReturn(ImmutableList.of(a, b))
        .thenReturn(Collections.emptyList());
    when(secondIterator.readNextBatch())
        .thenReturn(singletonList(c))
        .thenReturn(singletonList(d))
        .thenReturn(Collections.emptyList());

    shardReadersPool.start();
    List<KinesisRecord> fetchedRecords = new ArrayList<>();
    while (fetchedRecords.size() < 4) {
      CustomOptional<KinesisRecord> nextRecord = shardReadersPool.nextRecord();
      if (nextRecord.isPresent()) {
        fetchedRecords.add(nextRecord.get());
      }
    }
    assertThat(fetchedRecords).containsExactlyInAnyOrder(a, b, c, d);
    assertThat(shardReadersPool.getRecordsQueue().remainingCapacity()).isEqualTo(100 * 2);
  }

  @Test
  public void shouldReturnAbsentOptionalWhenNoRecords()
      throws TransientKinesisException, KinesisShardClosedException {
    when(firstIterator.readNextBatch()).thenReturn(Collections.emptyList());
    when(secondIterator.readNextBatch()).thenReturn(Collections.emptyList());

    shardReadersPool.start();
    CustomOptional<KinesisRecord> nextRecord = shardReadersPool.nextRecord();
    assertThat(nextRecord.isPresent()).isFalse();
  }

  @Test
  public void shouldCheckpointReadRecords()
      throws TransientKinesisException, KinesisShardClosedException {
    when(firstIterator.readNextBatch())
        .thenReturn(ImmutableList.of(a, b))
        .thenReturn(Collections.emptyList());
    when(secondIterator.readNextBatch())
        .thenReturn(singletonList(c))
        .thenReturn(singletonList(d))
        .thenReturn(Collections.emptyList());

    shardReadersPool.start();
    int recordsFound = 0;
    while (recordsFound < 4) {
      CustomOptional<KinesisRecord> nextRecord = shardReadersPool.nextRecord();
      if (nextRecord.isPresent()) {
        recordsFound++;
        KinesisRecord kinesisRecord = nextRecord.get();
        if ("shard1".equals(kinesisRecord.getShardId())) {
          verify(firstIterator).ackRecord(kinesisRecord);
        } else {
          verify(secondIterator).ackRecord(kinesisRecord);
        }
      }
    }
  }

  @Test
  public void shouldInterruptKinesisReadingAndStopShortly()
      throws TransientKinesisException, KinesisShardClosedException {
    when(firstIterator.readNextBatch())
        .thenAnswer(
            (Answer<List<KinesisRecord>>)
                invocation -> {
                  Thread.sleep(TIMEOUT_IN_MILLIS / 2);
                  return Collections.emptyList();
                });
    shardReadersPool.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    shardReadersPool.stop();
    assertThat(stopwatch.elapsed(TimeUnit.MILLISECONDS)).isLessThan(TIMEOUT_IN_MILLIS);
  }

  @Test
  public void shouldInterruptPuttingRecordsToQueueAndStopShortly()
      throws TransientKinesisException, KinesisShardClosedException {
    when(firstIterator.readNextBatch()).thenReturn(ImmutableList.of(a, b, c));
    KinesisReaderCheckpoint checkpoint =
        new KinesisReaderCheckpoint(ImmutableList.of(firstCheckpoint, secondCheckpoint));

    WatermarkPolicyFactory watermarkPolicyFactory = WatermarkPolicyFactory.withArrivalTimePolicy();
    RateLimitPolicyFactory rateLimitPolicyFactory = RateLimitPolicyFactory.withoutLimiter();
    ShardReadersPool shardReadersPool =
        new ShardReadersPool(
            kinesis, checkpoint, watermarkPolicyFactory, rateLimitPolicyFactory, 2);
    shardReadersPool.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    shardReadersPool.stop();
    assertThat(stopwatch.elapsed(TimeUnit.MILLISECONDS)).isLessThan(TIMEOUT_IN_MILLIS);
  }

  @Test
  public void shouldStopReadingShardAfterReceivingShardClosedException() throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    when(firstIterator.findSuccessiveShardRecordIterators()).thenReturn(Collections.emptyList());

    shardReadersPool.start();

    verify(firstIterator, timeout(TIMEOUT_IN_MILLIS).times(1)).readNextBatch();
    verify(secondIterator, timeout(TIMEOUT_IN_MILLIS).atLeast(2)).readNextBatch();
  }

  @Test
  public void shouldStartReadingSuccessiveShardsAfterReceivingShardClosedException()
      throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    when(firstIterator.findSuccessiveShardRecordIterators())
        .thenReturn(ImmutableList.of(thirdIterator, fourthIterator));

    shardReadersPool.start();

    verify(thirdIterator, timeout(TIMEOUT_IN_MILLIS).atLeast(2)).readNextBatch();
    verify(fourthIterator, timeout(TIMEOUT_IN_MILLIS).atLeast(2)).readNextBatch();
  }

  @Test
  public void shouldStopReadersPoolWhenLastShardReaderStopped() throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    when(firstIterator.findSuccessiveShardRecordIterators()).thenReturn(Collections.emptyList());

    shardReadersPool.start();

    verify(firstIterator, timeout(TIMEOUT_IN_MILLIS).times(1)).readNextBatch();
  }

  @Test
  public void shouldStopReadersPoolAlsoWhenExceptionsOccurDuringStopping() throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    when(firstIterator.findSuccessiveShardRecordIterators())
        .thenThrow(TransientKinesisException.class)
        .thenReturn(Collections.emptyList());

    shardReadersPool.start();

    verify(firstIterator, timeout(TIMEOUT_IN_MILLIS).times(2)).readNextBatch();
  }

  @Test
  public void shouldReturnAbsentOptionalWhenStartedWithNoIterators() throws Exception {
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(Collections.emptyList());
    WatermarkPolicyFactory watermarkPolicyFactory = WatermarkPolicyFactory.withArrivalTimePolicy();
    RateLimitPolicyFactory rateLimitPolicyFactory = RateLimitPolicyFactory.withoutLimiter();
    shardReadersPool =
        Mockito.spy(
            new ShardReadersPool(
                kinesis,
                checkpoint,
                watermarkPolicyFactory,
                rateLimitPolicyFactory,
                ShardReadersPool.DEFAULT_CAPACITY_PER_SHARD));
    doReturn(firstIterator)
        .when(shardReadersPool)
        .createShardIterator(eq(kinesis), any(ShardCheckpoint.class));

    shardReadersPool.start();

    assertThat(shardReadersPool.nextRecord()).isEqualTo(CustomOptional.absent());
  }

  @Test
  public void shouldForgetClosedShardIterator() throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    List<ShardRecordsIterator> emptyList = Collections.emptyList();
    when(firstIterator.findSuccessiveShardRecordIterators()).thenReturn(emptyList);

    shardReadersPool.start();
    verify(shardReadersPool).startReadingShards(ImmutableList.of(firstIterator, secondIterator));
    verify(shardReadersPool, timeout(TIMEOUT_IN_MILLIS)).startReadingShards(emptyList);

    KinesisReaderCheckpoint checkpointMark = shardReadersPool.getCheckpointMark();
    assertThat(checkpointMark.iterator())
        .extracting("shardId", String.class)
        .containsOnly("shard2")
        .doesNotContain("shard1");
  }

  @Test
  public void shouldReturnTheLeastWatermarkOfAllShards() throws TransientKinesisException {
    Instant threeMin = now.minus(Duration.standardMinutes(3));
    Instant twoMin = now.minus(Duration.standardMinutes(2));

    when(firstIterator.getShardWatermark()).thenReturn(threeMin).thenReturn(now);
    when(secondIterator.getShardWatermark()).thenReturn(twoMin);

    shardReadersPool.start();

    assertThat(shardReadersPool.getWatermark()).isEqualTo(threeMin);
    assertThat(shardReadersPool.getWatermark()).isEqualTo(twoMin);

    verify(firstIterator, times(2)).getShardWatermark();
    verify(secondIterator, times(2)).getShardWatermark();
  }

  @Test
  public void shouldReturnTheOldestFromLatestRecordTimestampOfAllShards()
      throws TransientKinesisException {
    Instant threeMin = now.minus(Duration.standardMinutes(3));
    Instant twoMin = now.minus(Duration.standardMinutes(2));

    when(firstIterator.getLatestRecordTimestamp()).thenReturn(threeMin).thenReturn(now);
    when(secondIterator.getLatestRecordTimestamp()).thenReturn(twoMin);

    shardReadersPool.start();

    assertThat(shardReadersPool.getLatestRecordTimestamp()).isEqualTo(threeMin);
    assertThat(shardReadersPool.getLatestRecordTimestamp()).isEqualTo(twoMin);

    verify(firstIterator, times(2)).getLatestRecordTimestamp();
    verify(secondIterator, times(2)).getLatestRecordTimestamp();
  }

  @Test
  public void shouldCallRateLimitPolicy()
      throws TransientKinesisException, KinesisShardClosedException, InterruptedException {
    KinesisClientThrottledException e = new KinesisClientThrottledException("", null);
    when(firstIterator.readNextBatch())
        .thenThrow(e)
        .thenReturn(ImmutableList.of(a, b))
        .thenReturn(Collections.emptyList());
    when(secondIterator.readNextBatch())
        .thenReturn(singletonList(c))
        .thenReturn(singletonList(d))
        .thenReturn(Collections.emptyList());
    when(rateLimitPolicyFactory.getRateLimitPolicy()).thenReturn(customRateLimitPolicy);

    shardReadersPool.start();
    List<KinesisRecord> fetchedRecords = new ArrayList<>();
    while (fetchedRecords.size() < 4) {
      CustomOptional<KinesisRecord> nextRecord = shardReadersPool.nextRecord();
      if (nextRecord.isPresent()) {
        fetchedRecords.add(nextRecord.get());
      }
    }

    verify(customRateLimitPolicy, timeout(TIMEOUT_IN_MILLIS)).onThrottle(same(e));
    verify(customRateLimitPolicy, timeout(TIMEOUT_IN_MILLIS)).onSuccess(eq(ImmutableList.of(a, b)));
    verify(customRateLimitPolicy, timeout(TIMEOUT_IN_MILLIS)).onSuccess(eq(singletonList(c)));
    verify(customRateLimitPolicy, timeout(TIMEOUT_IN_MILLIS)).onSuccess(eq(singletonList(d)));
    verify(customRateLimitPolicy, timeout(TIMEOUT_IN_MILLIS).atLeastOnce())
        .onSuccess(eq(Collections.emptyList()));
  }
}
