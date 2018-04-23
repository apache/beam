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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

/**
 * Tests {@link ShardReadersPool}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardReadersPoolTest {

  private static final int TIMEOUT_IN_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);

  @Mock
  private ShardRecordsIterator firstIterator, secondIterator, thirdIterator, fourthIterator;
  @Mock
  private ShardCheckpoint firstCheckpoint, secondCheckpoint;
  @Mock
  private SimplifiedKinesisClient kinesis;
  @Mock
  private KinesisRecord a, b, c, d;

  private ShardReadersPool shardReadersPool;

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
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(
        Arrays.asList(firstCheckpoint, secondCheckpoint));
    shardReadersPool = Mockito.spy(new ShardReadersPool(kinesis, checkpoint));
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
        .thenReturn(asList(a, b))
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
  }

  @Test
  public void shouldReturnAbsentOptionalWhenNoRecords()
      throws TransientKinesisException, KinesisShardClosedException {
    when(firstIterator.readNextBatch())
        .thenReturn(Collections.emptyList());
    when(secondIterator.readNextBatch())
        .thenReturn(Collections.emptyList());

    shardReadersPool.start();
    CustomOptional<KinesisRecord> nextRecord = shardReadersPool.nextRecord();
    assertThat(nextRecord.isPresent()).isFalse();
  }

  @Test
  public void shouldCheckpointReadRecords()
      throws TransientKinesisException, KinesisShardClosedException {
    when(firstIterator.readNextBatch())
        .thenReturn(asList(a, b))
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
    when(firstIterator.readNextBatch()).thenAnswer((Answer<List<KinesisRecord>>) invocation -> {
      Thread.sleep(TimeUnit.MINUTES.toMillis(1));
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
    when(firstIterator.readNextBatch()).thenReturn(asList(a, b, c));
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(
        Arrays.asList(firstCheckpoint, secondCheckpoint));
    ShardReadersPool shardReadersPool = new ShardReadersPool(kinesis, checkpoint, 2);
    shardReadersPool.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    shardReadersPool.stop();
    assertThat(stopwatch.elapsed(TimeUnit.MILLISECONDS)).isLessThan(TIMEOUT_IN_MILLIS);

  }

  @Test
  public void shouldDetectThatNotAllShardsAreUpToDate() throws TransientKinesisException {
    when(firstIterator.isUpToDate()).thenReturn(true);
    when(secondIterator.isUpToDate()).thenReturn(false);
    shardReadersPool.start();

    assertThat(shardReadersPool.allShardsUpToDate()).isFalse();
  }

  @Test
  public void shouldDetectThatAllShardsAreUpToDate() throws TransientKinesisException {
    when(firstIterator.isUpToDate()).thenReturn(true);
    when(secondIterator.isUpToDate()).thenReturn(true);
    shardReadersPool.start();

    assertThat(shardReadersPool.allShardsUpToDate()).isTrue();
  }

  @Test
  public void shouldStopReadingShardAfterReceivingShardClosedException() throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    when(firstIterator.findSuccessiveShardRecordIterators())
        .thenReturn(Collections.emptyList());

    shardReadersPool.start();

    verify(firstIterator, timeout(TIMEOUT_IN_MILLIS).times(1)).readNextBatch();
    verify(secondIterator, timeout(TIMEOUT_IN_MILLIS).atLeast(2)).readNextBatch();
  }

  @Test
  public void shouldStartReadingSuccessiveShardsAfterReceivingShardClosedException()
      throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    when(firstIterator.findSuccessiveShardRecordIterators())
        .thenReturn(asList(thirdIterator, fourthIterator));

    shardReadersPool.start();

    verify(thirdIterator, timeout(TIMEOUT_IN_MILLIS).atLeast(2)).readNextBatch();
    verify(fourthIterator, timeout(TIMEOUT_IN_MILLIS).atLeast(2)).readNextBatch();
  }

  @Test
  public void shouldStopReadersPoolWhenLastShardReaderStopped() throws Exception {
    when(firstIterator.readNextBatch()).thenThrow(KinesisShardClosedException.class);
    when(firstIterator.findSuccessiveShardRecordIterators())
        .thenReturn(Collections.emptyList());

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
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(
        Collections.emptyList());
    shardReadersPool = Mockito.spy(new ShardReadersPool(kinesis, checkpoint));
    doReturn(firstIterator).when(shardReadersPool)
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
    verify(shardReadersPool).startReadingShards(Arrays.asList(firstIterator, secondIterator));
    verify(shardReadersPool, timeout(TIMEOUT_IN_MILLIS)).startReadingShards(emptyList);

    KinesisReaderCheckpoint checkpointMark = shardReadersPool.getCheckpointMark();
    assertThat(checkpointMark.iterator())
        .extracting("shardId", String.class)
        .containsOnly("shard2")
        .doesNotContain("shard1");
  }
}
