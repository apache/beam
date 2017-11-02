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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

/**
 * Tests {@link ShardReadersPool}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardReadersPoolTest {

  @Mock
  private ShardRecordsIterator firstIterator, secondIterator;
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
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(
        Arrays.asList(firstCheckpoint, secondCheckpoint));
    shardReadersPool = Mockito.spy(new ShardReadersPool(kinesis, checkpoint));
    doReturn(firstIterator).when(shardReadersPool).createShardIterator(kinesis, firstCheckpoint);
    doReturn(secondIterator).when(shardReadersPool).createShardIterator(kinesis, secondCheckpoint);
  }

  @Test
  public void shouldReturnAllRecords() throws TransientKinesisException {
    when(firstIterator.readNextBatch())
        .thenReturn(Collections.<KinesisRecord>emptyList())
        .thenReturn(asList(a, b))
        .thenReturn(Collections.<KinesisRecord>emptyList());
    when(secondIterator.readNextBatch())
        .thenReturn(singletonList(c))
        .thenReturn(singletonList(d))
        .thenReturn(Collections.<KinesisRecord>emptyList());

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
  public void shouldReturnAbsentOptionalWhenNoRecords() throws TransientKinesisException {
    when(firstIterator.readNextBatch())
        .thenReturn(Collections.<KinesisRecord>emptyList());
    when(secondIterator.readNextBatch())
        .thenReturn(Collections.<KinesisRecord>emptyList());

    shardReadersPool.start();
    CustomOptional<KinesisRecord> nextRecord = shardReadersPool.nextRecord();
    assertThat(nextRecord.isPresent()).isFalse();
  }

  @Test
  public void shouldCheckpointReadRecords() throws TransientKinesisException {
    when(firstIterator.readNextBatch())
        .thenReturn(asList(a, b))
        .thenReturn(Collections.<KinesisRecord>emptyList());
    when(secondIterator.readNextBatch())
        .thenReturn(singletonList(c))
        .thenReturn(singletonList(d))
        .thenReturn(Collections.<KinesisRecord>emptyList());

    shardReadersPool.start();
    int recordsFound = 0;
    while (recordsFound < 4) {
      CustomOptional<KinesisRecord> nextRecord = shardReadersPool.nextRecord();
      if (nextRecord.isPresent()) {
        recordsFound++;
        KinesisRecord kinesisRecord = nextRecord.get();
        if (kinesisRecord.getShardId().equals("shard1")) {
          verify(firstIterator).ackRecord(kinesisRecord);
        } else {
          verify(secondIterator).ackRecord(kinesisRecord);
        }
      }
    }
  }

  @Test
  public void shouldInterruptKinesisReadingAndStopShortly() throws TransientKinesisException {
    when(firstIterator.readNextBatch()).thenAnswer(new Answer<List<KinesisRecord>>() {

      @Override
      public List<KinesisRecord> answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        return Collections.emptyList();
      }
    });
    shardReadersPool.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    shardReadersPool.stop();
    assertThat(stopwatch.elapsed(TimeUnit.MILLISECONDS)).isLessThan(TimeUnit.SECONDS.toMillis(1));
  }

  @Test
  public void shouldInterruptPuttingRecordsToQueueAndStopShortly()
      throws TransientKinesisException {
    when(firstIterator.readNextBatch()).thenReturn(asList(a, b, c));
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(
        Arrays.asList(firstCheckpoint, secondCheckpoint));
    ShardReadersPool shardReadersPool = new ShardReadersPool(kinesis, checkpoint, 2);
    shardReadersPool.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    shardReadersPool.stop();
    assertThat(stopwatch.elapsed(TimeUnit.MILLISECONDS)).isLessThan(TimeUnit.SECONDS.toMillis(1));

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
}
