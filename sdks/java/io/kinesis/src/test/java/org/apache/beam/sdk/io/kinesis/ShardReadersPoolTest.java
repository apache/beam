package org.apache.beam.sdk.io.kinesis;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
  private KinesisRecord a, b, c, d;

  @Before
  public void setUp() {
    when(firstIterator.getShardId()).thenReturn("shard1");
    when(secondIterator.getShardId()).thenReturn("shard2");

    when(a.getShardId()).thenReturn("shard1");
    when(b.getShardId()).thenReturn("shard1");
    when(c.getShardId()).thenReturn("shard2");
    when(d.getShardId()).thenReturn("shard2");
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

    ShardReadersPool shardReadersPool = new ShardReadersPool(asList(firstIterator, secondIterator));
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

    ShardReadersPool shardReadersPool = new ShardReadersPool(asList(firstIterator, secondIterator));
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

    ShardReadersPool shardReadersPool = new ShardReadersPool(asList(firstIterator, secondIterator));
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
    ShardReadersPool shardReadersPool = new ShardReadersPool(asList(firstIterator, secondIterator));
    shardReadersPool.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    shardReadersPool.stop();
    assertThat(stopwatch.elapsed(TimeUnit.MILLISECONDS)).isLessThan(TimeUnit.SECONDS.toMillis(1));
  }

  @Test
  public void shouldInterruptPuttingRecordsToQueueAndStopShortly()
      throws TransientKinesisException {
    when(firstIterator.readNextBatch()).thenReturn(asList(a, b, c));
    ShardReadersPool shardReadersPool = new ShardReadersPool(asList(firstIterator, secondIterator),
        2);
    shardReadersPool.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    shardReadersPool.stop();
    assertThat(stopwatch.elapsed(TimeUnit.MILLISECONDS)).isLessThan(TimeUnit.SECONDS.toMillis(1));

  }

  @Test
  public void shouldDetectThatNotAllShardsAreUpToDate() {
    when(firstIterator.isUpToDate()).thenReturn(true);
    when(secondIterator.isUpToDate()).thenReturn(false);
    ShardReadersPool shardReadersPool = new ShardReadersPool(asList(firstIterator, secondIterator));
    shardReadersPool.start();

    assertThat(shardReadersPool.allShardsUpToDate()).isFalse();
  }

  @Test
  public void shouldDetectThatAllShardsAreUpToDate() {
    when(firstIterator.isUpToDate()).thenReturn(true);
    when(secondIterator.isUpToDate()).thenReturn(true);
    ShardReadersPool shardReadersPool = new ShardReadersPool(asList(firstIterator, secondIterator));
    shardReadersPool.start();

    assertThat(shardReadersPool.allShardsUpToDate()).isTrue();
  }
}
