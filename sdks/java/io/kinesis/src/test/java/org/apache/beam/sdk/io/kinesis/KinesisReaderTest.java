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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

/**
 * Tests {@link KinesisReader}.
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisReaderTest {

  @Mock
  private SimplifiedKinesisClient kinesis;
  @Mock
  private CheckpointGenerator generator;
  @Mock
  private ShardCheckpoint firstCheckpoint, secondCheckpoint;
  @Mock
  private KinesisRecord a, b, c, d;
  @Mock
  private KinesisSource kinesisSource;
  @Mock
  private ShardReadersPool shardReadersPool;

  private KinesisReader reader;

  @Before
  public void setUp() throws IOException, TransientKinesisException {
    when(generator.generate(kinesis)).thenReturn(new KinesisReaderCheckpoint(
        asList(firstCheckpoint, secondCheckpoint)
    ));
    when(shardReadersPool.nextRecord()).thenReturn(CustomOptional.absent());
    when(a.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(b.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(c.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(d.getApproximateArrivalTimestamp()).thenReturn(Instant.now());

    reader = createReader(Duration.ZERO);
  }

  private KinesisReader createReader(Duration backlogBytesCheckThreshold)
      throws TransientKinesisException {
    KinesisReader kinesisReader = spy(new KinesisReader(kinesis, generator, kinesisSource,
        Duration.ZERO, backlogBytesCheckThreshold));
    doReturn(shardReadersPool).when(kinesisReader)
        .createShardReadersPool();
    return kinesisReader;
  }

  @Test
  public void startReturnsFalseIfNoDataAtTheBeginning() throws IOException {
    assertThat(reader.start()).isFalse();
  }

  @Test(expected = NoSuchElementException.class)
  public void throwsNoSuchElementExceptionIfNoData() throws IOException {
    reader.start();
    reader.getCurrent();
  }

  @Test
  public void startReturnsTrueIfSomeDataAvailable() throws IOException,
      TransientKinesisException {
    when(shardReadersPool.nextRecord())
        .thenReturn(CustomOptional.of(a))
        .thenReturn(CustomOptional.absent());

    assertThat(reader.start()).isTrue();
  }

  @Test
  public void readsThroughAllDataAvailable() throws IOException, TransientKinesisException {
    when(shardReadersPool.nextRecord())
        .thenReturn(CustomOptional.of(c))
        .thenReturn(CustomOptional.absent())
        .thenReturn(CustomOptional.of(a))
        .thenReturn(CustomOptional.absent())
        .thenReturn(CustomOptional.of(d))
        .thenReturn(CustomOptional.of(b))
        .thenReturn(CustomOptional.absent());

    assertThat(reader.start()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(c);
    assertThat(reader.advance()).isFalse();
    assertThat(reader.advance()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(a);
    assertThat(reader.advance()).isFalse();
    assertThat(reader.advance()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(d);
    assertThat(reader.advance()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(b);
    assertThat(reader.advance()).isFalse();
  }

  @Test
  public void watermarkDoesNotChangeWhenToFewSampleRecords()
      throws IOException, TransientKinesisException {
    try {
      Instant now = Instant.now();
      DateTimeUtils.setCurrentMillisFixed(now.getMillis());
      Instant recordsStartTimestamp = now.minus(Duration.standardHours(1));
      final long timestampMs = recordsStartTimestamp.getMillis();
      Duration safetyPeriod = Duration.standardMinutes(1);
      Instant minKinesisWatermark = now.minus(KinesisReader.MAX_KINESIS_STREAM_RETENTION_PERIOD);

      prepareRecordsWithArrivalTimestamps(timestampMs, 1, KinesisReader.MIN_WATERMARK_MESSAGES / 2);

      for (boolean more = reader.start(); more; more = reader.advance()) {
        assertThat(reader.getWatermark()).isBetween(
            minKinesisWatermark.minus(safetyPeriod),
            minKinesisWatermark.plus(safetyPeriod));
      }
    } finally {
      DateTimeUtils.setCurrentMillisSystem();
    }
  }

  @Test
  public void watermarkAdvancesWhenEnoughRecordsReadRecently()
      throws IOException, TransientKinesisException {
    try {
      Instant now = Instant.now();
      DateTimeUtils.setCurrentMillisFixed(now.getMillis());
      Instant recordsStartTimestamp = now.minus(Duration.standardHours(1));
      long timestampMs = recordsStartTimestamp.getMillis();
      Duration safetyPeriod = Duration.standardMinutes(1);
      Instant minKinesisWatermark = now.minus(KinesisReader.MAX_KINESIS_STREAM_RETENTION_PERIOD);

      prepareRecordsWithArrivalTimestamps(timestampMs, 1, KinesisReader.MIN_WATERMARK_MESSAGES);

      int recordsNeededForWatermarkAdvancing = KinesisReader.MIN_WATERMARK_MESSAGES;
      for (boolean more = reader.start(); more; more = reader.advance()) {
        if (--recordsNeededForWatermarkAdvancing > 0) {
          assertThat(reader.getWatermark()).isBetween(
              minKinesisWatermark.minus(safetyPeriod),
              minKinesisWatermark.plus(safetyPeriod));
        } else {
          assertThat(reader.getWatermark()).isEqualTo(new Instant(timestampMs));
        }
      }
    } finally {
      DateTimeUtils.setCurrentMillisSystem();
    }
  }

  @Test
  public void watermarkMonotonicallyIncreases()
      throws IOException, TransientKinesisException {
    long timestampMs = 1000L;

    prepareRecordsWithArrivalTimestamps(timestampMs, -1, KinesisReader.MIN_WATERMARK_MESSAGES * 2);

    Instant lastWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    for (boolean more = reader.start(); more; more = reader.advance()) {
      Instant currentWatermark = reader.getWatermark();
      assertThat(currentWatermark).isGreaterThanOrEqualTo(lastWatermark);
      lastWatermark = currentWatermark;
    }
    assertThat(reader.advance()).isFalse();
  }

  private void prepareRecordsWithArrivalTimestamps(long initialTimestampMs, int increment,
      int count) throws TransientKinesisException {
    long timestampMs = initialTimestampMs;
    KinesisRecord firstRecord = prepareRecordMockWithArrivalTimestamp(timestampMs);
    OngoingStubbing<CustomOptional<KinesisRecord>> shardReadersPoolStubbing =
        when(shardReadersPool.nextRecord()).thenReturn(CustomOptional.of(firstRecord));
    for (int i = 0; i < count; i++) {
      timestampMs += increment;
      KinesisRecord record = prepareRecordMockWithArrivalTimestamp(timestampMs);
      shardReadersPoolStubbing = shardReadersPoolStubbing.thenReturn(CustomOptional.of(record));
    }
    shardReadersPoolStubbing.thenReturn(CustomOptional.absent());
  }

  private KinesisRecord prepareRecordMockWithArrivalTimestamp(long timestampMs) {
    KinesisRecord record = mock(KinesisRecord.class);
    when(record.getApproximateArrivalTimestamp()).thenReturn(new Instant(timestampMs));
    return record;
  }

  @Test
  public void getTotalBacklogBytesShouldReturnLastSeenValueWhenKinesisExceptionsOccur()
      throws TransientKinesisException, IOException {
    reader.start();
    when(kinesisSource.getStreamName()).thenReturn("stream1");
    when(kinesis.getBacklogBytes(eq("stream1"), any(Instant.class)))
        .thenReturn(10L)
        .thenThrow(TransientKinesisException.class)
        .thenReturn(20L);

    assertThat(reader.getTotalBacklogBytes()).isEqualTo(10);
    assertThat(reader.getTotalBacklogBytes()).isEqualTo(10);
    assertThat(reader.getTotalBacklogBytes()).isEqualTo(20);
  }

  @Test
  public void getTotalBacklogBytesShouldReturnLastSeenValueWhenCalledFrequently()
      throws TransientKinesisException, IOException {
    KinesisReader backlogCachingReader = createReader(Duration.standardSeconds(30));
    backlogCachingReader.start();
    when(kinesisSource.getStreamName()).thenReturn("stream1");
    when(kinesis.getBacklogBytes(eq("stream1"), any(Instant.class)))
        .thenReturn(10L)
        .thenReturn(20L);

    assertThat(backlogCachingReader.getTotalBacklogBytes()).isEqualTo(10);
    assertThat(backlogCachingReader.getTotalBacklogBytes()).isEqualTo(10);
  }
}
