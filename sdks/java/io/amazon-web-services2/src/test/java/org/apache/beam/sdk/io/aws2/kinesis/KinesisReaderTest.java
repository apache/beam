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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests {@link KinesisReader}. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class KinesisReaderTest {

  @Mock private SimplifiedKinesisClient kinesis;
  @Mock private CheckpointGenerator generator;
  @Mock private ShardCheckpoint firstCheckpoint, secondCheckpoint;
  @Mock private KinesisRecord a, b, c, d;
  @Mock private KinesisSource kinesisSource;
  @Mock private ShardReadersPool shardReadersPool;

  private KinesisReader reader;

  @Before
  public void setUp() throws TransientKinesisException {
    when(generator.generate(kinesis))
        .thenReturn(new KinesisReaderCheckpoint(asList(firstCheckpoint, secondCheckpoint)));
    when(shardReadersPool.nextRecord()).thenReturn(CustomOptional.absent());
    when(a.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(b.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(c.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(d.getApproximateArrivalTimestamp()).thenReturn(Instant.now());

    reader = spy(createReader(Duration.ZERO));
  }

  private KinesisReader createReader(Duration backlogBytesCheckThreshold) {
    return new KinesisReader(
        kinesis,
        generator,
        kinesisSource,
        WatermarkPolicyFactory.withArrivalTimePolicy(),
        RateLimitPolicyFactory.withoutLimiter(),
        Duration.ZERO,
        backlogBytesCheckThreshold,
        ShardReadersPool.DEFAULT_CAPACITY_PER_SHARD) {
      @Override
      ShardReadersPool createShardReadersPool() {
        return shardReadersPool;
      }
    };
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
  public void startReturnsTrueIfSomeDataAvailable() throws IOException {
    when(shardReadersPool.nextRecord())
        .thenReturn(CustomOptional.of(a))
        .thenReturn(CustomOptional.absent());

    assertThat(reader.start()).isTrue();
  }

  @Test
  public void readsThroughAllDataAvailable() throws IOException {
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
  public void returnsCurrentWatermark() throws IOException {
    Instant expectedWatermark = new Instant(123456L);
    when(shardReadersPool.getWatermark()).thenReturn(expectedWatermark);

    reader.start();
    Instant currentWatermark = reader.getWatermark();

    assertThat(currentWatermark).isEqualTo(expectedWatermark);
  }

  @Test
  public void getSplitBacklogBytesShouldReturnLastSeenValueWhenKinesisExceptionsOccur()
      throws TransientKinesisException, IOException {
    reader.start();
    when(kinesisSource.getStreamName()).thenReturn("stream1");
    when(shardReadersPool.getLatestRecordTimestamp())
        .thenReturn(Instant.now().minus(Duration.standardMinutes(1)));
    when(kinesis.getBacklogBytes(eq("stream1"), any(Instant.class)))
        .thenReturn(10L)
        .thenThrow(TransientKinesisException.class)
        .thenReturn(20L);

    assertThat(reader.getSplitBacklogBytes()).isEqualTo(10);
    assertThat(reader.getSplitBacklogBytes()).isEqualTo(10);
    assertThat(reader.getSplitBacklogBytes()).isEqualTo(20);
  }

  @Test
  public void getSplitBacklogBytesShouldReturnLastSeenValueWhenCalledFrequently()
      throws TransientKinesisException, IOException {
    KinesisReader backlogCachingReader = spy(createReader(Duration.standardSeconds(30)));
    backlogCachingReader.start();
    when(shardReadersPool.getLatestRecordTimestamp())
        .thenReturn(Instant.now().minus(Duration.standardMinutes(1)));
    when(kinesisSource.getStreamName()).thenReturn("stream1");
    when(kinesis.getBacklogBytes(eq("stream1"), any(Instant.class)))
        .thenReturn(10L)
        .thenReturn(20L);

    assertThat(backlogCachingReader.getSplitBacklogBytes()).isEqualTo(10);
    assertThat(backlogCachingReader.getSplitBacklogBytes()).isEqualTo(10);
  }

  @Test
  public void getSplitBacklogBytesShouldReturnBacklogUnknown()
      throws IOException, TransientKinesisException {
    reader.start();
    when(kinesisSource.getStreamName()).thenReturn("stream1");
    when(shardReadersPool.getLatestRecordTimestamp())
        .thenReturn(BoundedWindow.TIMESTAMP_MIN_VALUE)
        .thenReturn(Instant.now().minus(Duration.standardMinutes(1)));
    when(kinesis.getBacklogBytes(eq("stream1"), any(Instant.class))).thenReturn(10L);

    assertThat(reader.getSplitBacklogBytes())
        .isEqualTo(UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN);
    assertThat(reader.getSplitBacklogBytes()).isEqualTo(10);
  }
}
