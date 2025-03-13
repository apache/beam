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
import static org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicyFactory.withArrivalTimePolicy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

@RunWith(MockitoJUnitRunner.Silent.class)
public class EFOKinesisReaderTest {
  @Mock private KinesisAsyncClient kinesis;
  @Mock private KinesisIO.Read read;
  @Mock private KinesisReaderCheckpoint checkpoint;
  @Mock private ShardCheckpoint firstCheckpoint, secondCheckpoint;
  @Mock private KinesisRecord a, b, c, d;
  @Mock private KinesisSource kinesisSource;
  @Mock private EFOShardSubscribersPool subscribersPool;

  private EFOKinesisReader reader;

  @Before
  public void setUp() throws IOException {
    when(read.getWatermarkPolicyFactory()).thenReturn(withArrivalTimePolicy());
    when(read.getStreamName()).thenReturn("stream1");
    when(read.getConsumerArn()).thenReturn("consumer1");

    checkpoint = new KinesisReaderCheckpoint(asList(firstCheckpoint, secondCheckpoint));
    when(subscribersPool.getNextRecord()).thenReturn(null);
    when(subscribersPool.getCheckpointMark()).thenReturn(checkpoint);
    when(a.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(b.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(c.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    when(d.getApproximateArrivalTimestamp()).thenReturn(Instant.now());
    reader =
        new EFOKinesisReader(read, read.getConsumerArn(), kinesis, checkpoint, kinesisSource) {
          @Override
          EFOShardSubscribersPool createPool() {
            return subscribersPool;
          }
        };
  }

  @After
  public void after() throws IOException {
    reader.close();
    verify(kinesis).close();
  }

  @Test
  public void getCheckpointMarkReturnsCheckpoints() throws IOException {
    Throwable e = assertThrows(IllegalStateException.class, () -> reader.getCheckpointMark());
    assertThat(e.getMessage()).isEqualTo("Reader was not started");
    reader.start();
    assertThat(reader.getCheckpointMark()).isEqualTo(checkpoint);
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
    when(subscribersPool.getNextRecord()).thenReturn(a).thenReturn(null);
    assertThat(reader.start()).isTrue();
  }

  @Test
  public void readsThroughAllDataAvailable() throws IOException {
    byte[] cId = new byte[] {1, 2};
    Instant cTs = Instant.now();
    when(c.getUniqueId()).thenReturn(cId);
    when(c.getApproximateArrivalTimestamp()).thenReturn(cTs);

    when(subscribersPool.getNextRecord())
        .thenReturn(c)
        .thenReturn(null)
        .thenReturn(a)
        .thenReturn(null)
        .thenReturn(d)
        .thenReturn(b)
        .thenReturn(null);

    assertThat(reader.start()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(c);

    assertThat(reader.getCurrentRecordId()).isEqualTo(cId);
    assertThat(reader.getCurrentTimestamp()).isEqualTo(cTs);

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
    when(subscribersPool.getWatermark()).thenReturn(expectedWatermark);

    reader.start();
    Instant currentWatermark = reader.getWatermark();

    assertThat(currentWatermark).isEqualTo(expectedWatermark);
  }
}
