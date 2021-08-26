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
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;

/** Tests {@link ShardRecordsIterator}. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class ShardRecordsIteratorTest {

  private static final String INITIAL_ITERATOR = "INITIAL_ITERATOR";
  private static final String SECOND_ITERATOR = "SECOND_ITERATOR";
  private static final String SECOND_REFRESHED_ITERATOR = "SECOND_REFRESHED_ITERATOR";
  private static final String THIRD_ITERATOR = "THIRD_ITERATOR";
  private static final String STREAM_NAME = "STREAM_NAME";
  private static final String SHARD_ID = "SHARD_ID";
  private static final Instant NOW = Instant.now();

  @Mock private SimplifiedKinesisClient kinesisClient;
  @Mock private ShardCheckpoint firstCheckpoint, aCheckpoint, bCheckpoint, cCheckpoint, dCheckpoint;
  @Mock private GetKinesisRecordsResult firstResult, secondResult, thirdResult;
  @Mock private KinesisRecord a, b, c, d;
  @Mock private RecordFilter recordFilter;

  private ShardRecordsIterator iterator;

  @Before
  public void setUp() throws IOException, TransientKinesisException {
    when(firstCheckpoint.getShardIterator(kinesisClient)).thenReturn(INITIAL_ITERATOR);
    when(firstCheckpoint.getStreamName()).thenReturn(STREAM_NAME);
    when(firstCheckpoint.getShardId()).thenReturn(SHARD_ID);

    when(firstCheckpoint.moveAfter(a)).thenReturn(aCheckpoint);
    when(aCheckpoint.moveAfter(b)).thenReturn(bCheckpoint);
    when(aCheckpoint.getStreamName()).thenReturn(STREAM_NAME);
    when(aCheckpoint.getShardId()).thenReturn(SHARD_ID);
    when(bCheckpoint.moveAfter(c)).thenReturn(cCheckpoint);
    when(bCheckpoint.getStreamName()).thenReturn(STREAM_NAME);
    when(bCheckpoint.getShardId()).thenReturn(SHARD_ID);
    when(cCheckpoint.moveAfter(d)).thenReturn(dCheckpoint);
    when(cCheckpoint.getStreamName()).thenReturn(STREAM_NAME);
    when(cCheckpoint.getShardId()).thenReturn(SHARD_ID);
    when(dCheckpoint.getStreamName()).thenReturn(STREAM_NAME);
    when(dCheckpoint.getShardId()).thenReturn(SHARD_ID);

    when(kinesisClient.getRecords(INITIAL_ITERATOR, STREAM_NAME, SHARD_ID)).thenReturn(firstResult);
    when(kinesisClient.getRecords(SECOND_ITERATOR, STREAM_NAME, SHARD_ID)).thenReturn(secondResult);
    when(kinesisClient.getRecords(THIRD_ITERATOR, STREAM_NAME, SHARD_ID)).thenReturn(thirdResult);

    when(firstResult.getNextShardIterator()).thenReturn(SECOND_ITERATOR);
    when(secondResult.getNextShardIterator()).thenReturn(THIRD_ITERATOR);
    when(thirdResult.getNextShardIterator()).thenReturn(THIRD_ITERATOR);

    when(firstResult.getRecords()).thenReturn(Collections.emptyList());
    when(secondResult.getRecords()).thenReturn(Collections.emptyList());
    when(thirdResult.getRecords()).thenReturn(Collections.emptyList());

    when(recordFilter.apply(anyList(), any(ShardCheckpoint.class)))
        .thenAnswer(new IdentityAnswer());

    WatermarkPolicyFactory watermarkPolicyFactory = WatermarkPolicyFactory.withArrivalTimePolicy();
    iterator =
        new ShardRecordsIterator(
            firstCheckpoint, kinesisClient, watermarkPolicyFactory, recordFilter);
  }

  @Test
  public void goesThroughAvailableRecords()
      throws IOException, TransientKinesisException, KinesisShardClosedException {
    when(firstResult.getRecords()).thenReturn(asList(a, b, c));
    when(secondResult.getRecords()).thenReturn(singletonList(d));
    when(thirdResult.getRecords()).thenReturn(Collections.emptyList());

    assertThat(iterator.getCheckpoint()).isEqualTo(firstCheckpoint);
    assertThat(iterator.readNextBatch()).isEqualTo(asList(a, b, c));
    assertThat(iterator.readNextBatch()).isEqualTo(singletonList(d));
    assertThat(iterator.readNextBatch()).isEqualTo(Collections.emptyList());
  }

  @Test
  public void conformingRecordsMovesCheckpoint() throws IOException, TransientKinesisException {
    when(firstResult.getRecords()).thenReturn(asList(a, b, c));
    when(secondResult.getRecords()).thenReturn(singletonList(d));
    when(thirdResult.getRecords()).thenReturn(Collections.emptyList());

    when(a.getApproximateArrivalTimestamp()).thenReturn(NOW);
    when(b.getApproximateArrivalTimestamp()).thenReturn(NOW.plus(Duration.standardSeconds(1)));
    when(c.getApproximateArrivalTimestamp()).thenReturn(NOW.plus(Duration.standardSeconds(2)));
    when(d.getApproximateArrivalTimestamp()).thenReturn(NOW.plus(Duration.standardSeconds(3)));

    iterator.ackRecord(a);
    assertThat(iterator.getCheckpoint()).isEqualTo(aCheckpoint);
    iterator.ackRecord(b);
    assertThat(iterator.getCheckpoint()).isEqualTo(bCheckpoint);
    iterator.ackRecord(c);
    assertThat(iterator.getCheckpoint()).isEqualTo(cCheckpoint);
    iterator.ackRecord(d);
    assertThat(iterator.getCheckpoint()).isEqualTo(dCheckpoint);
  }

  @Test
  public void refreshesExpiredIterator()
      throws IOException, TransientKinesisException, KinesisShardClosedException {
    when(firstResult.getRecords()).thenReturn(singletonList(a));
    when(secondResult.getRecords()).thenReturn(singletonList(b));

    when(a.getApproximateArrivalTimestamp()).thenReturn(NOW);
    when(b.getApproximateArrivalTimestamp()).thenReturn(NOW.plus(Duration.standardSeconds(1)));

    when(kinesisClient.getRecords(SECOND_ITERATOR, STREAM_NAME, SHARD_ID))
        .thenThrow(ExpiredIteratorException.class);
    when(aCheckpoint.getShardIterator(kinesisClient)).thenReturn(SECOND_REFRESHED_ITERATOR);
    when(kinesisClient.getRecords(SECOND_REFRESHED_ITERATOR, STREAM_NAME, SHARD_ID))
        .thenReturn(secondResult);

    assertThat(iterator.readNextBatch()).isEqualTo(singletonList(a));
    iterator.ackRecord(a);
    assertThat(iterator.readNextBatch()).isEqualTo(singletonList(b));
    assertThat(iterator.readNextBatch()).isEqualTo(Collections.emptyList());
  }

  @Test
  public void tracksLatestRecordTimestamp() {
    when(firstResult.getRecords()).thenReturn(singletonList(a));
    when(secondResult.getRecords()).thenReturn(asList(b, c));
    when(thirdResult.getRecords()).thenReturn(singletonList(c));

    when(a.getApproximateArrivalTimestamp()).thenReturn(NOW);
    when(b.getApproximateArrivalTimestamp()).thenReturn(NOW.plus(Duration.standardSeconds(4)));
    when(c.getApproximateArrivalTimestamp()).thenReturn(NOW.plus(Duration.standardSeconds(2)));
    when(d.getApproximateArrivalTimestamp()).thenReturn(NOW.plus(Duration.standardSeconds(6)));

    iterator.ackRecord(a);
    assertThat(iterator.getLatestRecordTimestamp()).isEqualTo(NOW);
    iterator.ackRecord(b);
    assertThat(iterator.getLatestRecordTimestamp())
        .isEqualTo(NOW.plus(Duration.standardSeconds(4)));
    iterator.ackRecord(c);
    assertThat(iterator.getLatestRecordTimestamp())
        .isEqualTo(NOW.plus(Duration.standardSeconds(4)));
    iterator.ackRecord(d);
    assertThat(iterator.getLatestRecordTimestamp())
        .isEqualTo(NOW.plus(Duration.standardSeconds(6)));
  }

  private static class IdentityAnswer implements Answer<Object> {

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      return invocation.getArguments()[0];
    }
  }
}
