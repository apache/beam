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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.action;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.ReadChangeStreamPartitionProgressTracker;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ChangeStreamActionTest {

  private ChangeStreamMetrics metrics;
  private ChangeStreamAction action;

  private RestrictionTracker<StreamProgress, StreamProgress> tracker;
  private PartitionRecord partitionRecord;
  private DoFn.OutputReceiver<KV<ByteString, ChangeStreamMutation>> receiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    metrics = mock(ChangeStreamMetrics.class);
    tracker = mock(ReadChangeStreamPartitionProgressTracker.class);
    partitionRecord = mock(PartitionRecord.class);
    receiver = mock(DoFn.OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);

    action = new ChangeStreamAction(metrics);
    when(tracker.tryClaim(any())).thenReturn(true);
  }

  @Test
  public void testHeartBeat() {
    final Timestamp lowWatermark = Timestamp.newBuilder().setSeconds(1000).build();
    ChangeStreamContinuationToken changeStreamContinuationToken =
        new ChangeStreamContinuationToken(ByteStringRange.create("a", "b"), "1234");
    Heartbeat mockHeartBeat = Mockito.mock(Heartbeat.class);
    Mockito.when(mockHeartBeat.getLowWatermark()).thenReturn(lowWatermark);
    Mockito.when(mockHeartBeat.getChangeStreamContinuationToken())
        .thenReturn(changeStreamContinuationToken);

    final Optional<DoFn.ProcessContinuation> result =
        action.run(partitionRecord, mockHeartBeat, tracker, receiver, watermarkEstimator, false);

    assertFalse(result.isPresent());
    verify(metrics).incHeartbeatCount();
    verify(watermarkEstimator).setWatermark(eq(TimestampConverter.toInstant(lowWatermark)));
    StreamProgress streamProgress = new StreamProgress(changeStreamContinuationToken, lowWatermark);
    verify(tracker).tryClaim(eq(streamProgress));
  }
}
