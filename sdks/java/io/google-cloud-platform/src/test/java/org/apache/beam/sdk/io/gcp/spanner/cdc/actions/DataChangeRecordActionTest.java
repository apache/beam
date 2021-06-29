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
package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class DataChangeRecordActionTest {

  private DataChangeRecordAction action;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;
  private OutputReceiver<DataChangeRecord> outputReceiver;
  private ManualWatermarkEstimator<Instant> watermarkEstimator;

  @Before
  public void setUp() {
    action = new DataChangeRecordAction();
    tracker = mock(RestrictionTracker.class);
    outputReceiver = mock(OutputReceiver.class);
    watermarkEstimator = mock(ManualWatermarkEstimator.class);
  }

  @Test
  public void testRestrictionClaimed() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final DataChangeRecord record = mock(DataChangeRecord.class);
    when(record.getCommitTimestamp()).thenReturn(timestamp);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(timestamp))).thenReturn(true);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(record, tracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(outputReceiver).output(record);
    verify(watermarkEstimator).setWatermark(new Instant(timestamp.toSqlTimestamp().getTime()));
  }

  @Test
  public void testRestrictionNotClaimed() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final DataChangeRecord record = mock(DataChangeRecord.class);
    when(record.getCommitTimestamp()).thenReturn(timestamp);
    when(tracker.tryClaim(PartitionPosition.queryChangeStream(timestamp))).thenReturn(false);

    final Optional<ProcessContinuation> maybeContinuation =
        action.run(record, tracker, outputReceiver, watermarkEstimator);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(outputReceiver, never()).output(any());
    verify(watermarkEstimator, never()).setWatermark(any());
  }
}
