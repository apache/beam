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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.action;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.junit.Before;
import org.junit.Test;

public class UpdateStateActionTest {

  private UpdateStateAction action;
  private PartitionMetadataDao dao;
  private ChangeStreamMetrics metrics;
  private RestrictionTracker<PartitionRestriction, PartitionPosition> tracker;

  @Before
  public void setUp() throws Exception {
    dao = mock(PartitionMetadataDao.class);
    metrics = mock(ChangeStreamMetrics.class);
    action = new UpdateStateAction(dao, metrics);
    tracker = mock(RestrictionTracker.class);
  }

  @Test
  public void testRestrictionClaimedAndPartitionExists() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.updateState())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(partition.getScheduledAt()).thenReturn(Timestamp.now());

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(dao).updateToRunning(partitionToken);
  }

  @Test
  public void testRestrictionClaimedAndPartitionDoesNotExist() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    final SpannerException spannerException = mock(SpannerException.class);
    when(tracker.tryClaim(PartitionPosition.updateState())).thenReturn(true);
    when(partition.getPartitionToken()).thenReturn(partitionToken);
    when(spannerException.getErrorCode()).thenReturn(ErrorCode.NOT_FOUND);
    doThrow(spannerException).when(dao).updateToRunning(any());

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker);

    assertEquals(Optional.empty(), maybeContinuation);
    verify(dao).updateToRunning(partitionToken);
  }

  @Test
  public void testRestrictionNotClaimed() {
    final String partitionToken = "partitionToken";
    final PartitionMetadata partition = mock(PartitionMetadata.class);
    when(tracker.tryClaim(PartitionPosition.updateState())).thenReturn(false);
    when(partition.getPartitionToken()).thenReturn(partitionToken);

    final Optional<ProcessContinuation> maybeContinuation = action.run(partition, tracker);

    assertEquals(Optional.of(ProcessContinuation.stop()), maybeContinuation);
    verify(dao, never()).updateToRunning(anyString());
  }
}
