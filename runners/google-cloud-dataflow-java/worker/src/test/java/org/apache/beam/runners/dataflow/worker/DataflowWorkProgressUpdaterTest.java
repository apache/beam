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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.TimeUtil.toCloudDuration;
import static org.apache.beam.runners.dataflow.util.TimeUtil.toCloudTime;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.toDynamicSplitRequest;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.FixedClock;
import com.google.api.services.dataflow.model.HotKeyDetection;
import com.google.api.services.dataflow.model.Position;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitRequest;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitResult;
import org.apache.beam.runners.dataflow.worker.util.common.worker.StubbedExecutor;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link DataflowWorkProgressUpdater}. */
@RunWith(JUnit4.class)
public class DataflowWorkProgressUpdaterTest {

  private static final long LEASE_MS = 2000;

  private static final String PROJECT_ID = "TEST_PROJECT_ID";
  private static final String JOB_ID = "TEST_JOB_ID";
  private static final Long WORK_ID = 1234567890L;
  private static final String STEP_ID = "TEST_STEP_ID";
  private static final Duration HOT_KEY_AGE = Duration.standardSeconds(1);

  @Rule public final ExpectedException thrown = ExpectedException.none();

  private StubbedExecutor executor;

  private DataflowWorkProgressUpdater progressUpdater;
  private long startTime;
  private FixedClock clock;
  @Mock private WorkItemStatusClient workItemStatusClient;
  @Mock private DataflowWorkExecutor worker;
  @Mock private HotKeyLogger hotKeyLogger;
  @Captor private ArgumentCaptor<DynamicSplitResult> splitResultCaptor;

  @Before
  public void initMocksAndWorkflowServiceAndWorkerAndWork() {
    MockitoAnnotations.initMocks(this);
    startTime = 0L;
    clock = new FixedClock(startTime);
    executor = new StubbedExecutor(clock);

    WorkItem workItem = new WorkItem();
    workItem.setProjectId(PROJECT_ID);
    workItem.setJobId(JOB_ID);
    workItem.setId(WORK_ID);
    workItem.setLeaseExpireTime(toCloudTime(new Instant(clock.currentTimeMillis() + 1000)));
    workItem.setReportStatusInterval(toCloudDuration(Duration.millis(300)));
    workItem.setInitialReportIndex(1L);

    progressUpdater =
        new DataflowWorkProgressUpdater(
            workItemStatusClient, workItem, worker, executor.getExecutor(), clock, hotKeyLogger) {

          // Shorten reporting interval boundaries for faster testing.
          @Override
          protected long getMinReportingInterval() {
            return 100;
          }

          @Override
          protected long getLeaseRenewalLatencyMargin() {
            return 150;
          }
        };
  }

  @Test
  public void workProgressSendsAnUpdate() throws Exception {
    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(generateServiceState(null, 1000));
    progressUpdater.startReportingProgress();
    executor.runNextRunnable();

    // The initial update should be sent at startTime + 300 ms.
    assertEquals(clock.currentTimeMillis(), startTime + 300);

    verify(workItemStatusClient, atLeastOnce())
        .reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class));

    progressUpdater.stopReportingProgress();
  }

  @Test
  public void workProgressLogsHotKeyDetection() throws Exception {
    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(generateServiceState(null, 1000));
    progressUpdater.startReportingProgress();
    executor.runNextRunnable();

    verify(hotKeyLogger, atLeastOnce()).logHotKeyDetection(STEP_ID, HOT_KEY_AGE);

    progressUpdater.stopReportingProgress();
  }

  @Test
  public void workProgressAskedToAbortWorker() throws Exception {
    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(generateServiceAbort());
    progressUpdater.startReportingProgress();
    executor.runNextRunnable();

    verify(worker, atLeastOnce()).abort();

    progressUpdater.stopReportingProgress();
  }

  /** Verifies that the update after a split is requested acknowledeges it. */
  @Test
  public void workProgressSendsSplitResults() throws Exception {
    // The setup process sends one update after 300ms. Enqueue another that should be scheduled
    // 1000ms after that.
    WorkItemServiceState firstResponse =
        generateServiceState(ReaderTestUtils.positionAtIndex(2L), 1000);
    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(firstResponse);
    when(worker.getWorkerProgress())
        .thenReturn(cloudProgressToReaderProgress(ReaderTestUtils.approximateProgressAtIndex(1L)));
    when(worker.requestDynamicSplit(toDynamicSplitRequest(firstResponse.getSplitRequest())))
        .thenReturn(
            new NativeReader.DynamicSplitResultWithPosition(
                cloudPositionToReaderPosition(firstResponse.getSplitRequest().getPosition())));
    progressUpdater.startReportingProgress();
    executor.runNextRunnable();

    // The initial update should be sent at startTime + 300 ms.
    assertEquals(clock.currentTimeMillis(), startTime + 300);
    verify(workItemStatusClient)
        .reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class));
    verify(worker).requestDynamicSplit(isA(DynamicSplitRequest.class));

    // The second update should be sent at startTime + 1300 ms and includes the split response.
    // Also schedule another update after 1000ms.
    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(generateServiceState(null, 1000));
    executor.runNextRunnable();
    assertEquals(clock.currentTimeMillis(), startTime + 1300);
    // Verify that the update includes the respuonse to the split request.
    verify(workItemStatusClient, atLeastOnce())
        .reportUpdate(splitResultCaptor.capture(), isA(Duration.class));
    assertEquals(
        "Second update is sent and contains the latest split result",
        splitResultCaptor.getValue(),
        new NativeReader.DynamicSplitResultWithPosition(
            cloudPositionToReaderPosition(ReaderTestUtils.positionAtIndex(2L))));

    executor.runNextRunnable();
    verify(workItemStatusClient, times(3))
        .reportUpdate(splitResultCaptor.capture(), isA(Duration.class));

    // Stop the progressUpdater now, and expect the last update immediately
    progressUpdater.stopReportingProgress();
  }

  @Test
  public void workProgressAdaptsNextDuration() throws Exception {
    progressUpdater.startReportingProgress();

    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(generateServiceState(null, 1000));
    executor.runNextRunnable();
    assertEquals(clock.currentTimeMillis(), startTime + 300);

    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(generateServiceState(null, 400));
    executor.runNextRunnable();
    assertEquals(clock.currentTimeMillis(), startTime + 1300);

    executor.runNextRunnable();
    assertEquals(clock.currentTimeMillis(), startTime + 1700);

    progressUpdater.stopReportingProgress();
  }

  /** Verifies that a last update is sent when there is an unacknowledged split request. */
  @Test
  public void workProgressUpdaterSendsLastPendingUpdateWhenStopped() throws Exception {
    // The setup process sends one update after 300ms. Enqueue another that should be scheduled
    // 1000ms after that.
    WorkItemServiceState firstResponse =
        generateServiceState(ReaderTestUtils.positionAtIndex(2L), 1000);
    when(workItemStatusClient.reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class)))
        .thenReturn(firstResponse);
    when(worker.getWorkerProgress())
        .thenReturn(cloudProgressToReaderProgress(ReaderTestUtils.approximateProgressAtIndex(1L)));
    when(worker.requestDynamicSplit(toDynamicSplitRequest(firstResponse.getSplitRequest())))
        .thenReturn(
            new NativeReader.DynamicSplitResultWithPosition(
                cloudPositionToReaderPosition(firstResponse.getSplitRequest().getPosition())));
    progressUpdater.startReportingProgress();
    executor.runNextRunnable();

    // The initial update should be sent at startTime + 300 ms.
    assertEquals(clock.currentTimeMillis(), startTime + 300);
    verify(workItemStatusClient)
        .reportUpdate(isNull(DynamicSplitResult.class), isA(Duration.class));
    verify(worker).requestDynamicSplit(isA(DynamicSplitRequest.class));

    clock.setTime(clock.currentTimeMillis() + 100);

    // Stop the progressUpdater now, and expect the last update immediately
    progressUpdater.stopReportingProgress();

    assertEquals(clock.currentTimeMillis(), startTime + 400);
    // Verify that the last update is sent immediately and contained the latest split result.
    verify(workItemStatusClient, atLeastOnce())
        .reportUpdate(splitResultCaptor.capture(), isA(Duration.class));
    assertEquals(
        "Final update is sent and contains the latest split result",
        splitResultCaptor.getValue(),
        new NativeReader.DynamicSplitResultWithPosition(
            cloudPositionToReaderPosition(ReaderTestUtils.positionAtIndex(2L))));

    // And nothing happened after that.
    verify(workItemStatusClient, Mockito.atLeastOnce()).uniqueWorkId();
    verifyNoMoreInteractions(workItemStatusClient);
  }

  private WorkItemServiceState generateServiceState(
      @Nullable Position suggestedStopPosition, long millisToNextUpdate) {
    WorkItemServiceState responseState = new WorkItemServiceState();
    responseState.setFactory(Transport.getJsonFactory());
    responseState.setLeaseExpireTime(
        toCloudTime(new Instant(clock.currentTimeMillis() + LEASE_MS)));
    responseState.setReportStatusInterval(toCloudDuration(Duration.millis(millisToNextUpdate)));

    if (suggestedStopPosition != null) {
      responseState.setSplitRequest(
          ReaderTestUtils.approximateSplitRequestAtPosition(suggestedStopPosition));
    }

    HotKeyDetection hotKeyDetection = new HotKeyDetection();
    hotKeyDetection.setUserStepName(STEP_ID);
    hotKeyDetection.setHotKeyAge(toCloudDuration(HOT_KEY_AGE));
    responseState.setHotKeyDetection(hotKeyDetection);

    return responseState;
  }

  private WorkItemServiceState generateServiceAbort() {
    WorkItemServiceState responseState = new WorkItemServiceState();
    responseState.setCompleteWorkStatus(
        com.google.rpc.Status.newBuilder()
          .setCode(com.google.rpc.Code.ABORTED)
          .setMessage("Worker was asked to abort!")
          .build());
    return responseState;
  }
}
