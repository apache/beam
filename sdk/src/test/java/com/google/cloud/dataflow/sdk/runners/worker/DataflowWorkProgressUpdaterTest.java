/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.approximateProgressAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.approximateProgressAtPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.splitRequestToApproximateProgress;
import static com.google.cloud.dataflow.sdk.util.CloudCounterUtils.extractCounter;
import static com.google.cloud.dataflow.sdk.util.CloudMetricUtils.extractCloudMetric;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudDuration;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudTime;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MAX;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MIN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.api.services.dataflow.model.Position;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterTestUtils;
import com.google.cloud.dataflow.sdk.util.common.Metric;
import com.google.cloud.dataflow.sdk.util.common.Metric.DoubleMetric;
import com.google.cloud.dataflow.sdk.util.common.worker.MapTaskExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.Operation;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;

import org.hamcrest.Description;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

/** Unit tests for {@link DataflowWorkProgressUpdater}. */
@RunWith(JUnit4.class)
@SuppressWarnings("resource")
public class DataflowWorkProgressUpdaterTest {
  static class TestMapTaskExecutor extends MapTaskExecutor {
    ApproximateProgress progress = null;

    public TestMapTaskExecutor(CounterSet counters) {
      super(new ArrayList<Operation>(), counters,
          new StateSampler("test", counters.getAddCounterMutator()));
    }

    @Override
    public Reader.Progress getWorkerProgress() {
      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public Reader.DynamicSplitResult requestDynamicSplit(Reader.DynamicSplitRequest splitRequest) {
      @Nullable
      ApproximateProgress progress = splitRequestToApproximateProgress(splitRequest);
      if (progress == null) {
        return null;
      }
      return new Reader.DynamicSplitResultWithPosition(
          cloudPositionToReaderPosition(progress.getPosition()));
    }

    public void setWorkerProgress(ApproximateProgress progress) {
      this.progress = progress;
    }
  }

  private static final String PROJECT_ID = "TEST_PROJECT_ID";
  private static final String JOB_ID = "TEST_JOB_ID";
  private static final String WORKER_ID = "TEST_WORKER_ID";
  private static final Long WORK_ID = 1234567890L;
  private static final String COUNTER_NAME = "test-counter-";
  private static final AggregationKind[] COUNTER_KINDS = {SUM, MAX, MIN};
  private static final Long COUNTER_VALUE1 = 12345L;
  private static final Double COUNTER_VALUE2 = Math.PI;
  private static final Long COUNTER_VALUE3 = -389L;

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Mock
  private DataflowWorker.WorkUnitClient workUnitClient;
  private CounterSet counters;
  private List<Metric<?>> metrics;
  private TestMapTaskExecutor worker;
  private WorkItem workItem;
  private DataflowWorkerHarnessOptions options;
  private DataflowWorkProgressUpdater progressUpdater;
  private long nowMillis;

  @Before
  public void initMocksAndWorkflowServiceAndWorkerAndWork() throws IOException {
    MockitoAnnotations.initMocks(this);

    options = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    options.setProject(PROJECT_ID);
    options.setJobId(JOB_ID);
    options.setWorkerId(WORKER_ID);

    metrics = new ArrayList<>();
    counters = new CounterSet();
    worker = new TestMapTaskExecutor(counters) {
      @Override
      public Collection<Metric<?>> getOutputMetrics() {
        return metrics;
      }
    };
    nowMillis = System.currentTimeMillis();

    workItem = new WorkItem();
    workItem.setProjectId(PROJECT_ID);
    workItem.setJobId(JOB_ID);
    workItem.setId(WORK_ID);
    workItem.setLeaseExpireTime(toCloudTime(new Instant(nowMillis + 1000)));
    workItem.setReportStatusInterval(toCloudDuration(Duration.millis(300)));
    workItem.setInitialReportIndex(1L);

    progressUpdater = new DataflowWorkProgressUpdater(workItem, worker, workUnitClient, options) {
      // Shorten reporting interval boundaries for faster testing.
      @Override
      protected long getMinReportingInterval() {
        return 100;
      }

      @Override
      protected long getLeaseRenewalLatencyMargin() {
        return 100;
      }
    };
  }

  // TODO: Remove sleeps from this test by using a mock sleeper.  This
  // requires a redesign of the WorkProgressUpdater to use a Sleeper and
  // not use a ScheduledThreadExecutor that relies on real time passing.
  @Test(timeout = 1000)
  public void workProgressUpdaterUpdates() throws Exception {
    when(workUnitClient.reportWorkItemStatus(any(WorkItemStatus.class)))
        .thenReturn(generateServiceState(nowMillis + 2000, 1000, null, 2L));
    setUpCounters(2);
    setUpMetrics(3);
    setUpProgress(approximateProgressAtIndex(1L));
    progressUpdater.startReportingProgress();
    // The initial update should be sent after 300.
    verify(workUnitClient, timeout(400))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withCounters(2).withMetrics(3).withProgress(
                approximateProgressAtIndex(1L))));
    progressUpdater.stopReportingProgress();
  }

  // Verifies that ReportWorkItemStatusRequest contains correct progress report
  // and actual dynamic split result.
  @Test(timeout = 5000)
  public void workProgressUpdaterAdaptsProgressInterval() throws Exception {
    // Mock that the next reportProgress call will return a response that asks
    // us to truncate the task at index 3, and the next two will not ask us to
    // truncate at all.
    when(workUnitClient.reportWorkItemStatus(any(WorkItemStatus.class)))
        .thenReturn(generateServiceState(nowMillis + 2000, 1000, positionAtIndex(3L), 2L))
        .thenReturn(generateServiceState(nowMillis + 3000, 2000, null, 3L))
        .thenReturn(generateServiceState(nowMillis + 1000, 3000, null, 4L))
        .thenReturn(generateServiceState(nowMillis + 4000, 3000, null, 5L));

    setUpCounters(3);
    setUpMetrics(2);
    setUpProgress(approximateProgressAtIndex(1L));
    progressUpdater.startReportingProgress();
    // The initial update should be sent after 300.
    verify(workUnitClient, timeout(400))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withCounters(3).withMetrics(2).withProgress(
                approximateProgressAtIndex(1L)).withReportIndex(1L)));

    setUpCounters(5);
    setUpMetrics(6);
    setUpProgress(approximateProgressAtIndex(2L));
    // The second update should be sent after one second as requested.
    verify(workUnitClient, timeout(1100))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus()
                .withCounters(5)
                .withMetrics(6)
                .withProgress(approximateProgressAtIndex(2L))
                .withDynamicSplitAtPosition(positionAtIndex(3L))
                .withReportIndex(2L)));

    // After the request is sent, reset cached dynamic split result to null.
    assertNull(progressUpdater.getDynamicSplitResultToReport());

    setUpProgress(approximateProgressAtIndex(3L));

    // The third update should be sent after 2 seconds.
    verify(workUnitClient, timeout(2100))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withProgress(approximateProgressAtIndex(3L))
                .withReportIndex(3L)));

    setUpProgress(approximateProgressAtIndex(4L));

    // The fourth update should not respect the suggested report interval.
    // It should be sent before the lease expires
    verify(workUnitClient, timeout(900))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withProgress(approximateProgressAtIndex(4L))
                .withReportIndex(4L)));

    progressUpdater.stopReportingProgress();

    assertEquals(5L, progressUpdater.getNextReportIndex());
  }

  // Verifies that a last update is sent when there is an unacknowledged split request.
  @Test(timeout = 2000)
  public void workProgressUpdaterSendsLastPendingUpdateWhenStopped() throws Exception {
    // The setup process sends one update after 300ms. Enqueue another that should be scheduled
    // 1000ms after that.
    when(workUnitClient.reportWorkItemStatus(any(WorkItemStatus.class)))
        .thenReturn(generateServiceState(nowMillis + 2000, 1000, positionAtIndex(2L), 2L));

    setUpProgress(approximateProgressAtIndex(1L));
    progressUpdater.startReportingProgress();

    // The initial update should be sent after 300 msec.
    Thread.sleep(50);
    verifyZeroInteractions(workUnitClient);

    verify(workUnitClient, timeout(350))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withProgress(approximateProgressAtIndex(1L))));

    // The second update should be scheduled to happen after one second.

    // not immediately
    verifyNoMoreInteractions(workUnitClient);

    // still not yet after 50ms
    Thread.sleep(50);
    verifyNoMoreInteractions(workUnitClient);

    // Stop the progressUpdater now, and expect the last update immediately
    progressUpdater.stopReportingProgress();

    // Verify that the last update is sent immediately and contained the latest split result.
    verify(workUnitClient)
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withDynamicSplitAtPosition(positionAtIndex(2L))));

    // And nothing happened after that.
    verifyNoMoreInteractions(workUnitClient);
  }

  private void setUpCounters(int n) {
    counters.clear();
    for (int i = 0; i < n; i++) {
      counters.add(makeCounter(i));
    }
  }

  private static Counter<?> makeCounter(int i) {
    if (i % 3 == 0) {
      return Counter.longs(COUNTER_NAME + i, COUNTER_KINDS[0])
          .addValue(COUNTER_VALUE1 + i)
          .addValue(COUNTER_VALUE1 + i * 2);
    } else if (i % 3 == 1) {
      return Counter.doubles(COUNTER_NAME + i, COUNTER_KINDS[1])
          .addValue(COUNTER_VALUE2 + i)
          .addValue(COUNTER_VALUE2 + i * 3);
    } else {
      return Counter.longs(COUNTER_NAME + i, COUNTER_KINDS[2])
          .addValue(COUNTER_VALUE3 + i)
          .addValue(COUNTER_VALUE3 + i * 5);
    }
  }

  private static Metric<?> makeMetric(int i) {
    return new DoubleMetric(String.valueOf(i), (double) i);
  }

  private void setUpMetrics(int n) {
    metrics = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      metrics.add(makeMetric(i));
    }
  }

  private void setUpProgress(ApproximateProgress progress) {
    worker.setWorkerProgress(progress);
  }

  private WorkItemServiceState generateServiceState(long leaseExpirationTimestamp,
      int progressReportIntervalMs, Position suggestedStopPosition,
      long nextReportIndex) throws IOException {
    WorkItemServiceState responseState = new WorkItemServiceState();
    responseState.setFactory(Transport.getJsonFactory());
    responseState.setLeaseExpireTime(toCloudTime(new Instant(leaseExpirationTimestamp)));
    responseState.setReportStatusInterval(
        toCloudDuration(Duration.millis(progressReportIntervalMs)));
    responseState.setNextReportIndex(nextReportIndex);

    if (suggestedStopPosition != null) {
      responseState.setSuggestedStopPoint(approximateProgressAtPosition(suggestedStopPosition));
    }

    return responseState;
  }

  private static final class ExpectedDataflowWorkItemStatus
      extends ArgumentMatcher<WorkItemStatus> {
    @Nullable
    Integer counterCount;

    @Nullable
    Integer metricCount;

    @Nullable
    ApproximateProgress expectedProgress;

    @Nullable
    Position expectedSplitPosition;

    @Nullable
    Long expectedReportIndex;

    public ExpectedDataflowWorkItemStatus withCounters(Integer counterCount) {
      this.counterCount = counterCount;
      return this;
    }

    public ExpectedDataflowWorkItemStatus withMetrics(Integer metricCount) {
      this.metricCount = metricCount;
      return this;
    }

    public ExpectedDataflowWorkItemStatus withProgress(ApproximateProgress expectedProgress) {
      this.expectedProgress = expectedProgress;
      return this;
    }

    public ExpectedDataflowWorkItemStatus withDynamicSplitAtPosition(
        Position expectedSplitPosition) {
      this.expectedSplitPosition = expectedSplitPosition;
      return this;
    }

    public ExpectedDataflowWorkItemStatus withReportIndex(Long reportIndex) {
      this.expectedReportIndex = reportIndex;
      return this;
    }

    @Override
    public void describeTo(Description description) {
      List<String> values = new ArrayList<>();
      if (this.counterCount != null) {
        for (int i = 0; i < counterCount; i++) {
          values.add(extractCounter(makeCounter(i), false).toString());
        }
      }
      if (this.metricCount != null) {
        for (int i = 0; i < metricCount; i++) {
          values.add(extractCloudMetric(makeMetric(i), WORKER_ID).toString());
        }
      }
      if (this.expectedProgress != null) {
        values.add("progress " + this.expectedProgress);
      }
      if (this.expectedSplitPosition != null) {
        values.add("split position " + this.expectedSplitPosition);
      } else {
        values.add("no split position present");
      }
      if (this.expectedReportIndex != null) {
        values.add("reportIndex " + this.expectedReportIndex);
      }
      description.appendValueList("Dataflow WorkItemStatus with ", ", ", ".", values);
    }

    @Override
    public boolean matches(Object status) {
      WorkItemStatus st = (WorkItemStatus) status;
      return matchCountersAndMetrics(st) && matchProgress(st) && matchStopPosition(st)
          && matchReportIndex(st);
    }

    private boolean matchCountersAndMetrics(WorkItemStatus status) {
      if (counterCount == null && metricCount == null) {
        return true;
      }

      List<MetricUpdate> sentUpdates = status.getMetricUpdates();

      if (counterCount + metricCount != sentUpdates.size()) {
        return false;
      }

      for (int i = 0; i < counterCount; i++) {
        if (!sentUpdates.contains(CounterTestUtils.extractCounterUpdate(makeCounter(i), false))) {
          return false;
        }
      }

      for (int i = 0; i < metricCount; i++) {
        if (!sentUpdates.contains(extractCloudMetric(makeMetric(i), WORKER_ID))) {
          return false;
        }
      }

      return true;
    }

    private boolean matchProgress(WorkItemStatus status) {
      if (expectedProgress == null) {
        return true;
      }
      ApproximateProgress progress = status.getProgress();
      return expectedProgress.equals(progress);
    }

    private boolean matchStopPosition(WorkItemStatus status) {
      Position actualStopPosition = status.getStopPosition();
      if (expectedSplitPosition == null) {
        return actualStopPosition == null;
      }
      return expectedSplitPosition.equals(actualStopPosition);
    }

    private boolean matchReportIndex(WorkItemStatus status) {
      if (expectedReportIndex == null) {
        return true;
      }
      return expectedReportIndex.equals(status.getReportIndex());
    }
  }
}
