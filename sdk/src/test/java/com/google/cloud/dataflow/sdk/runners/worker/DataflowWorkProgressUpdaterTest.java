/*
 * Copyright (C) 2014 Google Inc.
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
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.forkRequestToApproximateProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toCloudPosition;
import static com.google.cloud.dataflow.sdk.util.CloudCounterUtils.extractCounter;
import static com.google.cloud.dataflow.sdk.util.CloudMetricUtils.extractCloudMetric;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudDuration;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudTime;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MAX;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SET;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
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
    public Reader.ForkResult requestFork(Reader.ForkRequest forkRequest) {
      @Nullable
      ApproximateProgress progress = forkRequestToApproximateProgress(forkRequest);
      if (progress == null) {
        return null;
      }
      return new Reader.ForkResultWithPosition(
          cloudPositionToReaderPosition(progress.getPosition()));
    }

    public void setWorkerProgress(ApproximateProgress progress) {
      this.progress = progress;
    }
  }

  static {
    // To shorten wait times during testing.
    System.setProperty("minimum_worker_update_interval_millis", "100");
    System.setProperty("worker_lease_renewal_latency_margin", "100");
  }

  private static final String PROJECT_ID = "TEST_PROJECT_ID";
  private static final String JOB_ID = "TEST_JOB_ID";
  private static final String WORKER_ID = "TEST_WORKER_ID";
  private static final Long WORK_ID = 1234567890L;
  private static final String COUNTER_NAME = "test-counter-";
  private static final AggregationKind[] COUNTER_KINDS = {SUM, MAX, SET};
  private static final Long COUNTER_VALUE1 = 12345L;
  private static final Double COUNTER_VALUE2 = Math.PI;
  private static final String COUNTER_VALUE3 = "value";

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

    options = PipelineOptionsFactory.createFromSystemProperties();
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
    workItem.setReportStatusInterval(toCloudDuration(Duration.millis(500)));

    progressUpdater = new DataflowWorkProgressUpdater(workItem, worker, workUnitClient, options);
  }

  // TODO: Remove sleeps from this test by using a mock sleeper.  This
  // requires a redesign of the WorkProgressUpdater to use a Sleeper and
  // not use a ScheduledThreadExecutor which relies on real time passing.
  @Test(timeout = 2000)
  public void workProgressUpdaterUpdates() throws Exception {
    when(workUnitClient.reportWorkItemStatus(any(WorkItemStatus.class)))
        .thenReturn(generateServiceState(nowMillis + 2000, 1000, null));
    setUpCounters(2);
    setUpMetrics(3);
    setUpProgress(approximateProgressAtIndex(1L));
    progressUpdater.startReportingProgress();
    // The initial update should be sent after leaseRemainingTime / 2.
    verify(workUnitClient, timeout(600))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withCounters(2).withMetrics(3).withProgress(
                approximateProgressAtIndex(1L))));
    progressUpdater.stopReportingProgress();
  }

  // Verifies that ReportWorkItemStatusRequest contains correct progress report
  // and actual fork result.
  @Test(timeout = 5000)
  public void workProgressUpdaterAdaptsProgressInterval() throws Exception {
    // Mock that the next reportProgress call will return a response that asks
    // us to truncate the task at index 3, and the next two will not ask us to
    // truncate at all.
    when(workUnitClient.reportWorkItemStatus(any(WorkItemStatus.class)))
        .thenReturn(generateServiceState(nowMillis + 2000, 1000, positionAtIndex(3L)))
        .thenReturn(generateServiceState(nowMillis + 3000, 2000, null))
        .thenReturn(generateServiceState(nowMillis + 4000, 3000, null));

    setUpCounters(3);
    setUpMetrics(2);
    setUpProgress(approximateProgressAtIndex(1L));
    progressUpdater.startReportingProgress();
    // The initial update should be sent after
    // leaseRemainingTime (1000) / 2 = 500.
    verify(workUnitClient, timeout(600))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withCounters(3).withMetrics(2).withProgress(
                approximateProgressAtIndex(1L))));

    setUpCounters(5);
    setUpMetrics(6);
    setUpProgress(approximateProgressAtIndex(2L));
    // The second update should be sent after one second (2000 / 2).
    verify(workUnitClient, timeout(1100))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus()
                .withCounters(5)
                .withMetrics(6)
                .withProgress(approximateProgressAtIndex(2L))
                .withForkAtPosition(positionAtIndex(3L))));

    // After the request is sent, reset cached fork result to null.
    assertNull(progressUpdater.getForkResultToReport());

    setUpProgress(approximateProgressAtIndex(3L));

    // The third update should be sent after one and half seconds (3000 / 2).
    verify(workUnitClient, timeout(1600))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withProgress(approximateProgressAtIndex(3L))));

    progressUpdater.stopReportingProgress();
  }

  // Verifies that a last update is sent when there is an unacknowledged split request.
  @Test(timeout = 3000)
  public void workProgressUpdaterLastUpdate() throws Exception {
    when(workUnitClient.reportWorkItemStatus(any(WorkItemStatus.class)))
        .thenReturn(generateServiceState(nowMillis + 2000, 1000, positionAtIndex(2L)))
        .thenReturn(generateServiceState(nowMillis + 3000, 2000, null));

    setUpProgress(approximateProgressAtIndex(1L));
    progressUpdater.startReportingProgress();
    // The initial update should be sent after leaseRemainingTime / 2 = 500 msec.
    Thread.sleep(600);
    verify(workUnitClient, timeout(200))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withProgress(approximateProgressAtIndex(1L))));

    // The first update should include the new fork result..
    // Verify that the progressUpdater has recorded it.
    Reader.ForkResultWithPosition forkResult =
        (Reader.ForkResultWithPosition) progressUpdater.getForkResultToReport();
    assertEquals(positionAtIndex(2L), toCloudPosition(forkResult.getAcceptedPosition()));

    setUpProgress(approximateProgressAtIndex(2L));
    // The second update should be sent after one second (2000 / 2).

    // Not enough time for an update so the latest fork result is not acknowledged.
    Thread.sleep(200);

    // Check that the progressUpdater still has a pending fork result to send
    forkResult = (Reader.ForkResultWithPosition) progressUpdater.getForkResultToReport();
    assertEquals(positionAtIndex(2L), toCloudPosition(forkResult.getAcceptedPosition()));

    progressUpdater.stopReportingProgress(); // Should send the last update.
    // Check that the progressUpdater is done with reporting its latest fork result.
    assertNull(progressUpdater.getForkResultToReport());

    // Verify that the last update contained the latest fork result.
    verify(workUnitClient, timeout(1000))
        .reportWorkItemStatus(argThat(
            new ExpectedDataflowWorkItemStatus().withForkAtPosition(positionAtIndex(2L))));
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
      return Counter.strings(COUNTER_NAME + i, COUNTER_KINDS[2])
          .addValue(COUNTER_VALUE3 + i)
          .addValue(COUNTER_NAME + i * 5);
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
      int progressReportIntervalMs, Position suggestedStopPosition) throws IOException {
    WorkItemServiceState responseState = new WorkItemServiceState();
    responseState.setFactory(Transport.getJsonFactory());
    responseState.setLeaseExpireTime(toCloudTime(new Instant(leaseExpirationTimestamp)));
    responseState.setReportStatusInterval(
        toCloudDuration(Duration.millis(progressReportIntervalMs)));

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
    Position expectedForkPosition;

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

    public ExpectedDataflowWorkItemStatus withForkAtPosition(Position expectedForkPosition) {
      this.expectedForkPosition = expectedForkPosition;
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
      if (this.expectedForkPosition != null) {
        values.add("fork position " + this.expectedForkPosition);
      } else {
        values.add("no fork position present");
      }
      description.appendValueList("Dataflow WorkItemStatus with ", ", ", ".", values);
    }

    @Override
    public boolean matches(Object status) {
      WorkItemStatus st = (WorkItemStatus) status;
      return matchCountersAndMetrics(st) && matchProgress(st) && matchStopPosition(st);
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
      if (expectedForkPosition == null) {
        return actualStopPosition == null;
      }
      return expectedForkPosition.equals(actualStopPosition);
    }
  }
}
