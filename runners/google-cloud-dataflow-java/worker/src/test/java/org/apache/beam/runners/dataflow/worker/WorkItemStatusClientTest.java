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

import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.api.services.dataflow.model.NameAndKind;
import com.google.api.services.dataflow.model.Position;
import com.google.api.services.dataflow.model.Status;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.DataflowReaderPosition;
import org.apache.beam.runners.dataflow.worker.WorkerCustomSources.BoundedSourceSplit;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitResult;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.Progress;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link WorkItemStatusClient}. */
@RunWith(JUnit4.class)
public class WorkItemStatusClientTest {

  private static final String PROJECT_ID = "ProjectId";
  private static final String JOB_ID = "JobId";
  private static final long WORK_ID = 0xDEADBEEF;
  private static final Duration LEASE_DURATION = Duration.standardSeconds(10);

  private static final long INITIAL_REPORT_INDEX = 5;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private WorkUnitClient workUnitClient;
  private WorkItem workItem =
      new WorkItem()
          .setProjectId(PROJECT_ID)
          .setJobId(JOB_ID)
          .setId(WORK_ID)
          .setInitialReportIndex(INITIAL_REPORT_INDEX);
  private DataflowPipelineOptions options;

  @Mock private DataflowWorkExecutor worker;
  private BatchModeExecutionContext executionContext;
  @Captor private ArgumentCaptor<WorkItemStatus> statusCaptor;

  private WorkItemStatusClient statusClient;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    executionContext = BatchModeExecutionContext.forTesting(options, "testStage");
    statusClient = new WorkItemStatusClient(workUnitClient, workItem);
  }

  /** Verify that we can set the worker once, but not again. */
  @Test
  public void setWorker() {
    // We should be able to set the worker the first time.
    statusClient.setWorker(worker, executionContext);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("setWorker once");
    statusClient.setWorker(worker, executionContext);
  }

  /** Reporting an error before setWorker has been called should work. */
  @Test
  public void reportError() throws IOException {
    RuntimeException error = new RuntimeException();
    error.fillInStackTrace();

    statusClient.reportError(error);
    verify(workUnitClient).reportWorkItemStatus(statusCaptor.capture());
    WorkItemStatus workStatus = statusCaptor.getValue();

    assertThat(workStatus.getWorkItemId(), equalTo(Long.toString(WORK_ID)));
    assertThat(workStatus.getCompleted(), equalTo(true));
    assertThat(workStatus.getReportIndex(), equalTo(INITIAL_REPORT_INDEX));
    assertThat(workStatus.getErrors(), hasSize(1));
    Status status = workStatus.getErrors().get(0);
    assertThat(status.getCode(), equalTo(2));
    assertThat(status.getMessage(), containsString("WorkItemStatusClientTest"));
  }

  /** Reporting an error after setWorker has been called should also work. */
  @Test
  public void reportErrorAfterSetWorker() throws IOException {
    RuntimeException error = new RuntimeException();
    error.fillInStackTrace();

    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);
    statusClient.reportError(error);
    verify(workUnitClient).reportWorkItemStatus(statusCaptor.capture());
    WorkItemStatus workStatus = statusCaptor.getValue();

    assertThat(workStatus.getWorkItemId(), equalTo(Long.toString(WORK_ID)));
    assertThat(workStatus.getCompleted(), equalTo(true));
    assertThat(workStatus.getReportIndex(), equalTo(INITIAL_REPORT_INDEX));
    assertThat(workStatus.getErrors(), hasSize(1));
    Status status = workStatus.getErrors().get(0);
    assertThat(status.getCode(), equalTo(2));
    assertThat(status.getMessage(), containsString("WorkItemStatusClientTest"));
  }

  /** Reporting an out of memory error should log it in addition to the regular flow. */
  @Test
  public void reportOutOfMemoryErrorAfterSetWorker() throws IOException {
    OutOfMemoryError error = new OutOfMemoryError();
    error.fillInStackTrace();

    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);
    statusClient.reportError(error);
    verify(workUnitClient).reportWorkItemStatus(statusCaptor.capture());
    WorkItemStatus workStatus = statusCaptor.getValue();

    assertThat(workStatus.getWorkItemId(), equalTo(Long.toString(WORK_ID)));
    assertThat(workStatus.getCompleted(), equalTo(true));
    assertThat(workStatus.getReportIndex(), equalTo(INITIAL_REPORT_INDEX));
    assertThat(workStatus.getErrors(), hasSize(1));
    Status status = workStatus.getErrors().get(0);
    assertThat(status.getCode(), equalTo(2));
    assertThat(status.getMessage(), containsString("WorkItemStatusClientTest"));
    assertThat(status.getMessage(), containsString("An OutOfMemoryException occurred."));
  }

  @Test
  public void reportUpdateAfterErrorShouldFail() throws Exception {
    RuntimeException error = new RuntimeException();
    error.fillInStackTrace();
    statusClient.reportError(error);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("reportUpdate");
    statusClient.reportUpdate(null, LEASE_DURATION);
  }

  @Test
  public void reportSuccessBeforeSetWorker() throws IOException {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("setWorker");
    thrown.expectMessage("reportSuccess");
    statusClient.reportSuccess();
  }

  @Test
  public void reportSuccess() throws IOException {
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);

    statusClient.reportSuccess();
    verify(workUnitClient).reportWorkItemStatus(statusCaptor.capture());
    WorkItemStatus workStatus = statusCaptor.getValue();

    assertThat(workStatus.getWorkItemId(), equalTo(Long.toString(WORK_ID)));
    assertThat(workStatus.getCompleted(), equalTo(true));
    assertThat(workStatus.getReportIndex(), equalTo(INITIAL_REPORT_INDEX));
    assertThat(workStatus.getErrors(), nullValue());
  }

  @Test
  public void reportSuccessWithSourceOperation() throws IOException {
    SourceOperationExecutor sourceWorker = mock(SourceOperationExecutor.class);
    when(sourceWorker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(sourceWorker, executionContext);

    statusClient.reportSuccess();
    verify(workUnitClient).reportWorkItemStatus(statusCaptor.capture());
    WorkItemStatus workStatus = statusCaptor.getValue();

    assertThat(workStatus.getWorkItemId(), equalTo(Long.toString(WORK_ID)));
    assertThat(workStatus.getCompleted(), equalTo(true));
    assertThat(workStatus.getReportIndex(), equalTo(INITIAL_REPORT_INDEX));
    assertThat(workStatus.getErrors(), nullValue());
  }

  @Test
  public void reportUpdateAfterSuccess() throws Exception {
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);
    statusClient.reportSuccess();

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("reportUpdate");
    statusClient.reportUpdate(null, LEASE_DURATION);
  }

  @Test
  public void reportUpdateNullSplit() throws Exception {
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);
    statusClient.reportUpdate(null, LEASE_DURATION);
    verify(workUnitClient).reportWorkItemStatus(statusCaptor.capture());
    WorkItemStatus workStatus = statusCaptor.getValue();
    assertThat(workStatus.getCompleted(), equalTo(false));
  }

  @Test
  public void reportUpdate() throws Exception {
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);

    statusClient.reportUpdate(null, LEASE_DURATION);
    verify(workUnitClient).reportWorkItemStatus(statusCaptor.capture());
    WorkItemStatus workStatus = statusCaptor.getValue();
    assertThat(workStatus.getCompleted(), equalTo(false));
  }

  @Test
  public void reportIndexSequence() throws Exception {
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);

    when(workUnitClient.reportWorkItemStatus(isA(WorkItemStatus.class)))
        .thenReturn(new WorkItemServiceState().setNextReportIndex(INITIAL_REPORT_INDEX + 4));
    statusClient.reportUpdate(null, LEASE_DURATION);

    when(workUnitClient.reportWorkItemStatus(isA(WorkItemStatus.class)))
        .thenReturn(new WorkItemServiceState().setNextReportIndex(INITIAL_REPORT_INDEX + 8));
    statusClient.reportUpdate(null, LEASE_DURATION);

    statusClient.reportSuccess();

    verify(workUnitClient, times(3)).reportWorkItemStatus(statusCaptor.capture());

    List<WorkItemStatus> updates = statusCaptor.getAllValues();
    assertThat(updates.get(0).getReportIndex(), equalTo(INITIAL_REPORT_INDEX));
    assertThat(updates.get(1).getReportIndex(), equalTo(INITIAL_REPORT_INDEX + 4));
    assertThat(updates.get(2).getReportIndex(), equalTo(INITIAL_REPORT_INDEX + 8));
  }

  @Test
  public void populateMetricUpdatesNoStateSamplerInfo() throws Exception {
    // When executionContext.getExecutionStateTracker() returns null, we get no metric updates.
    WorkItemStatus status = new WorkItemStatus();

    BatchModeExecutionContext executionContext = mock(BatchModeExecutionContext.class);
    when(executionContext.getExecutionStateTracker()).thenReturn(null);
    statusClient.setWorker(worker, executionContext);
    statusClient.populateMetricUpdates(status);
    assertThat(status.getMetricUpdates(), empty());
  }

  @Test
  public void populateMetricUpdatesStateSamplerInfo() throws Exception {
    // When executionContext.getExecutionStateTracker() returns non-null, we get one metric update.
    WorkItemStatus status = new WorkItemStatus();

    BatchModeExecutionContext executionContext = mock(BatchModeExecutionContext.class);
    ExecutionStateTracker executionStateTracker = mock(ExecutionStateTracker.class);
    ExecutionState executionState = mock(ExecutionState.class);
    when(executionState.getDescription()).thenReturn("stageName-systemName-some-state");
    when(executionContext.getExecutionStateTracker()).thenReturn(executionStateTracker);
    when(executionStateTracker.getMillisSinceLastTransition()).thenReturn(20L);
    when(executionStateTracker.getNumTransitions()).thenReturn(10L);
    when(executionStateTracker.getCurrentState()).thenReturn(executionState);
    statusClient.setWorker(worker, executionContext);
    statusClient.populateMetricUpdates(status);

    assertThat(status.getMetricUpdates(), hasSize(1));
    MetricUpdate update = status.getMetricUpdates().get(0);
    assertThat(update.getName().getName(), equalTo("state-sampler"));
    assertThat(update.getKind(), equalTo("internal"));
    Map<String, Object> samplerMetrics = (Map<String, Object>) update.getInternal();
    assertThat(samplerMetrics, hasEntry("last-state-name", "stageName-systemName-some-state"));
    assertThat(samplerMetrics, hasEntry("num-transitions", 10L));
    assertThat(samplerMetrics, hasEntry("last-state-duration-ms", 20L));
  }

  @Test
  public void populateCounterUpdatesEmptyOutputCounters() throws Exception {
    // When worker.getOutputCounters == null, there should be no counters.
    WorkItemStatus status = new WorkItemStatus();
    statusClient.setWorker(worker, executionContext);
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.populateCounterUpdates(status);
    assertThat(status.getCounterUpdates(), hasSize(0));
  }

  @Test
  /** Validates that an "internal" Counter is reported. */
  public void populateCounterUpdatesWithOutputCounters() throws Exception {
    final CounterUpdate counter =
        new CounterUpdate()
            .setNameAndKind(new NameAndKind().setName("some-counter").setKind(Kind.SUM.toString()))
            .setCumulative(true)
            .setInteger(DataflowCounterUpdateExtractor.longToSplitInt(42));

    CounterSet counterSet = new CounterSet();
    counterSet.intSum(CounterName.named("some-counter")).addValue(42);

    WorkItemStatus status = new WorkItemStatus();
    when(worker.getOutputCounters()).thenReturn(counterSet);
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);
    statusClient.populateCounterUpdates(status);

    assertThat(status.getCounterUpdates(), containsInAnyOrder(counter));
  }

  /** Validates that Beam Metrics and "internal" Counters are merged in the update. */
  @Test
  public void populateCounterUpdatesWithMetricsAndCounters() throws Exception {
    final CounterUpdate expectedCounter =
        new CounterUpdate()
            .setNameAndKind(new NameAndKind().setName("some-counter").setKind(Kind.SUM.toString()))
            .setCumulative(true)
            .setInteger(DataflowCounterUpdateExtractor.longToSplitInt(42));

    CounterSet counterSet = new CounterSet();
    counterSet.intSum(CounterName.named("some-counter")).addValue(42);

    final CounterUpdate expectedMetric =
        new CounterUpdate()
            .setStructuredNameAndMetadata(
                new CounterStructuredNameAndMetadata()
                    .setName(
                        new CounterStructuredName()
                            .setOrigin("USER")
                            .setOriginNamespace("namespace")
                            .setName("some-counter")
                            .setOriginalStepName("step"))
                    .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
            .setCumulative(true)
            .setInteger(DataflowCounterUpdateExtractor.longToSplitInt(42));

    MetricsContainerImpl metricsContainer = new MetricsContainerImpl("step");
    BatchModeExecutionContext context = mock(BatchModeExecutionContext.class);
    when(context.extractMetricUpdates(anyBoolean())).thenReturn(ImmutableList.of(expectedMetric));
    when(context.extractMsecCounters(anyBoolean())).thenReturn(Collections.emptyList());

    CounterCell counter =
        metricsContainer.getCounter(MetricName.named("namespace", "some-counter"));
    counter.inc(1);
    counter.inc(41);
    counter.inc(1);
    counter.inc(-1);

    WorkItemStatus status = new WorkItemStatus();

    when(worker.getOutputCounters()).thenReturn(counterSet);
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, context);
    statusClient.populateCounterUpdates(status);

    assertThat(status.getCounterUpdates(), containsInAnyOrder(expectedCounter, expectedMetric));
  }

  @Test
  public void populateCounterUpdatesWithMsecCounter() throws Exception {
    final CounterUpdate expectedMsec =
        new CounterUpdate()
            .setStructuredNameAndMetadata(
                new CounterStructuredNameAndMetadata()
                    .setName(
                        new CounterStructuredName()
                            .setOrigin("SYSTEM")
                            .setName("start-msecs")
                            .setOriginalStepName("step"))
                    .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
            .setCumulative(true)
            .setInteger(DataflowCounterUpdateExtractor.longToSplitInt(42));

    BatchModeExecutionContext context = mock(BatchModeExecutionContext.class);
    when(context.extractMetricUpdates(anyBoolean())).thenReturn(ImmutableList.of());
    when(context.extractMsecCounters(anyBoolean())).thenReturn(ImmutableList.of(expectedMsec));

    WorkItemStatus status = new WorkItemStatus();

    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, context);
    statusClient.populateCounterUpdates(status);

    assertThat(status.getCounterUpdates(), containsInAnyOrder(expectedMsec));
  }

  @Test
  public void populateProgressNull() throws Exception {
    WorkItemStatus status = new WorkItemStatus();
    statusClient.setWorker(worker, executionContext);
    statusClient.populateProgress(status);

    assertThat(status.getReportedProgress(), nullValue());
  }

  @Test
  public void populateProgress() throws Exception {
    WorkItemStatus status = new WorkItemStatus();
    Progress progress =
        cloudProgressToReaderProgress(ReaderTestUtils.approximateProgressAtIndex(42L));
    when(worker.getWorkerProgress()).thenReturn(progress);
    statusClient.setWorker(worker, executionContext);
    statusClient.populateProgress(status);

    assertThat(
        status.getReportedProgress(), equalTo(ReaderTestUtils.approximateProgressAtIndex(42L)));
  }

  @Test
  public void populateSplitResultNativeReader() throws Exception {
    WorkItemStatus status = new WorkItemStatus();
    statusClient.setWorker(worker, executionContext);
    Position position = ReaderTestUtils.positionAtIndex(42L);
    DynamicSplitResult result =
        new NativeReader.DynamicSplitResultWithPosition(new DataflowReaderPosition(position));
    statusClient.populateSplitResult(status, result);

    assertThat(status.getStopPosition(), equalTo(position));
    assertThat(status.getDynamicSourceSplit(), nullValue());
  }

  @Test
  public void populateSplitResultCustomReader() throws Exception {
    WorkItemStatus status = new WorkItemStatus();
    statusClient.setWorker(worker, executionContext);
    BoundedSource<Integer> primary = new DummyBoundedSource(5);
    BoundedSource<Integer> residual = new DummyBoundedSource(10);

    BoundedSourceSplit<Integer> split = new BoundedSourceSplit<>(primary, residual);
    statusClient.populateSplitResult(status, split);

    assertThat(status.getDynamicSourceSplit(), equalTo(WorkerCustomSources.toSourceSplit(split)));
    assertThat(status.getStopPosition(), nullValue());
  }

  @Test
  public void populateSplitResultNull() throws Exception {
    WorkItemStatus status = new WorkItemStatus();
    statusClient.setWorker(worker, executionContext);
    statusClient.populateSplitResult(status, null);

    assertThat(status.getDynamicSourceSplit(), nullValue());
    assertThat(status.getStopPosition(), nullValue());
  }

  @Test
  public void reportUpdateBeforeSetWorker() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("setWorker");
    thrown.expectMessage("reportUpdate");
    statusClient.reportUpdate(null, null);
  }

  private static class DummyBoundedSource extends BoundedSource<Integer> {

    private final int number;

    public DummyBoundedSource(int number) {
      this.number = number;
    }

    @Override
    public List<? extends BoundedSource<Integer>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void validate() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Coder<Integer> getDefaultOutputCoder() {
      return null;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(number);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof DummyBoundedSource)) {
        return false;
      }
      return number == ((DummyBoundedSource) obj).number;
    }
  }
}
