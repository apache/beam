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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.NameAndKind;
import com.google.api.services.dataflow.model.SplitInt64;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Multi-threaded concurrency tests verifying race conditions and invariants between progress
 * reporting, lightweight lease renewal, and final completion in {@link WorkItemStatusClient}.
 */
@RunWith(JUnit4.class)
public class LeaseRenewalRaceTest {

  private static final Duration LEASE = Duration.standardSeconds(10L);

  private WorkUnitClient workUnitClient;
  private DataflowWorkExecutor worker;
  private BatchModeExecutionContext executionContext;
  private WorkItemStatusClient statusClient;
  private List<WorkItemStatus> sent;

  @Before
  public void setUp() throws Exception {
    workUnitClient = mock(WorkUnitClient.class);
    worker = mock(DataflowWorkExecutor.class);
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    executionContext = BatchModeExecutionContext.forTesting(options, "testStage");

    WorkItem workItem =
        new WorkItem().setProjectId("p").setJobId("j").setId(1L).setInitialReportIndex(5L);

    statusClient = new WorkItemStatusClient(workUnitClient, workItem);
    sent = new ArrayList<>();

    when(workUnitClient.reportWorkItemStatus(any(WorkItemStatus.class)))
        .thenAnswer(
            invocation -> {
              WorkItemStatus status = invocation.getArgument(0);
              sent.add(status);
              return new WorkItemServiceState().setNextReportIndex(status.getReportIndex() + 1);
            });
  }

  private static CounterUpdate namedCounter(String name, long value) {
    return new CounterUpdate()
        .setNameAndKind(new NameAndKind().setName(name).setKind("SUM"))
        .setInteger(new SplitInt64().setLowBits(value));
  }

  /**
   * Verifies that when a progress update is called after or racing with {@code reportSuccess()}, it
   * takes the graceful skip path and returns {@code null} instead of throwing {@link
   * IllegalStateException}.
   */
  @Test
  public void inFlightProgressUpdateThrowsAfterFinalState() throws Exception {
    when(worker.extractMetricUpdates()).thenReturn(Collections.emptyList());
    statusClient.setWorker(worker, executionContext);
    statusClient.reportSuccess();
    assertTrue(statusClient.isFinalStateSent());

    WorkItemServiceState state = null;
    Throwable thrown = null;
    try {
      state = statusClient.reportUpdate(null, LEASE);
    } catch (Throwable t) {
      thrown = t;
    }

    assertNull("reportUpdate should take the graceful skip path, not throw", thrown);
    assertNull("reportUpdate should return null after final state", state);
  }

  /**
   * Verifies that metric updates extracted by a concurrent progress update thread are never
   * silently discarded when racing with {@code reportSuccess()}.
   *
   * <p>Because {@code reportUpdate()} and {@code reportSuccess()} synchronize on {@link
   * WorkItemStatusClient}, metric extraction, status RPC execution, and metric commits are atomic.
   * An in-flight update cannot have its drained metrics dropped by a racing completion.
   */
  @Test
  public void drainedMetricUpdatesSurviveConcurrentReportSuccess() throws Exception {
    CountDownLatch drained = new CountDownLatch(1);
    AtomicReference<Throwable> progressFailure = new AtomicReference<>();
    List<CounterUpdate> pending = new ArrayList<>();
    pending.add(namedCounter("user-metric", 1L));

    when(worker.extractMetricUpdates())
        .thenAnswer(
            invocation -> {
              synchronized (pending) {
                List<CounterUpdate> copy = new ArrayList<>(pending);
                pending.clear();
                if (!copy.isEmpty()) {
                  drained.countDown();
                }
                return copy;
              }
            });

    statusClient.setWorker(worker, executionContext);

    Thread progressThread =
        new Thread(
            () -> {
              try {
                statusClient.reportUpdate(null, LEASE);
              } catch (Throwable t) {
                progressFailure.set(t);
              }
            },
            "progress-updater");
    progressThread.start();

    assertTrue("progress thread should have drained", drained.await(5, TimeUnit.SECONDS));

    statusClient.reportSuccess();
    progressThread.join(5000);

    boolean anyStatusCarriesTheMetric =
        sent.stream()
            .filter(s -> s.getCounterUpdates() != null)
            .flatMap(s -> s.getCounterUpdates().stream())
            .anyMatch(
                c ->
                    c.getNameAndKind() != null
                        && "user-metric".equals(c.getNameAndKind().getName()));

    assertTrue(
        "drained metric update was silently dropped -- never reached the service",
        anyStatusCarriesTheMetric);
  }

  /** Verifies that lightweight lease pings succeed and do not extract or commit metric updates. */
  @Test
  public void reportLeasePingDoesNotExtractOrDropMetrics() throws Exception {
    when(worker.extractMetricUpdates())
        .thenReturn(Collections.singletonList(namedCounter("pending-metric", 42L)));
    statusClient.setWorker(worker, executionContext);

    WorkItemServiceState pingState = statusClient.reportLeasePing(LEASE);
    assertTrue(pingState != null);

    // Verify ping status had no metric or counter updates
    assertEquals(1, sent.size());
    WorkItemStatus pingStatus = sent.get(0);
    assertNull("Lease ping must not include counter updates", pingStatus.getCounterUpdates());
    assertNull("Lease ping must not include metric updates", pingStatus.getMetricUpdates());

    // Verify pending metrics are still present and sent by reportSuccess
    statusClient.reportSuccess();
    assertEquals(2, sent.size());
    WorkItemStatus successStatus = sent.get(1);
    boolean successCarriesMetric =
        successStatus.getCounterUpdates() != null
            && successStatus.getCounterUpdates().stream()
                .anyMatch(
                    c ->
                        c.getNameAndKind() != null
                            && "pending-metric".equals(c.getNameAndKind().getName()));
    assertTrue("reportSuccess must report the pending metric", successCarriesMetric);
  }
}
