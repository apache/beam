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
package org.apache.beam.fn.harness.control;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionState;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTrackerStatus;
import org.apache.beam.runners.core.metrics.MonitoringInfoEncodings;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Tests for {@link ExecutionStateSampler}. */
@RunWith(JUnit4.class)
public class ExecutionStateSamplerTest {

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(ExecutionStateSampler.class);

  @Test
  public void testSamplingProducesCorrectFinalResults() throws Exception {
    MillisProvider clock = mock(MillisProvider.class);
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(
            PipelineOptionsFactory.fromArgs("--experiments=state_sampling_period_millis=10")
                .create(),
            clock);
    ExecutionStateTracker tracker1 = sampler.create();
    ExecutionState state1 =
        tracker1.create("shortId1", "ptransformId1", "ptransformIdName1", "process");

    ExecutionStateTracker tracker2 = sampler.create();
    ExecutionState state2 =
        tracker2.create("shortId2", "ptransformId2", "ptransformIdName2", "process");

    CountDownLatch waitTillActive = new CountDownLatch(1);
    CountDownLatch waitTillIntermediateReport = new CountDownLatch(1);
    CountDownLatch waitTillStatesDeactivated = new CountDownLatch(1);
    CountDownLatch waitForSamples = new CountDownLatch(1);
    CountDownLatch waitForMoreSamples = new CountDownLatch(1);
    CountDownLatch waitForEvenMoreSamples = new CountDownLatch(1);
    Thread testThread = Thread.currentThread();
    Mockito.when(clock.getMillis())
        .thenAnswer(
            new Answer<Long>() {
              private long currentTime;

              @Override
              public Long answer(InvocationOnMock invocation) throws Throwable {
                if (Thread.currentThread().equals(testThread)) {
                  return 1L;
                } else {
                  // Block the state sampling thread till the state is active
                  // and unblock the state transition once a certain number of samples
                  // have been taken.
                  // Block the state sampling thread till the state is active
                  // and unblock the state transition once a certain number of samples
                  // have been taken.
                  if (currentTime < 1000L) {
                    waitTillActive.await();
                    currentTime += 100L;
                  } else if (currentTime < 1500L) {
                    waitForSamples.countDown();
                    waitTillIntermediateReport.await();
                    currentTime += 100L;
                  } else if (currentTime == 1500L) {
                    waitForMoreSamples.countDown();
                    waitTillStatesDeactivated.await();
                    currentTime = 1600L;
                  } else if (currentTime == 1600L) {
                    waitForEvenMoreSamples.countDown();
                  }
                  return currentTime;
                }
              }
            });

    // No active PTransform
    assertNull(tracker1.getCurrentThreadsPTransformId());
    assertNull(tracker2.getCurrentThreadsPTransformId());

    // No tracked thread
    assertNull(tracker1.getStatus());
    assertNull(tracker2.getStatus());

    tracker1.start("bundleId1");
    tracker2.start("bundleId2");

    state1.activate();
    state2.activate();

    // Check that the current threads PTransform id is available
    assertEquals("ptransformId1", tracker1.getCurrentThreadsPTransformId());
    assertEquals("ptransformId2", tracker2.getCurrentThreadsPTransformId());

    // Check that the status returns a value as soon as it is activated.
    ExecutionStateTrackerStatus activeBundleStatus1 = tracker1.getStatus();
    ExecutionStateTrackerStatus activeBundleStatus2 = tracker2.getStatus();
    assertEquals("ptransformId1", activeBundleStatus1.getPTransformId());
    assertEquals("ptransformId2", activeBundleStatus2.getPTransformId());
    assertEquals("ptransformIdName1", activeBundleStatus1.getPTransformUniqueName());
    assertEquals("ptransformIdName2", activeBundleStatus2.getPTransformUniqueName());
    assertEquals(Thread.currentThread(), activeBundleStatus1.getTrackedThread());
    assertEquals(Thread.currentThread(), activeBundleStatus2.getTrackedThread());
    assertThat(
        activeBundleStatus1.getLastTransitionTimeMillis(),
        // Because we are using lazySet, we aren't guaranteed to see the latest value
        // but we should definitely be seeing a value that isn't zero
        equalTo(1L));
    assertThat(
        activeBundleStatus2.getLastTransitionTimeMillis(),
        // Internal implementation has this be equal to the second value we return (2 * 100L)
        equalTo(1L));

    waitTillActive.countDown();
    waitForSamples.await();

    // Check that the current threads PTransform id is available
    assertEquals("ptransformId1", tracker1.getCurrentThreadsPTransformId());
    assertEquals("ptransformId2", tracker2.getCurrentThreadsPTransformId());

    // Check that we get additional data about the active PTransform.
    ExecutionStateTrackerStatus activeStateStatus1 = tracker1.getStatus();
    ExecutionStateTrackerStatus activeStateStatus2 = tracker2.getStatus();
    assertEquals("ptransformId1", activeStateStatus1.getPTransformId());
    assertEquals("ptransformId2", activeStateStatus2.getPTransformId());
    assertEquals("ptransformIdName1", activeStateStatus1.getPTransformUniqueName());
    assertEquals("ptransformIdName2", activeStateStatus2.getPTransformUniqueName());
    assertEquals(Thread.currentThread(), activeStateStatus1.getTrackedThread());
    assertEquals(Thread.currentThread(), activeStateStatus2.getTrackedThread());
    assertThat(
        activeStateStatus1.getLastTransitionTimeMillis(),
        greaterThan(activeBundleStatus1.getLastTransitionTimeMillis()));
    assertThat(
        activeStateStatus2.getLastTransitionTimeMillis(),
        greaterThan(activeBundleStatus2.getLastTransitionTimeMillis()));

    // Validate intermediate monitoring data
    Map<String, ByteString> intermediateResults1 = new HashMap<>();
    Map<String, ByteString> intermediateResults2 = new HashMap<>();
    tracker1.updateIntermediateMonitoringData(intermediateResults1);
    tracker2.updateIntermediateMonitoringData(intermediateResults2);
    assertThat(
        MonitoringInfoEncodings.decodeInt64Counter(intermediateResults1.get("shortId1")),
        // Because we are using lazySet, we aren't guaranteed to see the latest value.
        // The CountDownLatch ensures that we will see either the prior value or
        // the latest value.
        anyOf(equalTo(900L), equalTo(1000L)));
    assertThat(
        MonitoringInfoEncodings.decodeInt64Counter(intermediateResults2.get("shortId2")),
        // Because we are using lazySet, we aren't guaranteed to see the latest value.
        // The CountDownLatch ensures that we will see either the prior value or
        // the latest value.
        anyOf(equalTo(900L), equalTo(1000L)));

    waitTillIntermediateReport.countDown();
    waitForMoreSamples.await();
    state1.deactivate();
    state2.deactivate();

    waitTillStatesDeactivated.countDown();
    waitForEvenMoreSamples.await();

    // Check that the current threads PTransform id is not available
    assertNull(tracker1.getCurrentThreadsPTransformId());
    assertNull(tracker2.getCurrentThreadsPTransformId());

    // Check the status once the states are deactivated but the bundle is still active
    ExecutionStateTrackerStatus inactiveStateStatus1 = tracker1.getStatus();
    ExecutionStateTrackerStatus inactiveStateStatus2 = tracker2.getStatus();
    assertNull(inactiveStateStatus1.getPTransformId());
    assertNull(inactiveStateStatus2.getPTransformId());
    assertNull(inactiveStateStatus1.getPTransformUniqueName());
    assertNull(inactiveStateStatus2.getPTransformUniqueName());
    assertEquals(Thread.currentThread(), inactiveStateStatus1.getTrackedThread());
    assertEquals(Thread.currentThread(), inactiveStateStatus2.getTrackedThread());
    assertThat(
        inactiveStateStatus1.getLastTransitionTimeMillis(),
        greaterThan(activeStateStatus1.getLastTransitionTimeMillis()));
    assertThat(
        inactiveStateStatus2.getLastTransitionTimeMillis(),
        greaterThan(activeStateStatus1.getLastTransitionTimeMillis()));

    // Validate the final monitoring data
    Map<String, ByteString> finalResults1 = new HashMap<>();
    Map<String, ByteString> finalResults2 = new HashMap<>();
    tracker1.updateFinalMonitoringData(finalResults1);
    tracker2.updateFinalMonitoringData(finalResults2);
    assertThat(
        MonitoringInfoEncodings.decodeInt64Counter(finalResults1.get("shortId1")),
        // Because we are using lazySet, we aren't guaranteed to see the latest value.
        // The CountDownLatch ensures that we will see either the prior value or
        // the latest value.
        anyOf(equalTo(1400L), equalTo(1500L)));
    assertThat(
        MonitoringInfoEncodings.decodeInt64Counter(finalResults2.get("shortId2")),
        // Because we are using lazySet, we aren't guaranteed to see the latest value.
        // The CountDownLatch ensures that we will see either the prior value or
        // the latest value.
        anyOf(equalTo(1400L), equalTo(1500L)));

    tracker1.reset();
    tracker2.reset();

    // Shouldn't have a status or pt ransform id returned since there is no active bundle.
    assertNull(tracker1.getCurrentThreadsPTransformId());
    assertNull(tracker2.getCurrentThreadsPTransformId());
    assertNull(tracker1.getStatus());
    assertNull(tracker2.getStatus());

    sampler.stop();
    expectedLogs.verifyNotLogged("Operation ongoing");
  }

  @Test
  public void testSamplingDoesntReportDuplicateFinalResults() throws Exception {
    MillisProvider clock = mock(MillisProvider.class);
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(
            PipelineOptionsFactory.fromArgs("--experiments=state_sampling_period_millis=10")
                .create(),
            clock);
    ExecutionStateTracker tracker1 = sampler.create();
    ExecutionState state1 =
        tracker1.create("shortId1", "ptransformId1", "ptransformIdName1", "process");

    ExecutionStateTracker tracker2 = sampler.create();
    ExecutionState state2 =
        tracker2.create("shortId2", "ptransformId2", "ptransformIdName2", "process");

    CountDownLatch waitTillActive = new CountDownLatch(1);
    CountDownLatch waitForSamples = new CountDownLatch(1);
    Thread testThread = Thread.currentThread();
    Mockito.when(clock.getMillis())
        .thenAnswer(
            new Answer<Long>() {
              private long currentTime;

              @Override
              public Long answer(InvocationOnMock invocation) throws Throwable {
                if (Thread.currentThread().equals(testThread)) {
                  return 0L;
                } else {
                  // Block the state sampling thread till the state is active
                  // and unblock the state transition once a certain number of samples
                  // have been taken.
                  waitTillActive.await();
                  if (currentTime < 1000L) {
                    currentTime += 100L;
                  } else {
                    waitForSamples.countDown();
                  }
                  return currentTime;
                }
              }
            });

    tracker1.start("bundleId1");
    tracker2.start("bundleId2");

    state1.activate();
    state2.activate();
    waitTillActive.countDown();
    waitForSamples.await();
    state1.deactivate();
    state2.deactivate();

    Map<String, ByteString> intermediateResults1 = new HashMap<>();
    Map<String, ByteString> intermediateResults2 = new HashMap<>();
    tracker1.updateIntermediateMonitoringData(intermediateResults1);
    tracker2.updateIntermediateMonitoringData(intermediateResults2);
    assertThat(
        MonitoringInfoEncodings.decodeInt64Counter(intermediateResults1.get("shortId1")),
        // Because we are using lazySet, we aren't guaranteed to see the latest value.
        // The CountDownLatch ensures that we will see either the prior value or
        // the latest value.
        anyOf(equalTo(900L), equalTo(1000L)));
    assertThat(
        MonitoringInfoEncodings.decodeInt64Counter(intermediateResults2.get("shortId2")),
        // Because we are using lazySet, we aren't guaranteed to see the latest value.
        // The CountDownLatch ensures that we will see either the prior value or
        // the latest value.
        anyOf(equalTo(900L), equalTo(1000L)));

    state1.deactivate();
    state2.deactivate();

    Map<String, ByteString> finalResults1 = new HashMap<>();
    Map<String, ByteString> finalResults2 = new HashMap<>();
    tracker1.updateFinalMonitoringData(finalResults1);
    tracker2.updateFinalMonitoringData(finalResults2);

    assertTrue(finalResults1.isEmpty());
    assertTrue(finalResults2.isEmpty());

    tracker1.reset();
    tracker2.reset();

    sampler.stop();
    expectedLogs.verifyNotLogged("Operation ongoing");
  }

  @Test
  public void testTrackerReuse() throws Exception {
    MillisProvider clock = mock(MillisProvider.class);
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(
            PipelineOptionsFactory.fromArgs("--experiments=state_sampling_period_millis=10")
                .create(),
            clock);
    ExecutionStateTracker tracker = sampler.create();
    ExecutionState state = tracker.create("shortId", "ptransformId", "ptransformIdName", "process");

    CountDownLatch waitTillActive = new CountDownLatch(1);
    CountDownLatch waitTillSecondStateActive = new CountDownLatch(1);
    CountDownLatch waitForSamples = new CountDownLatch(1);
    CountDownLatch waitForMoreSamples = new CountDownLatch(1);
    Thread testThread = Thread.currentThread();
    Mockito.when(clock.getMillis())
        .thenAnswer(
            new Answer<Long>() {
              private long currentTime;

              @Override
              public Long answer(InvocationOnMock invocation) throws Throwable {
                if (Thread.currentThread().equals(testThread)) {
                  return 0L;
                } else {
                  // Block the state sampling thread till the state is active
                  // and unblock the state transition once a certain number of samples
                  // have been taken.
                  if (currentTime < 1000L) {
                    waitTillActive.await();
                    currentTime += 100L;
                  } else if (currentTime < 1500L) {
                    waitForSamples.countDown();
                    waitTillSecondStateActive.await();
                    currentTime += 100L;
                  } else {
                    waitForMoreSamples.countDown();
                  }
                  return currentTime;
                }
              }
            });

    {
      tracker.start("bundleId1");
      state.activate();
      waitTillActive.countDown();
      waitForSamples.await();
      state.deactivate();
      Map<String, ByteString> finalResults = new HashMap<>();
      tracker.updateFinalMonitoringData(finalResults);
      assertThat(
          MonitoringInfoEncodings.decodeInt64Counter(finalResults.get("shortId")),
          // Because we are using lazySet, we aren't guaranteed to see the latest value.
          // The CountDownLatch ensures that we will see either the prior value or
          // the latest value.
          anyOf(equalTo(900L), equalTo(1000L)));
      tracker.reset();
    }

    {
      tracker.start("bundleId2");
      state.activate();
      waitTillSecondStateActive.countDown();
      waitForMoreSamples.await();
      state.deactivate();
      Map<String, ByteString> finalResults = new HashMap<>();
      tracker.updateFinalMonitoringData(finalResults);
      assertThat(
          MonitoringInfoEncodings.decodeInt64Counter(finalResults.get("shortId")),
          // Because we are using lazySet, we aren't guaranteed to see the latest value.
          // The CountDownLatch ensures that we will see either the prior value or
          // the latest value.
          anyOf(equalTo(400L), equalTo(500L)));
      tracker.reset();
    }

    expectedLogs.verifyNotLogged("Operation ongoing");
  }

  @Test
  public void testLullDetectionOccursInActiveBundle() throws Exception {
    MillisProvider clock = mock(MillisProvider.class);
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(
            PipelineOptionsFactory.fromArgs("--experiments=state_sampling_period_millis=10")
                .create(),
            clock);
    ExecutionStateTracker tracker = sampler.create();

    CountDownLatch waitTillActive = new CountDownLatch(1);
    CountDownLatch waitForSamples = new CountDownLatch(10);
    Thread testThread = Thread.currentThread();
    Mockito.when(clock.getMillis())
        .thenAnswer(
            new Answer<Long>() {
              private long currentTime;

              @Override
              public Long answer(InvocationOnMock invocation) throws Throwable {
                if (Thread.currentThread().equals(testThread)) {
                  return 0L;
                } else {
                  // Block the state sampling thread till the bundle is active
                  // and unblock the state transition once a certain number of samples
                  // have been taken.
                  waitTillActive.await();
                  waitForSamples.countDown();
                  currentTime += Duration.standardMinutes(1).getMillis();
                  return currentTime;
                }
              }
            });

    tracker.start("bundleId");
    waitTillActive.countDown();
    waitForSamples.await();
    tracker.reset();

    sampler.stop();
    expectedLogs.verifyWarn("Operation ongoing in bundle bundleId for at least");
  }

  @Test
  public void testLullDetectionOccursInActiveState() throws Exception {
    MillisProvider clock = mock(MillisProvider.class);
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(
            PipelineOptionsFactory.fromArgs("--experiments=state_sampling_period_millis=10")
                .create(),
            clock);
    ExecutionStateTracker tracker = sampler.create();
    ExecutionState state = tracker.create("shortId", "ptransformId", "ptransformIdName", "process");

    CountDownLatch waitTillActive = new CountDownLatch(1);
    CountDownLatch waitForSamples = new CountDownLatch(10);
    Thread testThread = Thread.currentThread();
    Mockito.when(clock.getMillis())
        .thenAnswer(
            new Answer<Long>() {
              private long currentTime;

              @Override
              public Long answer(InvocationOnMock invocation) throws Throwable {
                if (Thread.currentThread().equals(testThread)) {
                  return 0L;
                } else {
                  // Block the state sampling thread till the state is active
                  // and unblock the state transition once a certain number of samples
                  // have been taken.
                  waitTillActive.await();
                  waitForSamples.countDown();
                  currentTime += Duration.standardMinutes(1).getMillis();
                  return currentTime;
                }
              }
            });

    tracker.start("bundleId");
    state.activate();
    waitTillActive.countDown();
    waitForSamples.await();
    state.deactivate();
    tracker.reset();

    sampler.stop();
    expectedLogs.verifyWarn("Operation ongoing in bundle bundleId for PTransform");
  }
}
