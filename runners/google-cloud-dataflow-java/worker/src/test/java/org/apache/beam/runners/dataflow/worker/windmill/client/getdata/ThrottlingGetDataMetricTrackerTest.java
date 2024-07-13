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
package org.apache.beam.runners.dataflow.worker.windmill.client.getdata;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings("FutureReturnValueIgnored")
public class ThrottlingGetDataMetricTrackerTest {

  private final MemoryMonitor memoryMonitor = mock(MemoryMonitor.class);
  private final ThrottlingGetDataMetricTracker getDataMetricTracker =
      new ThrottlingGetDataMetricTracker(memoryMonitor);
  private final ExecutorService getDataProcessor = Executors.newCachedThreadPool();

  @Test
  public void testTrackFetchStateDataWithThrottling() throws InterruptedException {
    doNothing().when(memoryMonitor).waitForResources(anyString());
    CountDownLatch processCall = new CountDownLatch(1);
    CountDownLatch callProcessing = new CountDownLatch(1);
    CountDownLatch processingDone = new CountDownLatch(1);
    getDataProcessor.submit(
        () -> {
          try (AutoCloseable ignored = getDataMetricTracker.trackStateDataFetchWithThrottling()) {
            callProcessing.countDown();
            processCall.await();
          } catch (Exception e) {
            // Do nothing.
          }
          processingDone.countDown();
        });

    callProcessing.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsWhileProcessing =
        getDataMetricTracker.getMetricsSnapshot();

    assertThat(metricsWhileProcessing.activeStateReads()).isEqualTo(1);
    assertThat(metricsWhileProcessing.activeHeartbeats()).isEqualTo(0);
    assertThat(metricsWhileProcessing.activeSideInputs()).isEqualTo(0);

    // Free the thread inside the AutoCloseable, wait for processingDone and check that metrics gets
    // decremented
    processCall.countDown();
    processingDone.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsAfterProcessing =
        getDataMetricTracker.getMetricsSnapshot();
    assertThat(metricsAfterProcessing.activeStateReads()).isEqualTo(0);
    assertThat(metricsAfterProcessing.activeHeartbeats()).isEqualTo(0);
    assertThat(metricsAfterProcessing.activeSideInputs()).isEqualTo(0);
  }

  @Test
  public void testTrackSideInputFetchWithThrottling() throws InterruptedException {
    doNothing().when(memoryMonitor).waitForResources(anyString());
    CountDownLatch processCall = new CountDownLatch(1);
    CountDownLatch callProcessing = new CountDownLatch(1);
    CountDownLatch processingDone = new CountDownLatch(1);
    getDataProcessor.submit(
        () -> {
          try (AutoCloseable ignored = getDataMetricTracker.trackSideInputFetchWithThrottling()) {
            callProcessing.countDown();
            processCall.await();
          } catch (Exception e) {
            // Do nothing.
          }
          processingDone.countDown();
        });

    callProcessing.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsWhileProcessing =
        getDataMetricTracker.getMetricsSnapshot();

    assertThat(metricsWhileProcessing.activeStateReads()).isEqualTo(0);
    assertThat(metricsWhileProcessing.activeHeartbeats()).isEqualTo(0);
    assertThat(metricsWhileProcessing.activeSideInputs()).isEqualTo(1);

    // Free the thread inside the AutoCloseable, wait for processingDone and check that metrics gets
    // decremented
    processCall.countDown();
    processingDone.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsAfterProcessing =
        getDataMetricTracker.getMetricsSnapshot();
    assertThat(metricsAfterProcessing.activeStateReads()).isEqualTo(0);
    assertThat(metricsAfterProcessing.activeHeartbeats()).isEqualTo(0);
    assertThat(metricsAfterProcessing.activeSideInputs()).isEqualTo(0);
  }

  @Test
  public void testThrottledTrackSingleCallWithThrottling() throws InterruptedException {
    CountDownLatch mockThrottler = simulateMemoryPressure();
    CountDownLatch processCall = new CountDownLatch(1);
    CountDownLatch callProcessing = new CountDownLatch(1);
    CountDownLatch processingDone = new CountDownLatch(1);
    getDataProcessor.submit(
        () -> {
          try (AutoCloseable ignored = getDataMetricTracker.trackStateDataFetchWithThrottling()) {
            callProcessing.countDown();
            processCall.await();
          } catch (Exception e) {
            // Do nothing.
          }
          processingDone.countDown();
        });

    assertFalse(callProcessing.await(10, TimeUnit.MILLISECONDS));
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsBeforeProcessing =
        getDataMetricTracker.getMetricsSnapshot();
    assertThat(metricsBeforeProcessing.activeStateReads()).isEqualTo(0);
    assertThat(metricsBeforeProcessing.activeHeartbeats()).isEqualTo(0);
    assertThat(metricsBeforeProcessing.activeSideInputs()).isEqualTo(0);

    // Stop throttling.
    mockThrottler.countDown();
    callProcessing.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsWhileProcessing =
        getDataMetricTracker.getMetricsSnapshot();

    assertThat(metricsWhileProcessing.activeStateReads()).isEqualTo(1);

    // Free the thread inside the AutoCloseable, wait for processingDone and check that metrics gets
    // decremented
    processCall.countDown();
    processingDone.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsAfterProcessing =
        getDataMetricTracker.getMetricsSnapshot();
    assertThat(metricsAfterProcessing.activeStateReads()).isEqualTo(0);
  }

  @Test
  public void testTrackSingleCall_exceptionThrown() throws InterruptedException {
    doNothing().when(memoryMonitor).waitForResources(anyString());
    CountDownLatch callProcessing = new CountDownLatch(1);
    CountDownLatch beforeException = new CountDownLatch(1);
    CountDownLatch afterException = new CountDownLatch(1);

    // Catch the exception outside the try-with-resources block to ensure that
    // AutoCloseable.closed() runs in the midst of an exception.
    getDataProcessor.submit(
        () -> {
          try {
            try (AutoCloseable ignored = getDataMetricTracker.trackStateDataFetchWithThrottling()) {
              callProcessing.countDown();
              beforeException.await();
              throw new RuntimeException("something bad happened");
            }
          } catch (RuntimeException e) {
            afterException.countDown();
            throw e;
          }
        });

    callProcessing.await();

    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsWhileProcessing =
        getDataMetricTracker.getMetricsSnapshot();

    assertThat(metricsWhileProcessing.activeStateReads()).isEqualTo(1);
    beforeException.countDown();

    // In the midst of an exception, close() should still run.
    afterException.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsAfterProcessing =
        getDataMetricTracker.getMetricsSnapshot();
    assertThat(metricsAfterProcessing.activeStateReads()).isEqualTo(0);
  }

  @Test
  public void testTrackHeartbeats() throws InterruptedException {
    CountDownLatch processCall = new CountDownLatch(1);
    CountDownLatch callProcessing = new CountDownLatch(1);
    CountDownLatch processingDone = new CountDownLatch(1);
    int numHeartbeats = 5;
    getDataProcessor.submit(
        () -> {
          try (AutoCloseable ignored = getDataMetricTracker.trackHeartbeats(numHeartbeats)) {
            callProcessing.countDown();
            processCall.await();
          } catch (Exception e) {
            // Do nothing.
          }
          processingDone.countDown();
        });

    callProcessing.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsWhileProcessing =
        getDataMetricTracker.getMetricsSnapshot();

    assertThat(metricsWhileProcessing.activeHeartbeats()).isEqualTo(5);

    // Free the thread inside the AutoCloseable, wait for processingDone and check that metrics gets
    // decremented
    processCall.countDown();
    processingDone.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsAfterProcessing =
        getDataMetricTracker.getMetricsSnapshot();
    assertThat(metricsAfterProcessing.activeHeartbeats()).isEqualTo(0);
  }

  @Test
  public void testTrackHeartbeats_exceptionThrown() throws InterruptedException {
    CountDownLatch callProcessing = new CountDownLatch(1);
    CountDownLatch beforeException = new CountDownLatch(1);
    CountDownLatch afterException = new CountDownLatch(1);
    int numHeartbeats = 10;
    // Catch the exception outside the try-with-resources block to ensure that
    // AutoCloseable.closed() runs in the midst of an exception.
    getDataProcessor.submit(
        () -> {
          try {
            try (AutoCloseable ignored = getDataMetricTracker.trackHeartbeats(numHeartbeats)) {
              callProcessing.countDown();
              beforeException.await();
              throw new RuntimeException("something bad happened");
            }
          } catch (RuntimeException e) {
            afterException.countDown();
            throw e;
          }
        });

    callProcessing.await();

    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsWhileProcessing =
        getDataMetricTracker.getMetricsSnapshot();

    assertThat(metricsWhileProcessing.activeHeartbeats()).isEqualTo(numHeartbeats);
    beforeException.countDown();

    // In the midst of an exception, close() should still run.
    afterException.await();
    ThrottlingGetDataMetricTracker.ReadOnlySnapshot metricsAfterProcessing =
        getDataMetricTracker.getMetricsSnapshot();
    assertThat(metricsAfterProcessing.activeHeartbeats()).isEqualTo(0);
  }

  /** Have the memory monitor block when waitForResources is called simulating memory pressure. */
  private CountDownLatch simulateMemoryPressure() {
    CountDownLatch mockThrottler = new CountDownLatch(1);
    doAnswer(
            invocationOnMock -> {
              mockThrottler.await();
              return null;
            })
        .when(memoryMonitor)
        .waitForResources(anyString());
    return mockThrottler;
  }
}
