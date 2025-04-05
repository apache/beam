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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BackendWorkerMetadataVendorTest {

  private BackendWorkerMetadataVendor backendWorkerMetadataVendor;

  private static GetWorkerMetadataStream createMockStream() {
    GetWorkerMetadataStream mockStream = mock(GetWorkerMetadataStream.class);
    when(mockStream.currentWorkerMetadata())
        .thenReturn(WorkerMetadataResponse.getDefaultInstance());
    return mockStream;
  }

  @After
  public void tearDown() {
    if (backendWorkerMetadataVendor != null) {
      // Shutdown to prevent any dangling resources.
      backendWorkerMetadataVendor.shutdown();
    }
  }

  @Test
  public void testStreamRestart_propagatesWorkerMetadataVersionToNewStream() {
    List<TestStream> streams = new ArrayList<>();
    backendWorkerMetadataVendor =
        BackendWorkerMetadataVendor.create(
            (initialWorkerMetadata, ignored) -> {
              TestStream stream = new TestStream(initialWorkerMetadata);
              streams.add(stream);
              return stream;
            });

    backendWorkerMetadataVendor.start(ignored -> {});

    // This will trigger a new stream to be created w/ initialWorkerMetadata with version 2.
    TestStream stream1 =
        Iterables.getOnlyElement(
            streams.stream().filter(stream -> !stream.isTerminated).collect(Collectors.toList()));
    stream1.updateCurrentMetadataVersion(1);
    stream1.updateCurrentMetadataVersion(2);
    stream1.unblockTermination();

    // This will trigger a new stream to be created w/ initialWorkerMetadata with version 4.
    TestStream stream2 =
        Iterables.getOnlyElement(
            streams.stream().filter(stream -> !stream.isTerminated).collect(Collectors.toList()));
    stream2.updateCurrentMetadataVersion(3);
    stream2.updateCurrentMetadataVersion(4);
    stream2.unblockTermination();

    // This will trigger a new stream to be created w/ initialWorkerMetadata with version 6.
    TestStream stream3 =
        Iterables.getOnlyElement(
            streams.stream().filter(stream -> !stream.isTerminated).collect(Collectors.toList()));
    stream3.updateCurrentMetadataVersion(5);
    stream3.updateCurrentMetadataVersion(6);
    stream3.unblockTermination();

    // Sleep a bit for the last termination to propagate.
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    // 3 streams should be terminated, 1 stream should active.
    streams.forEach(
        stream -> {
          // Stream started w/ metadata version 0 and terminated w/ metadata version 2
          if (stream.initialMetadataVersion() == 0) {
            assertThat(stream.currentMetadataVersion()).isEqualTo(2);
          }
          // Stream started w/ metadata version 2 and terminated w/ metadata version 4
          if (stream.initialMetadataVersion() == 2) {
            assertThat(stream.currentMetadataVersion()).isEqualTo(4);
          }
          // Stream started w/ metadata version 4 and terminated w/ metadata version 6
          if (stream.initialMetadataVersion() == 4) {
            assertThat(stream.currentMetadataVersion()).isEqualTo(6);
          }
          // Stream started w/ metadata version 6 and is still active.
          if (stream.initialMetadataVersion() == 6) {
            assertThat(stream.currentMetadataVersion()).isEqualTo(6);
            assertFalse(stream.isTerminated);
          }
        });
  }

  @Test
  public void testStart_calledMultipleTimes() {
    GetWorkerMetadataStream mockStream = createMockStream();
    backendWorkerMetadataVendor =
        BackendWorkerMetadataVendor.create(
            (initialWorkerMetadata, endpointsConsumer) -> mockStream);

    backendWorkerMetadataVendor.start(ignored -> {});
    doNothing().when(mockStream).start();
    backendWorkerMetadataVendor.awaitInitialBackendWorkerMetadataStream();
    assertThrows(
        IllegalStateException.class, () -> backendWorkerMetadataVendor.start(ignored -> {}));
  }

  @Test
  public void testStart_calledAfterShutdown() {
    backendWorkerMetadataVendor =
        BackendWorkerMetadataVendor.create(
            (initialWorkerMetadata, endpointsConsumer) -> createMockStream());

    backendWorkerMetadataVendor.shutdown();
    assertThrows(
        IllegalStateException.class, () -> backendWorkerMetadataVendor.start(ignored -> {}));
  }

  @Test
  public void testAwaitInitialBackendWorkerMetadataStream_stopsAfterShutdown()
      throws ExecutionException, InterruptedException {
    GetWorkerMetadataStream mockStream = createMockStream();
    backendWorkerMetadataVendor =
        BackendWorkerMetadataVendor.create(
            (initialWorkerMetadata, endpointsConsumer) -> mockStream);

    CountDownLatch startBlocker = new CountDownLatch(1);
    doAnswer(
            ignored -> {
              startBlocker.await();
              return null;
            })
        .when(mockStream)
        .start();

    backendWorkerMetadataVendor.start(ignored -> {});

    ExecutorService awaitExecutor = Executors.newSingleThreadExecutor();
    Future<Boolean> awaitFuture =
        awaitExecutor.submit(
            () -> {
              backendWorkerMetadataVendor.awaitInitialBackendWorkerMetadataStream();
              return true;
            });

    // Shutdown while we are waiting for awaitInitialBackendWorkerMetadataStream().
    backendWorkerMetadataVendor.shutdown();

    assertTrue(awaitFuture.get());
    assertTrue(awaitFuture.isDone());
    assertFalse(awaitFuture.isCancelled());

    // Unblock the stream.start() call to clean up the test.
    startBlocker.countDown();
  }

  private static class TestStream implements GetWorkerMetadataStream {
    private final WorkerMetadataResponse initialMetadata;
    private final CountDownLatch awaitTermination = new CountDownLatch(1);
    private WorkerMetadataResponse currentMetadata;
    private boolean isTerminated = false;

    private TestStream(WorkerMetadataResponse initialMetadata) {
      this.initialMetadata = initialMetadata;
      this.currentMetadata = initialMetadata;
    }

    private long initialMetadataVersion() {
      return initialMetadata.getMetadataVersion();
    }

    private long currentMetadataVersion() {
      return currentMetadata.getMetadataVersion();
    }

    private void updateCurrentMetadataVersion(long metadataVersion) {
      if (!isTerminated) {
        currentMetadata =
            WorkerMetadataResponse.newBuilder().setMetadataVersion(metadataVersion).build();
      }
    }

    @Override
    public WorkerMetadataResponse currentWorkerMetadata() {
      return currentMetadata;
    }

    @Override
    public void start() {}

    @Override
    public String backendWorkerToken() {
      return "";
    }

    @Override
    public void halfClose() {}

    @Override
    public boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException {
      awaitTermination.await();
      return false;
    }

    private void unblockTermination() {
      isTerminated = true;
      awaitTermination.countDown();
    }

    @Override
    public Instant startTime() {
      return null;
    }

    @Override
    public void shutdown() {}
  }
}
