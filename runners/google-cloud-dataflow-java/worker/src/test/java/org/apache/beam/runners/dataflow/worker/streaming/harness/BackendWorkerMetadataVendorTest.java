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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BackendWorkerMetadataVendorTest {

  private static WindmillStream.GetWorkerMetadataStream createMockStream() {
    WindmillStream.GetWorkerMetadataStream mockStream =
        mock(WindmillStream.GetWorkerMetadataStream.class);
    when(mockStream.currentWorkerMetadata())
        .thenReturn(Windmill.WorkerMetadataResponse.getDefaultInstance());
    return mockStream;
  }

  @Test
  public void testStart_calledMultipleTimes() {
    WindmillStream.GetWorkerMetadataStream mockStream = createMockStream();
    BackendWorkerMetadataVendor backendWorkerMetadataVendor =
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
    BackendWorkerMetadataVendor backendWorkerMetadataVendor =
        BackendWorkerMetadataVendor.create(
            (initialWorkerMetadata, endpointsConsumer) -> createMockStream());

    backendWorkerMetadataVendor.shutdown();
    assertThrows(
        IllegalStateException.class, () -> backendWorkerMetadataVendor.start(ignored -> {}));
  }

  @Test
  public void testGracefulStreamTermination_interruptedWithoutShutdown()
      throws InterruptedException {
    WindmillStream.GetWorkerMetadataStream mockStream = createMockStream();
    BackendWorkerMetadataVendor backendWorkerMetadataVendor =
        BackendWorkerMetadataVendor.create(
            (initialWorkerMetadata, endpointsConsumer) -> mockStream);

    when(mockStream.awaitTermination(anyInt(), any(TimeUnit.class)))
        .thenThrow(InterruptedException.class);
    backendWorkerMetadataVendor.start(ignored -> {});
    assertThrows(
        IllegalStateException.class, () -> backendWorkerMetadataVendor.start(ignored -> {}));
  }

  @Test
  public void testAwaitInitialBackendWorkerMetadataStream_stopsAfterShutdown()
      throws ExecutionException, InterruptedException {
    WindmillStream.GetWorkerMetadataStream mockStream = createMockStream();
    BackendWorkerMetadataVendor backendWorkerMetadataVendor =
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
}
