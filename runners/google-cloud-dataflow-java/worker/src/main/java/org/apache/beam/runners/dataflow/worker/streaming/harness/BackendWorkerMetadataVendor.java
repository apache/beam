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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages vending the backend worker metadata to a single consumer.
 *
 * <p>Internally restarts the underlying {@link GetWorkerMetadataStream} to gracefully close the
 * stream and prevent gRPC {@link
 * org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status#DEADLINE_EXCEEDED} statuses.
 */
@ThreadSafe
final class BackendWorkerMetadataVendor {
  private static final Logger LOG = LoggerFactory.getLogger(BackendWorkerMetadataVendor.class);
  private static final int GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES = 3;
  private static final String BACKEND_WORKER_METADATA_VENDOR_THREAD_NAME =
      "BackendWorkerMetadataVendorThread-%d";
  private static final WorkerMetadataResponse NO_WORKER_METADATA =
      WorkerMetadataResponse.getDefaultInstance();

  private final WorkerMetadataStreamFactory getWorkerMetadataStreamFactory;
  private final ExecutorService fetchBackendWorkerMetadataExecutor;
  private final CountDownLatch initialStreamStarted;

  // Writes are synchronized on "this", reads are lock-free.
  private volatile boolean isStarted;
  // Writes are synchronized on "this", reads are lock-free.
  private volatile boolean isShutdown;

  private BackendWorkerMetadataVendor(
      WorkerMetadataStreamFactory getWorkerMetadataStreamFactory,
      ExecutorService fetchBackendWorkerMetadataExecutor) {
    this.getWorkerMetadataStreamFactory = getWorkerMetadataStreamFactory;
    this.fetchBackendWorkerMetadataExecutor = fetchBackendWorkerMetadataExecutor;
    this.initialStreamStarted = new CountDownLatch(1);
  }

  static BackendWorkerMetadataVendor create(
      WorkerMetadataStreamFactory getWorkerMetadataStreamFactory) {
    return new BackendWorkerMetadataVendor(
        getWorkerMetadataStreamFactory,
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(BACKEND_WORKER_METADATA_VENDOR_THREAD_NAME)
                .build()));
  }

  /**
   * Starts the vending of backend worker metadata, passing it to endpointsConsumer for processing.
   * Returns a {@link CompletableFuture} that can be used to block until the initial {@link
   * GetWorkerMetadataStream} has been started.
   */
  synchronized void start(Consumer<WindmillEndpoints> endpointsConsumer) {
    checkState(!isStarted, "Multiple calls to BackendWorkerMetadataVendor.start() is not allowed.");
    checkState(!isShutdown, "BackendWorkerMetadataVendor has previously been shutdown.");
    LOG.debug("Starting WorkerMetadataVendor...");

    fetchBackendWorkerMetadataExecutor.execute(
        () -> {
          WorkerMetadataResponse initialWorkerMetadata = NO_WORKER_METADATA;

          while (true) {
            GetWorkerMetadataStream getWorkerMetadataStream =
                getWorkerMetadataStreamFactory.create(initialWorkerMetadata, endpointsConsumer);
            LOG.debug(
                "Starting GetWorkerMetadataStream w/ metadata version {}.",
                initialWorkerMetadata.getMetadataVersion());
            getWorkerMetadataStream.start();
            initialStreamStarted.countDown();

            // Await stream termination and propagate the most current worker metadata to start
            // the next stream.
            try {
              initialWorkerMetadata = awaitGracefulTermination(getWorkerMetadataStream);
            } catch (InterruptedException e) {
              if (isShutdown) {
                break;
              }
              LOG.error("BackendWorkerMetadataVendor interrupted unexpectedly.", e);
            }

            LOG.debug(
                "Current GetWorkerMetadataStream terminated. Propagating metadata version {} to the next stream.",
                initialWorkerMetadata.getMetadataVersion());
          }

          LOG.info("BackendWorkerMetadata vending complete.");
        });

    isStarted = true;
  }

  /**
   * Wait for the initial backend worker metadata stream to start.
   *
   * @implNote Blocks the calling thread until the stream starts or {@link #shutdown()} is called.
   */
  void awaitInitialBackendWorkerMetadataStream() {
    int waitedSeconds = 0;
    try {
      while (!initialStreamStarted.await(10, TimeUnit.SECONDS) && !isShutdown) {
        waitedSeconds += 10;
        LOG.debug("Waited {}s for initial worker metadata stream to start.", waitedSeconds);
      }
    } catch (InterruptedException e) {
      LOG.warn(
          "Interrupted waiting for initial worker metadata stream. Retrying until shutdown() is called.");
    }
  }

  synchronized void shutdown() {
    if (!isShutdown) {
      LOG.debug("Shutting down WorkerMetadataVendor...");
      isShutdown = true;
      fetchBackendWorkerMetadataExecutor.shutdownNow();
      try {
        fetchBackendWorkerMetadataExecutor.awaitTermination(
            GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Unable to shutdown WorkerMetadataVendor.");
      }
    }
  }

  /**
   * Terminate the stream after {@code GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES}, and return the
   * current WorkerMetadata at the time of termination used to start the next {@link
   * GetWorkerMetadataStream}.
   */
  private WorkerMetadataResponse awaitGracefulTermination(
      GetWorkerMetadataStream getWorkerMetadataStream) throws InterruptedException {
    // Reconnect every now and again to avoid hitting gRPC deadlines. If at any point the server
    // closes the stream, we will reconnect immediately; otherwise shutdown the stream after some
    // time and create a new one.
    if (!getWorkerMetadataStream.awaitTermination(
        GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
      getWorkerMetadataStream.halfClose();
    }

    // If halfClose() didn't gracefully close the stream, forcefully shutdown().
    if (!getWorkerMetadataStream.awaitTermination(30, TimeUnit.SECONDS)) {
      getWorkerMetadataStream.shutdown();
    }

    return getWorkerMetadataStream.currentWorkerMetadata();
  }
}
