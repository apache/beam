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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
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
      "BackendWorkerMetadataVendorThread";
  private static final WorkerMetadataResponse NO_WORKER_METADATA =
      WorkerMetadataResponse.getDefaultInstance();

  private final WorkerMetadataStreamFactory getWorkerMetadataStreamFactory;
  private final ExecutorService workerMetadataFetcher;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private BackendWorkerMetadataVendor(
      WorkerMetadataStreamFactory getWorkerMetadataStreamFactory,
      ExecutorService workerMetadataFetcher) {
    this.getWorkerMetadataStreamFactory = getWorkerMetadataStreamFactory;
    this.workerMetadataFetcher = workerMetadataFetcher;
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
  CompletableFuture<Void> start(Consumer<WindmillEndpoints> endpointsConsumer) {
    if (isRunning.compareAndSet(false, true)) {
      LOG.debug("Starting WorkerMetadataVendor...");

      CountDownLatch initialStreamStarted = new CountDownLatch(1);

      workerMetadataFetcher.execute(
          () -> {
            boolean isInitialWorkerMetadata = true;
            WorkerMetadataResponse initialWorkerMetadata = NO_WORKER_METADATA;

            while (isRunning.get()) {
              GetWorkerMetadataStream getWorkerMetadataStream =
                  getWorkerMetadataStreamFactory.create(initialWorkerMetadata, endpointsConsumer);
              LOG.debug(
                  "Starting GetWorkerMetadataStream w/ metadata version {}.",
                  initialWorkerMetadata.getMetadataVersion());
              getWorkerMetadataStream.start();

              if (isInitialWorkerMetadata) {
                isInitialWorkerMetadata = false;
                initialStreamStarted.countDown();
              }

              // Await stream termination and propagate the most current worker metadata to start
              // the next stream.
              initialWorkerMetadata = awaitGracefulTermination(getWorkerMetadataStream);
              LOG.debug(
                  "Current GetWorkerMetadataStream terminated. Propagating metadata version {} to the next stream.",
                  initialWorkerMetadata.getMetadataVersion());
            }
          });

      return CompletableFuture.runAsync(
          () -> {
            int secondsWaited = 0;
            try {
              while (!initialStreamStarted.await(10, TimeUnit.SECONDS) && isRunning.get()) {
                secondsWaited += 10;
                LOG.debug(
                    "Waited {} seconds for initial GetWorkerMetadataStream to start.",
                    secondsWaited);
              }
            } catch (InterruptedException e) {
              LOG.debug("Interrupted waiting for initial GetWorkerMetadataStream to start.");
              Thread.currentThread().interrupt();
            }
          },
          // Run this on the calling thread.
          MoreExecutors.directExecutor());
    }

    return CompletableFuture.completedFuture(null);
  }

  void stop() {
    if (isRunning.compareAndSet(true, false)) {
      LOG.debug("Shutting down WorkerMetadataVendor...");
      workerMetadataFetcher.shutdownNow();
      boolean isShutdown = false;
      try {
        isShutdown =
            workerMetadataFetcher.awaitTermination(
                GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      if (!isShutdown) {
        LOG.warn("Unable to shutdown WorkerMetadataFetcher.");
      }
    }
  }

  /**
   * Terminate the stream after {@code GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES}, and return the
   * current WorkerMetadata at the time of termination used to start the next {@link
   * GetWorkerMetadataStream}.
   */
  private WorkerMetadataResponse awaitGracefulTermination(
      GetWorkerMetadataStream getWorkerMetadataStream) {
    try {
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

    } catch (InterruptedException e) {
      LOG.debug("WorkerMetadataVendor interrupted unexpectedly.");
    }

    return getWorkerMetadataStream.currentWorkerMetadata();
  }
}
