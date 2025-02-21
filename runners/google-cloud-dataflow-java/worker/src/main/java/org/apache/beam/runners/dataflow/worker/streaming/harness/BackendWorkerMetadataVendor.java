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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
final class BackendWorkerMetadataVendor {
  private static final Logger LOG = LoggerFactory.getLogger(BackendWorkerMetadataVendor.class);
  private static final int GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES = 3;
  private static final String BACKEND_WORKER_METADATA_VENDOR_THREAD_NAME =
      "BackendWorkerMetadataVendorThread";

  private final Function<Consumer<WindmillEndpoints>, GetWorkerMetadataStream>
      getWorkerMetadataStreamFactory;
  private final ExecutorService workerMetadataFetcher;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private BackendWorkerMetadataVendor(
      Function<Consumer<WindmillEndpoints>, GetWorkerMetadataStream> getWorkerMetadataStreamFactory,
      ExecutorService workerMetadataFetcher) {
    this.getWorkerMetadataStreamFactory = getWorkerMetadataStreamFactory;
    this.workerMetadataFetcher = workerMetadataFetcher;
  }

  static BackendWorkerMetadataVendor create(
      Function<Consumer<WindmillEndpoints>, GetWorkerMetadataStream>
          getWorkerMetadataStreamFactory) {
    return new BackendWorkerMetadataVendor(
        getWorkerMetadataStreamFactory,
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(BACKEND_WORKER_METADATA_VENDOR_THREAD_NAME)
                .build()));
  }

  /**
   * Starts the vending of backend worker metadata, passing it to endpointsConsumer for processing.
   */
  void start(Consumer<WindmillEndpoints> endpointsConsumer) {
    if (isRunning.compareAndSet(false, true)) {
      LOG.debug("Starting WorkerMetadataVendor...");
      // Block on the initial workerMetadata.
      // Run the subsequent fetches on a separate thread.
      fetchAndConsumeWorkerMetadata(endpointsConsumer);
      LOG.debug("Initial WorkerMetadata received.");
      workerMetadataFetcher.execute(
          () -> {
            while (isRunning.get()) {
              fetchAndConsumeWorkerMetadata(endpointsConsumer);
            }
          });
    }
  }

  void stop() {
    if (isRunning.compareAndSet(true, false)) {
      LOG.debug("Shutting down WorkerMetadataVendor...");
      workerMetadataFetcher.shutdown();
      boolean isShutdown = false;
      try {
        isShutdown =
            workerMetadataFetcher.awaitTermination(
                GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Unable to shutdown WorkerMetadataFetcher.");
      }

      if (!isShutdown) {
        workerMetadataFetcher.shutdownNow();
      }
    }
  }

  private void fetchAndConsumeWorkerMetadata(Consumer<WindmillEndpoints> endpointsConsumer) {
    GetWorkerMetadataStream getWorkerMetadataStream =
        getWorkerMetadataStreamFactory.apply(endpointsConsumer);
    try {
      getWorkerMetadataStream.start();
      // Reconnect every now and again to enable better load balancing and to avoid hitting
      // gRPC deadlines. If at any point the server closes the stream, we will reconnect
      // immediately; otherwise shutdown the stream after some time and create a new one.
      if (!getWorkerMetadataStream.awaitTermination(
          GET_WORKER_METADATA_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
        getWorkerMetadataStream.shutdown();
      }
    } catch (InterruptedException e) {
      LOG.debug("WorkerMetadataVendor interrupted unexpectedly.");
    }
  }
}
