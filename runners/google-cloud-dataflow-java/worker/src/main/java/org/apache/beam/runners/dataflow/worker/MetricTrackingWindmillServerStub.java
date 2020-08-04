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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.GetDataStream;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.SettableFuture;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Wrapper around a {@link WindmillServerStub} that tracks metrics for the number of in-flight
 * requests and throttles requests when memory pressure is high.
 *
 * <p>External API: individual worker threads request state for their computation via {@link
 * #getStateData}. However, we want to batch requests to WMS rather than calling for each thread, so
 * calls actually just enqueue a state request in the local queue, which will be handled by up to
 * {@link #NUM_THREADS} polling that queue and making requests to WMS in batches of size {@link
 * #MAX_READS_PER_BATCH}.
 */
public class MetricTrackingWindmillServerStub {

  private final AtomicInteger activeSideInputs = new AtomicInteger();
  private final AtomicInteger activeStateReads = new AtomicInteger();
  private final AtomicInteger activeHeartbeats = new AtomicInteger();
  private final WindmillServerStub server;
  private final MemoryMonitor gcThrashingMonitor;
  private final boolean useStreamingRequests;

  private final ArrayBlockingQueue<QueueEntry> readQueue;
  private final List<Thread> readPool;

  private WindmillServerStub.StreamPool<GetDataStream> streamPool;

  private static final int MAX_READS_PER_BATCH = 60;
  private static final int QUEUE_SIZE = 1000;
  private static final int NUM_THREADS = 10;
  private static final int NUM_STREAMS = 1;
  private static final Duration STREAM_TIMEOUT = Duration.standardSeconds(30);

  private static final class QueueEntry {

    final String computation;
    final Windmill.KeyedGetDataRequest request;
    final SettableFuture<Windmill.KeyedGetDataResponse> response;

    QueueEntry(
        String computation,
        Windmill.KeyedGetDataRequest request,
        SettableFuture<Windmill.KeyedGetDataResponse> response) {
      this.computation = computation;
      this.request = request;
      this.response = response;
    }
  }

  private static final class KeyAndComputation {

    final ByteString key;
    final long shardingKey;
    final String computation;

    KeyAndComputation(ByteString key, long shardingKey, String computation) {
      this.key = key;
      this.shardingKey = shardingKey;
      this.computation = computation;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof KeyAndComputation)) {
        return false;
      }
      KeyAndComputation that = (KeyAndComputation) other;
      return this.key.equals(that.key) && this.shardingKey == that.shardingKey && this.computation
          .equals(that.computation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, shardingKey, computation);
    }
  }

  public MetricTrackingWindmillServerStub(
      WindmillServerStub server, MemoryMonitor gcThrashingMonitor, boolean useStreamingRequests) {
    this.server = server;
    this.gcThrashingMonitor = gcThrashingMonitor;
    this.readQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    this.readPool = new ArrayList<>(NUM_THREADS);
    this.useStreamingRequests = useStreamingRequests;
  }

  public void start() {
    if (useStreamingRequests) {
      streamPool =
          new WindmillServerStub.StreamPool<>(
              NUM_STREAMS, STREAM_TIMEOUT, this.server::getDataStream);
    } else {
      for (int i = 0; i < NUM_THREADS; i++) {
        readPool.add(
            new Thread("GetDataThread" + i) {
              @Override
              public void run() {
                getDataLoop();
              }
            });
        readPool.get(i).start();
      }
    }
  }

  private void getDataLoop() {
    while (true) {
      // First, block until the readQueue has data, then pull up to MAX_READS_PER_BATCH or until the
      // queue is empty.
      QueueEntry entry;
      try {
        entry = readQueue.take();
      } catch (InterruptedException e) {
        continue;
      }
      int numReads = 1;
      Map<KeyAndComputation, SettableFuture<Windmill.KeyedGetDataResponse>> pendingResponses =
          new HashMap<>();
      Map<String, Windmill.ComputationGetDataRequest.Builder> computationBuilders = new HashMap<>();
      do {
        Windmill.ComputationGetDataRequest.Builder computationBuilder =
            computationBuilders.get(entry.computation);
        if (computationBuilder == null) {
          computationBuilder =
              Windmill.ComputationGetDataRequest.newBuilder().setComputationId(entry.computation);
          computationBuilders.put(entry.computation, computationBuilder);
        }

        computationBuilder.addRequests(entry.request);
        pendingResponses.put(
            new KeyAndComputation(entry.request.getKey(), entry.request.getShardingKey(),
                entry.computation), entry.response);
      } while (numReads++ < MAX_READS_PER_BATCH && (entry = readQueue.poll()) != null);

      // Build the full GetDataRequest from the KeyedGetDataRequests pulled from the queue.
      Windmill.GetDataRequest.Builder builder = Windmill.GetDataRequest.newBuilder();
      for (Windmill.ComputationGetDataRequest.Builder computationBuilder :
          computationBuilders.values()) {
        builder.addRequests(computationBuilder.build());
      }

      Windmill.GetDataResponse response = server.getData(builder.build());

      // Dispatch the per-key responses back to the waiting threads.
      for (Windmill.ComputationGetDataResponse computationResponse : response.getDataList()) {
        for (Windmill.KeyedGetDataResponse keyResponse : computationResponse.getDataList()) {
          pendingResponses
              .get(
                  new KeyAndComputation(
                      keyResponse.getKey(), keyResponse.getShardingKey(),
                      computationResponse.getComputationId()))
              .set(keyResponse);
        }
      }
    }
  }

  public Windmill.KeyedGetDataResponse getStateData(
      String computation, Windmill.KeyedGetDataRequest request) {
    gcThrashingMonitor.waitForResources("GetStateData");
    activeStateReads.getAndIncrement();

    try {
      if (useStreamingRequests) {
        GetDataStream stream = streamPool.getStream();
        try {
          return stream.requestKeyedData(computation, request);
        } finally {
          streamPool.releaseStream(stream);
        }
      } else {
        SettableFuture<Windmill.KeyedGetDataResponse> response = SettableFuture.create();
        readQueue.add(new QueueEntry(computation, request, response));
        return response.get();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      activeStateReads.getAndDecrement();
    }
  }

  public Windmill.GlobalData getSideInputData(Windmill.GlobalDataRequest request) {
    gcThrashingMonitor.waitForResources("GetSideInputData");
    activeSideInputs.getAndIncrement();
    try {
      if (useStreamingRequests) {
        GetDataStream stream = streamPool.getStream();
        try {
          return stream.requestGlobalData(request);
        } finally {
          streamPool.releaseStream(stream);
        }
      } else {
        return server
            .getData(
                Windmill.GetDataRequest.newBuilder().addGlobalDataFetchRequests(request).build())
            .getGlobalData(0);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get side input: ", e);
    } finally {
      activeSideInputs.getAndDecrement();
    }
  }

  /**
   * Tells windmill processing is ongoing for the given keys.
   */
  public void refreshActiveWork(Map<String, List<KeyedGetDataRequest>> active) {
    activeHeartbeats.set(active.size());
    try {
      if (useStreamingRequests) {
        // With streaming requests, always send the request even when it is empty, to ensure that
        // we trigger health checks for the stream even when it is idle.
        GetDataStream stream = streamPool.getStream();
        try {
          stream.refreshActiveWork(active);
        } finally {
          streamPool.releaseStream(stream);
        }
      } else if (!active.isEmpty()) {
        Windmill.GetDataRequest.Builder builder = Windmill.GetDataRequest.newBuilder();
        for (Map.Entry<String, List<KeyedGetDataRequest>> entry : active.entrySet()) {
          builder.addRequests(
              Windmill.ComputationGetDataRequest.newBuilder()
                  .setComputationId(entry.getKey())
                  .addAllRequests(entry.getValue()));
        }
        server.getData(builder.build());
      }
    } finally {
      activeHeartbeats.set(0);
    }
  }

  public void printHtml(PrintWriter writer) {
    writer.println("Active Fetches:");
    writer.println("  Side Inputs: " + activeSideInputs.get());
    writer.println("  State Reads: " + activeStateReads.get());
    writer.println("Heartbeat Keys Active: " + activeHeartbeats.get());
  }
}
