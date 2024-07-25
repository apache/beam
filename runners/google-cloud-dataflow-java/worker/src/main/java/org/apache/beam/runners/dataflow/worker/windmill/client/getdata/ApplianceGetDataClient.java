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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.windmill.ApplianceWindmillClient;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Appliance implementation of {@link GetDataClient}. */
@Internal
@ThreadSafe
public final class ApplianceGetDataClient implements GetDataClient {
  private static final int MAX_READS_PER_BATCH = 60;
  private static final int MAX_ACTIVE_READS = 10;

  private final ApplianceWindmillClient windmillClient;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;

  @GuardedBy("this")
  private final List<ReadBatch> pendingReadBatches;

  @GuardedBy("this")
  private int activeReadThreads;

  public ApplianceGetDataClient(
      ApplianceWindmillClient windmillClient, ThrottlingGetDataMetricTracker getDataMetricTracker) {
    this.windmillClient = windmillClient;
    this.getDataMetricTracker = getDataMetricTracker;
    this.pendingReadBatches = new ArrayList<>();
    this.activeReadThreads = 0;
  }

  @Override
  public Windmill.KeyedGetDataResponse getStateData(
      String computationId, Windmill.KeyedGetDataRequest request) {
    try (AutoCloseable ignored = getDataMetricTracker.trackStateDataFetchWithThrottling()) {
      SettableFuture<Windmill.KeyedGetDataResponse> response = SettableFuture.create();
      ReadBatch batch = addToReadBatch(new QueueEntry(computationId, request, response));
      if (batch != null) {
        issueReadBatch(batch);
      }
      return response.get();
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching state for computation="
              + computationId
              + ", key="
              + request.getShardingKey(),
          e);
    }
  }

  @Override
  public Windmill.GlobalData getSideInputData(Windmill.GlobalDataRequest request) {
    try (AutoCloseable ignored = getDataMetricTracker.trackSideInputFetchWithThrottling()) {
      return windmillClient
          .getData(Windmill.GetDataRequest.newBuilder().addGlobalDataFetchRequests(request).build())
          .getGlobalData(0);
    } catch (Exception e) {
      throw new GetDataException(
          "Error occurred fetching side input for tag=" + request.getDataId(), e);
    }
  }

  @Override
  public synchronized void printHtml(PrintWriter writer) {
    getDataMetricTracker.printHtml(writer);
    writer.println("  Read threads: " + activeReadThreads);
    writer.println("  Pending read batches: " + pendingReadBatches.size());
  }

  private void issueReadBatch(ReadBatch batch) {
    try {
      // Possibly block until the batch is allowed to start.
      batch.startRead.get();
    } catch (InterruptedException e) {
      // We don't expect this thread to be interrupted. To simplify handling, we just fall through
      // to issuing the call.
      assert (false);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      // startRead is a SettableFuture so this should never occur.
      throw new AssertionError("Should not have exception on startRead", e);
    }
    Map<WindmillComputationKey, SettableFuture<Windmill.KeyedGetDataResponse>> pendingResponses =
        new HashMap<>(batch.reads.size());
    Map<String, ComputationGetDataRequest.Builder> computationBuilders = new HashMap<>();
    for (QueueEntry entry : batch.reads) {
      ComputationGetDataRequest.Builder computationBuilder =
          computationBuilders.computeIfAbsent(
              entry.computation, k -> ComputationGetDataRequest.newBuilder().setComputationId(k));

      computationBuilder.addRequests(entry.request);
      pendingResponses.put(
          WindmillComputationKey.create(
              entry.computation, entry.request.getKey(), entry.request.getShardingKey()),
          entry.response);
    }

    // Build the full GetDataRequest from the KeyedGetDataRequests pulled from the queue.
    Windmill.GetDataRequest.Builder builder = Windmill.GetDataRequest.newBuilder();
    for (ComputationGetDataRequest.Builder computationBuilder : computationBuilders.values()) {
      builder.addRequests(computationBuilder);
    }

    try {
      Windmill.GetDataResponse response = windmillClient.getData(builder.build());
      // Dispatch the per-key responses back to the waiting threads.
      for (Windmill.ComputationGetDataResponse computationResponse : response.getDataList()) {
        for (Windmill.KeyedGetDataResponse keyResponse : computationResponse.getDataList()) {
          pendingResponses
              .get(
                  WindmillComputationKey.create(
                      computationResponse.getComputationId(),
                      keyResponse.getKey(),
                      keyResponse.getShardingKey()))
              .set(keyResponse);
        }
      }
    } catch (RuntimeException e) {
      // Fan the exception out to the reads.
      for (QueueEntry entry : batch.reads) {
        entry.response.setException(e);
      }
    } finally {
      synchronized (this) {
        Preconditions.checkState(activeReadThreads >= 1);
        if (pendingReadBatches.isEmpty()) {
          activeReadThreads--;
        } else {
          // Notify the thread responsible for issuing the next batch read.
          ReadBatch startBatch = pendingReadBatches.remove(0);
          startBatch.startRead.set(null);
        }
      }
    }
  }

  /**
   * Adds the entry to a read batch for sending to the windmill server. If a non-null batch is
   * returned, this thread will be responsible for sending the batch and should wait for the batch
   * startRead to be notified. If null is returned, the entry was added to a read batch that will be
   * issued by another thread.
   */
  private @Nullable ReadBatch addToReadBatch(QueueEntry entry) {
    synchronized (this) {
      ReadBatch batch;
      if (activeReadThreads < MAX_ACTIVE_READS) {
        assert (pendingReadBatches.isEmpty());
        activeReadThreads += 1;
        // fall through to below synchronized block
      } else if (pendingReadBatches.isEmpty()
          || pendingReadBatches.get(pendingReadBatches.size() - 1).reads.size()
              >= MAX_READS_PER_BATCH) {
        // This is the first read of a batch, it will be responsible for sending the batch.
        batch = new ReadBatch();
        pendingReadBatches.add(batch);
        batch.reads.add(entry);
        return batch;
      } else {
        // This fits within an existing batch, it will be sent by the first blocking thread in the
        // batch.
        pendingReadBatches.get(pendingReadBatches.size() - 1).reads.add(entry);
        return null;
      }
    }
    ReadBatch batch = new ReadBatch();
    batch.reads.add(entry);
    batch.startRead.set(null);
    return batch;
  }

  private static final class ReadBatch {
    ArrayList<QueueEntry> reads = new ArrayList<>();
    SettableFuture<Void> startRead = SettableFuture.create();
  }

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
}
