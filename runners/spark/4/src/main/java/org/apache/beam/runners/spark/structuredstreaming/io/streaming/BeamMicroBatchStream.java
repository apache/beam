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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockManagerId;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

/**
 * Driver side {@link MicroBatchStream} over a Beam {@link UnboundedSource}.
 *
 * <p>Offsets are opaque epochs, {@link #latestOffset()} advances by one every trigger. The source
 * is split once and the splits are pinned under the checkpoint location, every batch of every run
 * plans the same partitions. {@link #commit} purges marks below the committed epoch on a background
 * thread, Spark never asks for those again.
 *
 * <p>Each split prefers the executor rendezvous hashing assigns it, so an executor joining or
 * leaving moves only the splits of that executor. Locality is a hint, the reader cache restores a
 * split from its durable mark wherever it lands.
 */
public class BeamMicroBatchStream<T> implements MicroBatchStream {

  private static final Logger LOG = LoggerFactory.getLogger(BeamMicroBatchStream.class);

  private final BeamSourceSpec<T> spec;
  private final String checkpointLocation;
  private final BeamSourceCheckpoint checkpoint;

  private final ExecutorService purger =
      Executors.newSingleThreadExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "beam-source-mark-purge");
            thread.setDaemon(true);
            return thread;
          });
  private final AtomicBoolean purgeInFlight = new AtomicBoolean();
  private final AtomicLong purgeRequested = new AtomicLong();

  private long epoch;
  private @Nullable List<UnboundedSource<T, ?>> splits;

  BeamMicroBatchStream(BeamSourceSpec<T> spec, String checkpointLocation) {
    this.spec = spec;
    this.checkpointLocation = checkpointLocation;
    this.checkpoint =
        new BeamSourceCheckpoint(checkpointLocation, spec.hadoopConf().value().value());
  }

  @Override
  public Offset initialOffset() {
    return BeamOffset.ZERO;
  }

  @Override
  public synchronized Offset latestOffset() {
    return new BeamOffset(++epoch);
  }

  @Override
  public Offset deserializeOffset(String json) {
    BeamOffset offset = BeamOffset.fromJson(json);
    fastForwardEpoch(offset.epoch());
    return offset;
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    long startEpoch = ((BeamOffset) start).epoch();
    long endEpoch = ((BeamOffset) end).epoch();
    fastForwardEpoch(endEpoch);
    List<UnboundedSource<T, ?>> pinned = splits();
    long[] quotas = splitQuotas(spec.maxRecordsPerBatch(), pinned.size());
    List<String> executors = sortedExecutors();
    InputPartition[] partitions = new InputPartition[pinned.size()];
    for (int i = 0; i < pinned.size(); i++) {
      String[] locations =
          executors.isEmpty() ? new String[0] : new String[] {assign(i, executors)};
      partitions[i] =
          new BeamInputPartition<>(
              pinned.get(i),
              spec.coder(),
              spec.options(),
              spec.hadoopConf(),
              checkpointLocation,
              i,
              startEpoch,
              endEpoch,
              quotas[i],
              spec.maxBatchDurationMillis(),
              spec.readerIdleTimeoutMillis(),
              locations);
    }
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new BeamPartitionReaderFactory();
  }

  /** Purges marks below {@code end} off the stream thread, one purge runs at a time. */
  @Override
  public void commit(Offset end) {
    long endEpoch = ((BeamOffset) end).epoch();
    int numSplits = splits().size();
    purgeRequested.accumulateAndGet(endEpoch, Math::max);
    if (purgeInFlight.compareAndSet(false, true)) {
      purger.execute(() -> purgeRequested(numSplits));
    }
  }

  private void purgeRequested(int numSplits) {
    long epoch;
    do {
      epoch = purgeRequested.get();
      try {
        for (int i = 0; i < numSplits; i++) {
          checkpoint.purgeMarksBelow(i, epoch);
        }
      } catch (IOException | RuntimeException e) {
        LOG.warn("Failed to purge marks below epoch {} at {}.", epoch, checkpointLocation, e);
      }
      purgeInFlight.set(false);
    } while (purgeRequested.get() > epoch && purgeInFlight.compareAndSet(false, true));
  }

  @Override
  public void stop() {
    LOG.info(
        "Stopping Beam micro-batch stream {} at {}.", spec.transformName(), checkpointLocation);
    purger.shutdown();
  }

  /** Keeps {@link #latestOffset()} ahead of every epoch Spark logged before a restart. */
  private synchronized void fastForwardEpoch(long seen) {
    if (seen > epoch) {
      LOG.info("Fast forwarding epoch of {} from {} to {}.", spec.transformName(), epoch, seen);
      epoch = seen;
    }
  }

  private synchronized List<UnboundedSource<T, ?>> splits() {
    if (splits != null) {
      return splits;
    }
    List<UnboundedSource<?, ?>> pinned;
    try {
      pinned = checkpoint.readSplits();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read pinned splits at " + checkpointLocation, e);
    }
    if (pinned == null) {
      pinned = new ArrayList<>(splitSource());
      try {
        checkpoint.writeSplits(pinned);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to pin splits at " + checkpointLocation, e);
      }
    } else {
      LOG.info("Restored {} pinned split(s) from {}.", pinned.size(), checkpointLocation);
    }
    List<UnboundedSource<T, ?>> typed = new ArrayList<>(pinned.size());
    for (UnboundedSource<?, ?> split : pinned) {
      @SuppressWarnings("unchecked") // splits of this source share its element type
      UnboundedSource<T, ?> cast = (UnboundedSource<T, ?>) split;
      typed.add(cast);
    }
    splits = typed;
    return typed;
  }

  private List<? extends UnboundedSource<T, ?>> splitSource() {
    UnboundedSource<T, ?> source = spec.source();
    PipelineOptions options = spec.options().value().get();
    List<? extends UnboundedSource<T, ?>> result;
    try {
      result = source.split(spec.desiredNumSplits(), options);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to split UnboundedSource " + source.getClass().getCanonicalName(), e);
    }
    if (result.isEmpty()) {
      result = Collections.singletonList(source);
    }
    LOG.info(
        "Split {} into {} partition(s), desired {}.",
        spec.transformName(),
        result.size(),
        spec.desiredNumSplits());
    return result;
  }

  /**
   * Divides the batch quota over the splits, the remainder goes to the first splits. Below 1 means
   * unlimited for every split. A quota below the split count gives every split one record.
   */
  static long[] splitQuotas(long maxRecordsPerBatch, int numSplits) {
    long[] quotas = new long[numSplits];
    if (maxRecordsPerBatch < 1) {
      Arrays.fill(quotas, maxRecordsPerBatch);
      return quotas;
    }
    long base = maxRecordsPerBatch / numSplits;
    long remainder = maxRecordsPerBatch % numSplits;
    for (int i = 0; i < numSplits; i++) {
      quotas[i] = Math.max(1L, base + (i < remainder ? 1 : 0));
    }
    return quotas;
  }

  /** Rendezvous hashing of a split over the executors, stable under membership changes. */
  static String assign(int splitId, List<String> executors) {
    String best = executors.get(0);
    int bestHash = Integer.MIN_VALUE;
    for (String executor : executors) {
      int hash = Hashing.murmur3_32_fixed().hashUnencodedChars(executor + '#' + splitId).asInt();
      if (hash > bestHash) {
        bestHash = hash;
        best = executor;
      }
    }
    return best;
  }

  /** Sorted {@code executor_<host>_<id>} locations, empty in local mode or on any failure. */
  private static List<String> sortedExecutors() {
    try {
      BlockManager bm = SparkEnv.get().blockManager();
      Iterator<BlockManagerId> peers = bm.master().getPeers(bm.blockManagerId()).iterator();
      List<String> executors = new ArrayList<>();
      while (peers.hasNext()) {
        BlockManagerId peer = peers.next();
        executors.add("executor_" + peer.host() + "_" + peer.executorId());
      }
      Collections.sort(executors);
      return executors;
    } catch (RuntimeException e) {
      LOG.debug("No executor list available for preferred locations.", e);
      return Collections.emptyList();
    }
  }
}
