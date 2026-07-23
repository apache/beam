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
package org.apache.beam.sdk.io.solace.read;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.solace.broker.SempClient;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unbounded Reader to read messages from a Solace Router. */
@VisibleForTesting
class UnboundedSolaceReader<T> extends UnboundedReader<T> {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSolaceReader.class);
  private final UnboundedSolaceSource<T> currentSource;
  private final WatermarkPolicy<T> watermarkPolicy;
  private final SempClient sempClient;
  final String readerUuid;
  private final ExecutorService ackExecutor;
  @VisibleForTesting Supplier<Long> clock = System::currentTimeMillis;
  private final Object lock = new Object();
  private final SessionServiceFactory sessionServiceFactory;
  private @Nullable BytesXMLMessage solaceOriginalRecord;
  private @Nullable T solaceMappedRecord;
  private final Counter messagesReceived =
      Metrics.counter(UnboundedSolaceReader.class, "messages_received");
  private final Counter messagesAcked =
      Metrics.counter(UnboundedSolaceReader.class, "messages_acked");

  private final Duration ackDeadline;

  /**
   * Map to track pending checkpoints and their messages. Accessed by both reader
   * (getCheckpointMark) and finalizer (finalizeCheckpoint) threads.
   */
  private final TreeMap<Long, PendingCheckpoint> pendingCheckpoints = new TreeMap<>();

  private long nextCheckpointId = 1;

  /**
   * Queue for messages that were ingested in the {@link #advance()} method, but not sent yet to a
   * {@link SolaceCheckpointMark}.
   */
  private final Queue<BytesXMLMessage> receivedMessages = new ArrayDeque<>();

  private static final Cache<String, SessionService> sessionServiceCache;
  private static final ScheduledExecutorService cleanUpThread = Executors.newScheduledThreadPool(1);

  static {
    Duration cacheExpirationTimeout = Duration.ofMinutes(1);
    sessionServiceCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(cacheExpirationTimeout)
            .removalListener(
                (RemovalNotification<String, SessionService> notification) -> {
                  LOG.info(
                      "SolaceIO.Read: Closing session for the reader with uuid {} as it has been idle for over {}.",
                      notification.getKey(),
                      cacheExpirationTimeout);
                  SessionService sessionService = notification.getValue();
                  if (sessionService != null) {
                    sessionService.close();
                  }
                })
            .build();

    startCleanUpThread();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private static void startCleanUpThread() {
    cleanUpThread.scheduleAtFixedRate(sessionServiceCache::cleanUp, 1, 1, TimeUnit.MINUTES);
  }

  public UnboundedSolaceReader(UnboundedSolaceSource<T> currentSource) {
    this.currentSource = currentSource;
    this.watermarkPolicy =
        WatermarkPolicy.create(
            currentSource.getTimestampFn(), currentSource.getWatermarkIdleDurationThreshold());
    this.sessionServiceFactory = currentSource.getSessionServiceFactory();
    this.sempClient = currentSource.getSempClientFactory().create();
    this.readerUuid = UUID.randomUUID().toString();
    this.ackExecutor = Executors.newFixedThreadPool(4);
    this.ackDeadline = java.time.Duration.ofMillis(currentSource.getAckDeadline().getMillis());
  }

  private SessionService getSessionService() {
    try {
      return sessionServiceCache.get(
          readerUuid,
          () -> {
            LOG.info("SolaceIO.Read: creating a new session for reader with uuid {}.", readerUuid);
            SessionService sessionService = sessionServiceFactory.create();
            sessionService.connect();
            sessionService.getReceiver().start();
            return sessionService;
          });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean start() {
    // Create and initialize SessionService with Receiver
    getSessionService();
    return advance();
  }

  @Override
  public boolean advance() {
    checkTimeouts();
    BytesXMLMessage receivedXmlMessage;
    try {
      receivedXmlMessage = getSessionService().getReceiver().receive();
    } catch (IOException e) {
      LOG.warn("SolaceIO.Read: Exception when pulling messages from the broker.", e);
      return false;
    }

    if (receivedXmlMessage == null) {
      return false;
    }
    solaceOriginalRecord = receivedXmlMessage;
    solaceMappedRecord = getCurrentSource().getParseFn().apply(receivedXmlMessage);
    receivedMessages.add(receivedXmlMessage);
    messagesReceived.inc();

    return true;
  }

  @Override
  public void close() {
    sessionServiceCache.invalidate(readerUuid);
    ActiveReadersRegistry.unregister(readerUuid);
    ackExecutor.shutdown();
    try {
      if (!ackExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        ackExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      ackExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  void finalizeCheckpoint(long checkpointId) {
    List<BytesXMLMessage> messagesToAck = new ArrayList<>();

    synchronized (lock) {
      SortedMap<Long, PendingCheckpoint> toAck = pendingCheckpoints.headMap(checkpointId, true);
      for (PendingCheckpoint cp : toAck.values()) {
        messagesToAck.addAll(cp.messages);
      }
      toAck.clear();
    }

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (BytesXMLMessage msg : messagesToAck) {
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  msg.ackMessage();
                  messagesAcked.inc();
                } catch (IllegalStateException e) {
                  LOG.warn(
                      "SolaceIO.Read: Failed to acknowledge message with applicationMessageId={}, ackMessageId={}. Session might be closed.",
                      msg.getApplicationMessageId(),
                      msg.getAckMessageId(),
                      e);
                }
              },
              ackExecutor));
    }

    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    } catch (Exception e) {
      LOG.warn("SolaceIO.Read: Exception waiting for message acknowledgements", e);
    }
  }

  private void checkTimeouts() {
    long now = clock.get();
    List<PendingCheckpoint> expired = new ArrayList<>();
    synchronized (lock) {
      while (!pendingCheckpoints.isEmpty()) {
        long oldestId = pendingCheckpoints.firstKey();
        PendingCheckpoint oldest = pendingCheckpoints.get(oldestId);
        if (oldest != null && now - oldest.timestamp > ackDeadline.toMillis()) {
          pendingCheckpoints.remove(oldestId);
          expired.add(oldest);
        } else {
          break;
        }
      }
    }

    for (PendingCheckpoint cp : expired) {
      LOG.warn(
          "SolaceIO.Read: Checkpoint {} timed out after {}ms. Nacking messages.",
          cp.id,
          now - cp.timestamp);
      for (BytesXMLMessage msg : cp.messages) {
        ackExecutor.execute(
            () -> {
              try {
                msg.settle(XMLMessage.Outcome.FAILED);
              } catch (Exception e) {
                LOG.warn("SolaceIO.Read: Failed to nack message", e);
              }
            });
      }
    }
  }

  @Override
  public Instant getWatermark() {
    // should be only used by a test receiver
    if (getSessionService().getReceiver().isEOF()) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return watermarkPolicy.getWatermark();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    long checkpointId = nextCheckpointId++;
    ImmutableList<BytesXMLMessage> messages = ImmutableList.copyOf(receivedMessages);
    receivedMessages.clear();
    synchronized (lock) {
      pendingCheckpoints.put(
          checkpointId, new PendingCheckpoint(checkpointId, messages, clock.get()));
    }
    return new SolaceCheckpointMark(readerUuid, checkpointId);
  }

  @Override
  public T getCurrent() throws NoSuchElementException {
    if (solaceMappedRecord == null) {
      throw new NoSuchElementException();
    }
    return solaceMappedRecord;
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    if (solaceOriginalRecord == null) {
      throw new NoSuchElementException();
    }
    if (solaceOriginalRecord.getApplicationMessageId() != null) {
      return checkNotNull(solaceOriginalRecord)
          .getApplicationMessageId()
          .getBytes(StandardCharsets.UTF_8);
    } else {
      return checkNotNull(solaceOriginalRecord)
          .getReplicationGroupMessageId()
          .toString()
          .getBytes(StandardCharsets.UTF_8);
    }
  }

  @Override
  public UnboundedSolaceSource<T> getCurrentSource() {
    return currentSource;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (getCurrent() == null) {
      throw new NoSuchElementException();
    }
    return currentSource.getTimestampFn().apply(getCurrent());
  }

  @Override
  public long getTotalBacklogBytes() {
    try {
      return sempClient.getBacklogBytes(currentSource.getQueue().getName());
    } catch (IOException e) {
      LOG.warn("SolaceIO.Read: Could not query backlog bytes. Returning BACKLOG_UNKNOWN", e);
      return BACKLOG_UNKNOWN;
    }
  }

  private static class PendingCheckpoint {
    final long id;
    final List<BytesXMLMessage> messages;
    final long timestamp;

    PendingCheckpoint(long id, List<BytesXMLMessage> messages, long timestamp) {
      this.id = id;
      this.messages = messages;
      this.timestamp = timestamp;
    }
  }
}
