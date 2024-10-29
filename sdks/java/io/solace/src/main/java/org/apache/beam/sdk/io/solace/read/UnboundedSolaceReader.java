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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.solace.broker.SempClient;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
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
  private @Nullable BytesXMLMessage solaceOriginalRecord;
  private @Nullable T solaceMappedRecord;
  private @Nullable SessionService sessionService;
  AtomicBoolean active = new AtomicBoolean(true);

  /**
   * List of successfully ACKed message (surrogate) ids which need to be pruned from the above.
   * CAUTION: Accessed by both reader and checkpointing threads.
   */
  private final Queue<Long> ackedMessageIds;

  /**
   * Map to place advanced messages before {@link #getCheckpointMark()} is called. This is a
   * non-concurrent object, should only be accessed by the reader thread.
   */
  private final Map<Long, BytesXMLMessage> safeToAckMessages;

  /**
   * Surrogate id used as a key in Collections storing messages that are waiting to be acknowledged
   * ({@link UnboundedSolaceReader#safeToAckMessages}) and already acknowledged ({@link
   * UnboundedSolaceReader#ackedMessageIds}).
   */
  private Long surrogateId = 0L;

  public UnboundedSolaceReader(UnboundedSolaceSource<T> currentSource) {
    this.currentSource = currentSource;
    this.watermarkPolicy =
        WatermarkPolicy.create(
            currentSource.getTimestampFn(), currentSource.getWatermarkIdleDurationThreshold());
    this.sessionService = currentSource.getSessionServiceFactory().create();
    this.sempClient = currentSource.getSempClientFactory().create();
    this.safeToAckMessages = new HashMap<>();
    this.ackedMessageIds = new ConcurrentLinkedQueue<>();
  }

  @Override
  public boolean start() {
    populateSession();
    checkNotNull(sessionService).getReceiver().start();
    return advance();
  }

  public void populateSession() {
    if (sessionService == null) {
      sessionService = getCurrentSource().getSessionServiceFactory().create();
    }
    if (sessionService.isClosed()) {
      checkNotNull(sessionService).connect();
    }
  }

  @Override
  public boolean advance() {
    // Retire state associated with ACKed messages.
    retire();

    BytesXMLMessage receivedXmlMessage;
    try {
      receivedXmlMessage = checkNotNull(sessionService).getReceiver().receive();
    } catch (IOException e) {
      LOG.warn("SolaceIO.Read: Exception when pulling messages from the broker.", e);
      return false;
    }

    if (receivedXmlMessage == null) {
      return false;
    }
    solaceOriginalRecord = receivedXmlMessage;
    solaceMappedRecord = getCurrentSource().getParseFn().apply(receivedXmlMessage);
    safeToAckMessages.put(surrogateId, receivedXmlMessage);
    surrogateId++;

    return true;
  }

  @Override
  public void close() {
    active.set(false);
    checkNotNull(sessionService).close();
  }

  @Override
  public Instant getWatermark() {
    // should be only used by a test receiver
    if (checkNotNull(sessionService).getReceiver().isEOF()) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return watermarkPolicy.getWatermark();
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    // It's possible for a checkpoint to be taken but never finalized.
    // So we simply copy whatever safeToAckIds we currently have.
    Map<Long, BytesXMLMessage> snapshotSafeToAckMessages = Maps.newHashMap(safeToAckMessages);
    return new SolaceCheckpointMark(this::markAsAcked, active, snapshotSafeToAckMessages);
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

  public void markAsAcked(Long messageSurrogateId) {
    ackedMessageIds.add(messageSurrogateId);
  }

  /**
   * Messages which have been ACKed (via the checkpoint finalize) can be safely removed from the
   * list of messages to acknowledge.
   */
  private void retire() {
    while (!ackedMessageIds.isEmpty()) {
      Long ackMessageId = ackedMessageIds.poll();
      if (ackMessageId != null) {
        safeToAckMessages.remove(ackMessageId);
      }
    }
  }
}
