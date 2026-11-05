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
package org.apache.beam.sdk.io.jms;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.jms.JmsIO.AcknowledgeMode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checkpoint for an unbounded JMS source. Consists of the JMS messages waiting to be acknowledged
 * and oldest pending message timestamp.
 *
 * <p>The checkpoints of a reader are acknowledged in order: finalizing a checkpoint also
 * acknowledges the earlier checkpoints of the same reader that were not finalized yet. A runner
 * commits a checkpoint only once all earlier checkpoints of the same reader are committed (see
 * {@link UnboundedSource.UnboundedReader#getCheckpointMark()}), so this never acknowledges messages
 * that are not committed. It covers checkpoints whose finalize callback is lost, and checkpoints
 * that are taken but never committed, e.g. when a runner calls {@code getCheckpointMark()} only to
 * report progress.
 */
class JmsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  // Pinned to the value computed for this class before the reader id and sequence were added, so
  // checkpoints written by those versions can still be restored.
  private static final long serialVersionUID = -275230511397134772L;

  private static final Logger LOG = LoggerFactory.getLogger(JmsCheckpointMark.class);

  // The reader that took this checkpoint and the position of this checkpoint among the
  // checkpoints of that reader, so that a restored copy can find the reader in this JVM. The reader
  // id is null when restored from a checkpoint written by a version without these fields.
  private final @Nullable String readerId;
  private final long sequence;
  private Instant oldestMessageTimestamp;
  private transient @Nullable List<Message> messages;
  private transient int messageCount;
  private transient @Nullable MessageConsumer consumer;
  private transient @Nullable Session session;
  private transient @Nullable AtomicInteger activeCheckpoints;
  // Null when restored, the reader is then looked up by id.
  private transient @Nullable PendingCheckpoints pendingCheckpoints;
  private transient boolean restored;

  private JmsCheckpointMark(
      @Nullable PendingCheckpoints pendingCheckpoints,
      long sequence,
      Instant oldestMessageTimestamp,
      @Nullable List<Message> messages,
      int messageCount,
      @Nullable MessageConsumer consumer,
      @Nullable Session session,
      @Nullable AtomicInteger activeCheckpoints) {
    this.pendingCheckpoints = pendingCheckpoints;
    this.readerId = pendingCheckpoints == null ? null : pendingCheckpoints.readerId;
    this.sequence = sequence;
    this.oldestMessageTimestamp = oldestMessageTimestamp;
    this.messages = messages;
    this.messageCount = messageCount;
    this.consumer = consumer;
    this.session = session;
    this.activeCheckpoints = activeCheckpoints;
  }

  /**
   * Acknowledge the messages of this checkpoint and of the earlier checkpoints of the same reader
   * that are not finalized yet. A restored copy does so if the reader that took it lives in this
   * JVM.
   */
  @Override
  public void finalizeCheckpoint() {
    PendingCheckpoints pending = pendingCheckpoints;
    if (pending == null && readerId != null) {
      pending = PendingCheckpoints.LIVE.getIfPresent(readerId);
    }
    if (pending == null) {
      LOG.debug(
          "Finalized checkpoint {}: its reader is not in this JVM, nothing to acknowledge", this);
      return;
    }
    List<JmsCheckpointMark> checkpoints = pending.removeUpTo(sequence);
    int acknowledged = 0;
    for (JmsCheckpointMark checkpoint : checkpoints) {
      acknowledged += checkpoint.acknowledge();
    }
    if (LOG.isDebugEnabled()) {
      int earlier = 0;
      for (JmsCheckpointMark checkpoint : checkpoints) {
        if (checkpoint.sequence != sequence) {
          earlier++;
        }
      }
      LOG.debug(
          "Finalized checkpoint {}{}: acknowledged {} message(s) of {} checkpoint(s), including {}"
              + " earlier checkpoint(s) not finalized before",
          this,
          restored ? " (restored copy)" : "",
          acknowledged,
          checkpoints.size(),
          earlier);
    }
  }

  /**
   * Acknowledge the messages of this checkpoint and release its consumer and session, at most once.
   * Returns the number of messages acknowledged.
   */
  private synchronized int acknowledge() {
    int acknowledged = 0;
    try {
      if (messages != null) {
        for (Message message : messages) {
          message.acknowledge();
        }
        messages = null;
        acknowledged = messageCount;
      }
    } catch (JMSException e) {
      // The effect of this is message not get acknowledged and thus will be redelivered. It is
      // not fatal, so we just raise error log. Similar below.
      LOG.error(
          "Failed to acknowledge the messages of checkpoint {}. Will redeliver and might cause"
              + " duplication.",
          this,
          e);
    }

    // session is closed after message acknowledged otherwise other consumer may receive duplicate
    // messages.
    if (consumer != null) {
      try {
        consumer.close();
        consumer = null;
      } catch (JMSException e) {
        LOG.info("Error closing JMS consumer. It may have already been closed.");
      }
    }

    // session needs to be closed after message acknowledged because the latter needs session remain
    // active.
    if (session != null) {
      try {
        session.close();
        session = null;
      } catch (JMSException e) {
        LOG.info("Error closing JMS session. It may have already been closed.");
      }
    }

    release();
    return acknowledged;
  }

  /** Drop this checkpoint without acknowledging it, once its connection is closed. */
  private synchronized int abandon() {
    int abandoned = messages == null ? 0 : messageCount;
    messages = null;
    consumer = null;
    session = null;
    release();
    return abandoned;
  }

  private void release() {
    if (activeCheckpoints != null) {
      activeCheckpoints.decrementAndGet();
      activeCheckpoints = null;
    }
  }

  @VisibleForTesting
  @Nullable List<Message> getMessages() {
    return messages;
  }

  /** Number of messages read for this checkpoint, still to be acknowledged. */
  int getMessageCount() {
    return messages == null ? 0 : messageCount;
  }

  @VisibleForTesting
  @Nullable Session getSession() {
    return session;
  }

  @VisibleForTesting
  @Nullable MessageConsumer getConsumer() {
    return consumer;
  }

  // set an empty list to messages when deserialize
  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    messages = null;
    session = null;
    consumer = null;
    restored = true;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JmsCheckpointMark)) {
      return false;
    }
    JmsCheckpointMark that = (JmsCheckpointMark) o;
    // The reader id and sequence only locate the live checkpoint, so they are not part of equality.
    return oldestMessageTimestamp.equals(that.oldestMessageTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(oldestMessageTimestamp);
  }

  @Override
  public String toString() {
    return readerId == null ? "<from an older version>" : shortId(readerId) + "#" + sequence;
  }

  static String shortId(String readerId) {
    return readerId.substring(0, Math.min(8, readerId.length()));
  }

  /**
   * The checkpoints of a reader that are not finalized yet, in the order they were taken. Live
   * readers are registered by id so that a restored checkpoint can find the reader that took it.
   */
  private static final class PendingCheckpoints {
    // Weak values: an entry goes away with its reader and checkpoints.
    private static final Cache<String, PendingCheckpoints> LIVE =
        CacheBuilder.newBuilder().weakValues().build();

    private final String readerId = UUID.randomUUID().toString();
    private final List<JmsCheckpointMark> checkpoints = new ArrayList<>();
    private long nextSequence;

    static PendingCheckpoints create() {
      PendingCheckpoints pending = new PendingCheckpoints();
      LIVE.put(pending.readerId, pending);
      return pending;
    }

    synchronized long nextSequence() {
      return nextSequence++;
    }

    synchronized void add(JmsCheckpointMark checkpoint) {
      checkpoints.add(checkpoint);
    }

    /** Remove and return the checkpoints taken up to the given sequence, oldest first. */
    synchronized List<JmsCheckpointMark> removeUpTo(long sequence) {
      List<JmsCheckpointMark> removed = new ArrayList<>();
      checkpoints.removeIf(
          checkpoint -> {
            if (checkpoint.sequence <= sequence) {
              removed.add(checkpoint);
              return true;
            }
            return false;
          });
      return removed;
    }

    /** Remove and return all checkpoints, and unregister the reader. */
    synchronized List<JmsCheckpointMark> removeAll() {
      LIVE.invalidate(readerId);
      List<JmsCheckpointMark> removed = new ArrayList<>(checkpoints);
      checkpoints.clear();
      return removed;
    }

    synchronized int size() {
      return checkpoints.size();
    }
  }

  static Preparer newPreparer(AcknowledgeMode acknowledgeMode) {
    return new Preparer(acknowledgeMode);
  }

  /**
   * A class preparing the immutable checkpoint. It is mutable so that new messages can be added.
   */
  static class Preparer {
    private Instant oldestMessageTimestamp = Instant.now();
    private transient List<Message> messages = new ArrayList<>();
    private int messageCount;
    private final AcknowledgeMode acknowledgeMode;
    private final PendingCheckpoints pendingCheckpoints = PendingCheckpoints.create();

    @VisibleForTesting transient boolean discarded = false;

    @VisibleForTesting final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Preparer(AcknowledgeMode acknowledgeMode) {
      this.acknowledgeMode = acknowledgeMode;
    }

    /** Id of the reader, shared by its checkpoints. */
    String getReaderId() {
      return pendingCheckpoints.readerId;
    }

    void add(Message message) throws JMSException {
      lock.writeLock().lock();
      try {
        if (discarded) {
          throw new IllegalStateException(
              String.format(
                  "Attempting to add message %s to checkpoint that is discarded.", message));
        }
        Instant currentMessageTimestamp = new Instant(message.getJMSTimestamp());
        if (currentMessageTimestamp.isBefore(oldestMessageTimestamp)) {
          oldestMessageTimestamp = currentMessageTimestamp;
        }
        messageCount++;
        if (acknowledgeMode == AcknowledgeMode.INDIVIDUAL_ACKNOWLEDGE) {
          messages.add(message);
        } else {
          // Jms spec will implicitly acknowledge _all_ messaged already received by the same
          // session if one message in this session is being acknowledged. Only need to ack
          // last seen one.
          if (messages.isEmpty()) {
            messages.add(message);
          } else {
            messages.set(0, message);
          }
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    Instant getOldestMessageTimestamp() {
      lock.readLock().lock();
      try {
        return this.oldestMessageTimestamp;
      } finally {
        lock.readLock().unlock();
      }
    }

    void discard() {
      lock.writeLock().lock();
      try {
        messages.clear();
        messageCount = 0;
        this.discarded = true;
      } finally {
        lock.writeLock().unlock();
      }
    }

    /**
     * Create a new checkpoint mark based on the current preparer. This will reset the messages held
     * by the preparer. If AcknowledgeMode is CLIENT_ACKNOWLEDGE, the owner of the preparer is
     * responsible to create a new Jms session after this call.
     */
    JmsCheckpointMark newCheckpoint(
        @Nullable MessageConsumer consumer,
        @Nullable Session session,
        @Nullable AcknowledgeMode acknowledgeMode,
        @Nullable AtomicInteger activeCheckpoints) {
      JmsCheckpointMark checkpointMark;
      lock.writeLock().lock();
      try {
        if (discarded) {
          messages.clear();
          checkpointMark = this.emptyCheckpoint();
        } else {
          List<Message> messagesCopy = null;
          MessageConsumer consumerToPass = null;
          Session sessionToPass = null;
          if (!messages.isEmpty()) {
            messagesCopy = new ArrayList<>(messages);
          }
          if (acknowledgeMode == AcknowledgeMode.CLIENT_ACKNOWLEDGE) {
            consumerToPass = consumer;
            sessionToPass = session;
          }
          checkpointMark =
              new JmsCheckpointMark(
                  pendingCheckpoints,
                  pendingCheckpoints.nextSequence(),
                  oldestMessageTimestamp,
                  messagesCopy,
                  messageCount,
                  consumerToPass,
                  sessionToPass,
                  activeCheckpoints);
          messages.clear();
          messageCount = 0;
          oldestMessageTimestamp = Instant.now();
          pendingCheckpoints.add(checkpointMark);
          if (activeCheckpoints != null) {
            activeCheckpoints.incrementAndGet();
          }
        }
      } finally {
        lock.writeLock().unlock();
      }
      return checkpointMark;
    }

    /**
     * A checkpoint without messages. Finalizing it still acknowledges the earlier checkpoints of
     * this reader.
     */
    JmsCheckpointMark emptyCheckpoint() {
      return new JmsCheckpointMark(
          pendingCheckpoints,
          pendingCheckpoints.nextSequence(),
          oldestMessageTimestamp,
          null,
          0,
          null,
          null,
          null);
    }

    boolean isEmpty() {
      return messages.isEmpty();
    }

    /** Number of checkpoints of this reader that are not finalized yet. */
    int pendingCheckpointCount() {
      return pendingCheckpoints.size();
    }

    /**
     * Drop the checkpoints of this reader that are not finalized yet, when its connection is
     * closed: their messages can no longer be acknowledged and the broker redelivers them. Returns
     * the number of messages dropped.
     */
    int abandonPendingCheckpoints() {
      int abandoned = 0;
      for (JmsCheckpointMark checkpoint : pendingCheckpoints.removeAll()) {
        abandoned += checkpoint.abandon();
      }
      return abandoned;
    }
  }
}
