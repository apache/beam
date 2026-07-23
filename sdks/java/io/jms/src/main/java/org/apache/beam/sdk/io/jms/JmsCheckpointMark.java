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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.jms.JmsIO.AcknowledgeMode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checkpoint for an unbounded JMS source. Consists of the JMS messages waiting to be acknowledged
 * and oldest pending message timestamp.
 */
class JmsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JmsCheckpointMark.class);

  private Instant oldestMessageTimestamp;
  private transient @Nullable List<Message> messages;
  private transient @Nullable MessageConsumer consumer;
  private transient @Nullable Session session;
  private transient @Nullable AtomicInteger activeCheckpoints;

  private JmsCheckpointMark(
      Instant oldestMessageTimestamp,
      @Nullable List<Message> messages,
      @Nullable MessageConsumer consumer,
      @Nullable Session session,
      @Nullable AtomicInteger activeCheckpoints) {
    this.oldestMessageTimestamp = oldestMessageTimestamp;
    this.messages = messages;
    this.consumer = consumer;
    this.session = session;
    this.activeCheckpoints = activeCheckpoints;
  }

  /** Acknowledge all outstanding message. */
  @Override
  public void finalizeCheckpoint() {
    try {
      if (messages != null) {
        for (Message message : messages) {
          message.acknowledge();
        }
      }
    } catch (JMSException e) {
      // The effect of this is message not get acknowledged and thus will be redelivered. It is
      // not fatal, so we just raise error log. Similar below.
      LOG.error(
          "Failed to acknowledge the message. Will redeliver and might cause duplication.", e);
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

    if (activeCheckpoints != null) {
      activeCheckpoints.decrementAndGet();
    }
  }

  @VisibleForTesting
  @Nullable
  List<Message> getMessages() {
    return messages;
  }

  @VisibleForTesting
  @Nullable
  Session getSession() {
    return session;
  }

  @VisibleForTesting
  @Nullable
  MessageConsumer getConsumer() {
    return consumer;
  }

  // set an empty list to messages when deserialize
  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    messages = null;
    session = null;
    consumer = null;
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
    return oldestMessageTimestamp.equals(that.oldestMessageTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(oldestMessageTimestamp);
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
    private final AcknowledgeMode acknowledgeMode;

    @VisibleForTesting transient boolean discarded = false;

    @VisibleForTesting final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Preparer(AcknowledgeMode acknowledgeMode) {
      this.acknowledgeMode = acknowledgeMode;
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
                  oldestMessageTimestamp,
                  messagesCopy,
                  consumerToPass,
                  sessionToPass,
                  activeCheckpoints);
          messages.clear();
          oldestMessageTimestamp = Instant.now();
          if (activeCheckpoints != null) {
            activeCheckpoints.incrementAndGet();
          }
        }
      } finally {
        lock.writeLock().unlock();
      }
      return checkpointMark;
    }

    JmsCheckpointMark emptyCheckpoint() {
      return new JmsCheckpointMark(oldestMessageTimestamp, null, null, null, null);
    }

    boolean isEmpty() {
      return messages.isEmpty();
    }
  }
}
