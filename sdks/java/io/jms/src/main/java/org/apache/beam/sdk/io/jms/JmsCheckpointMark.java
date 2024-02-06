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
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.beam.sdk.io.UnboundedSource;
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
  private transient @Nullable Message lastMessage;
  private transient @Nullable MessageConsumer consumer;
  private transient @Nullable Session session;

  private JmsCheckpointMark(
      Instant oldestMessageTimestamp,
      @Nullable Message lastMessage,
      @Nullable MessageConsumer consumer,
      @Nullable Session session) {
    this.oldestMessageTimestamp = oldestMessageTimestamp;
    this.lastMessage = lastMessage;
    this.consumer = consumer;
    this.session = session;
  }

  /** Acknowledge all outstanding message. */
  @Override
  public void finalizeCheckpoint() {
    try {
      // Jms spec will implicitly acknowledge _all_ messaged already received by the same
      // session if one message in this session is being acknowledged.
      if (lastMessage != null) {
        lastMessage.acknowledge();
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
  }

  // set an empty list to messages when deserialize
  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    lastMessage = null;
    session = null;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JmsCheckpointMark that = (JmsCheckpointMark) o;
    return oldestMessageTimestamp.equals(that.oldestMessageTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(oldestMessageTimestamp);
  }

  static Preparer newPreparer() {
    return new Preparer();
  }

  /**
   * A class preparing the immutable checkpoint. It is mutable so that new messages can be added.
   */
  static class Preparer {
    private Instant oldestMessageTimestamp = Instant.now();
    private transient @Nullable Message lastMessage = null;

    @VisibleForTesting transient boolean discarded = false;

    @VisibleForTesting final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Preparer() {}

    void add(Message message) throws Exception {
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
        lastMessage = message;
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
        this.discarded = true;
      } finally {
        lock.writeLock().unlock();
      }
    }

    /**
     * Create a new checkpoint mark based on the current preparer. This will reset the messages held
     * by the preparer, and the owner of the preparer is responsible to create a new Jms session
     * after this call.
     */
    JmsCheckpointMark newCheckpoint(@Nullable MessageConsumer consumer, @Nullable Session session) {
      JmsCheckpointMark checkpointMark;
      lock.writeLock().lock();
      try {
        if (discarded) {
          lastMessage = null;
          checkpointMark = this.emptyCheckpoint();
        } else {
          checkpointMark =
              new JmsCheckpointMark(oldestMessageTimestamp, lastMessage, consumer, session);
          lastMessage = null;
          oldestMessageTimestamp = Instant.now();
        }
      } finally {
        lock.writeLock().unlock();
      }
      return checkpointMark;
    }

    JmsCheckpointMark emptyCheckpoint() {
      return new JmsCheckpointMark(oldestMessageTimestamp, null, null, null);
    }

    boolean isEmpty() {
      return lastMessage == null;
    }
  }
}
