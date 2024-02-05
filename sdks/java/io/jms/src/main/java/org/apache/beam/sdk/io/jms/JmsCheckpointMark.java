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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.JMSException;
import javax.jms.Message;
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
  private transient List<Message> messages;
  private transient @Nullable Session session;

  @VisibleForTesting final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private JmsCheckpointMark(
      Instant oldestMessageTimestamp, List<Message> messages, @Nullable Session session) {
    this.oldestMessageTimestamp = oldestMessageTimestamp;
    this.messages = messages;
    this.session = session;
  }

  /**
   * Acknowledge all outstanding message. Since we believe that messages will be delivered in
   * timestamp order, and acknowledged messages will not be retried, the newest message in this
   * batch is a good bound for future messages.
   */
  @Override
  public void finalizeCheckpoint() {
    // finalizeCheckpoint might be called multiple times
    lock.writeLock().lock();
    try {
      for (Message message : messages) {
        try {
          // Jms spec will implicitly acknowledge _all_ messaged already received by the same
          // session if one message in this session is being acknowledged. However, different
          // implementations may or may not use per-message acknowledgement. So we acknowledge
          // every message handled by the checkpoint.
          message.acknowledge();
          Instant currentMessageTimestamp = new Instant(message.getJMSTimestamp());
          if (currentMessageTimestamp.isAfter(oldestMessageTimestamp)) {
            oldestMessageTimestamp = currentMessageTimestamp;
          }
        } catch (Exception e) {
          LOG.error("Exception while finalizing message: ", e);
        }
      }
      messages.clear();
    } finally {
      try {
        if (session != null) {
          // non-null session means the session is now owned by the checkpoint. It is now closed
          // after messages acknowledged.
          session.close();
          session = null;
        }
      } catch (JMSException e) {
        LOG.info("Error closing JMS session. It may have already been closed.");
      }
      lock.writeLock().unlock();
    }
  }

  // set an empty list to messages when deserialize
  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    messages = new ArrayList<>();
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
    private transient List<Message> messages = new ArrayList<>();

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
        messages.add(message);
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
    JmsCheckpointMark newCheckpoint(@Nullable Session session) {
      JmsCheckpointMark checkpointMark;
      lock.writeLock().lock();
      try {
        if (discarded) {
          messages.clear();
          checkpointMark = new JmsCheckpointMark(oldestMessageTimestamp, messages, null);
        } else {
          checkpointMark = new JmsCheckpointMark(oldestMessageTimestamp, messages, session);
          messages = new ArrayList<>();
          oldestMessageTimestamp = Instant.now();
        }
      } finally {
        lock.writeLock().unlock();
      }
      return checkpointMark;
    }
  }
}
