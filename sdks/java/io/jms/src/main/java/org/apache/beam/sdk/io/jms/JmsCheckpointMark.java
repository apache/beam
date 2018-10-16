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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.jms.Message;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checkpoint for an unbounded JmsIO.Read. Consists of JMS destination name, and the latest message
 * ID consumed so far.
 */
@DefaultCoder(AvroCoder.class)
public class JmsCheckpointMark implements UnboundedSource.CheckpointMark {

  private static final Logger LOG = LoggerFactory.getLogger(JmsCheckpointMark.class);

  private final State state = new State();

  public JmsCheckpointMark() {}

  protected List<Message> getMessages() {
    return state.getMessages();
  }

  protected void addMessage(Message message) throws Exception {
    Instant currentMessageTimestamp = new Instant(message.getJMSTimestamp());
    state.atomicWrite(
        () -> {
          state.updateOldestPendingTimestampIf(currentMessageTimestamp, Instant::isBefore);
          state.addMessage(message);
        });
  }

  protected Instant getOldestPendingTimestamp() {
    return state.getOldestPendingTimestamp();
  }

  /**
   * Acknowledge all outstanding message. Since we believe that messages will be delivered in
   * timestamp order, and acknowledged messages will not be retried, the newest message in this
   * batch is a good bound for future messages.
   */
  @Override
  public void finalizeCheckpoint() {
    State snapshot = state.snapshot();
    for (Message message : snapshot.messages) {
      try {
        message.acknowledge();
        Instant currentMessageTimestamp = new Instant(message.getJMSTimestamp());
        snapshot.updateOldestPendingTimestampIf(currentMessageTimestamp, Instant::isAfter);
      } catch (Exception e) {
        LOG.error("Exception while finalizing message: {}", e);
      }
    }
    state.atomicWrite(
        () -> {
          state.removeMessages(snapshot.messages);
          state.updateOldestPendingTimestampIf(snapshot.oldestPendingTimestamp, Instant::isAfter);
        });
  }

  /**
   * Encapsulates the state of a checkpoint mark; the list of messages pending finalisation and the
   * oldest pending timestamp. Read/write-exclusive access is provided throughout, and constructs
   * allowing multiple operations to be performed atomically -- i.e. performed within the context of
   * a single lock operation -- are made available.
   */
  private class State {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final List<Message> messages;
    private Instant oldestPendingTimestamp;

    public State() {
      this(new ArrayList<>(), BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    private State(List<Message> messages, Instant oldestPendingTimestamp) {
      this.messages = messages;
      this.oldestPendingTimestamp = oldestPendingTimestamp;
    }

    /**
     * Create and return a copy of the current state.
     *
     * @return A new {@code State} instance which is a deep copy of the target instance at the time
     *     of execution.
     */
    public State snapshot() {
      return atomicRead(() -> new State(new ArrayList<>(messages), oldestPendingTimestamp));
    }

    public Instant getOldestPendingTimestamp() {
      return atomicRead(() -> oldestPendingTimestamp);
    }

    public List<Message> getMessages() {
      return atomicRead(() -> messages);
    }

    public void addMessage(Message message) {
      atomicWrite(() -> messages.add(message));
    }

    public void removeMessages(List<Message> messages) {
      atomicWrite(() -> this.messages.removeAll(messages));
    }

    /**
     * Conditionally sets {@code oldestPendingTimestamp} to the value of the supplied {@code
     * candidate}, iff the provided {@code check} yields true for the {@code candidate} when called
     * with the existing {@code oldestPendingTimestamp} value.
     *
     * @param candidate The potential new value.
     * @param check The comparison method to call on {@code candidate} passing the existing {@code
     *     oldestPendingTimestamp} value as a parameter.
     */
    private void updateOldestPendingTimestampIf(
        Instant candidate, BiFunction<Instant, Instant, Boolean> check) {
      atomicWrite(
          () -> {
            if (check.apply(candidate, oldestPendingTimestamp)) {
              oldestPendingTimestamp = candidate;
            }
          });
    }

    /**
     * Call the provided {@link Supplier} under this State's read lock and return its result.
     *
     * @param operation The code to execute in the context of this State's read lock.
     * @param <T> The return type of the provided {@link Supplier}.
     * @return The value produced by the provided {@link Supplier}.
     */
    public <T> T atomicRead(Supplier<T> operation) {
      lock.readLock().lock();
      try {
        return operation.get();
      } finally {
        lock.readLock().unlock();
      }
    }

    /**
     * Call the provided {@link Runnable} under this State's write lock.
     *
     * @param operation The code to execute in the context of this State's write lock.
     */
    public void atomicWrite(Runnable operation) {
      lock.writeLock().lock();
      try {
        operation.run();
      } finally {
        lock.writeLock().unlock();
      }
    }
  }
}
