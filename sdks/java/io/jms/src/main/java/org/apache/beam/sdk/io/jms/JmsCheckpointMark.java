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
import javax.jms.JMSException;
import javax.jms.Message;
import org.apache.beam.sdk.io.UnboundedSource;
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

  private transient JmsIO.UnboundedJmsReader<?> reader;
  private transient List<Message> messagesToAck;
  private final int readerHash;

  JmsCheckpointMark(JmsIO.UnboundedJmsReader<?> reader, @Nullable List<Message> messagesToAck) {
    this.reader = reader;
    this.messagesToAck = messagesToAck;
    this.readerHash = System.identityHashCode(reader);
  }

  // set an empty list to messages when deserialize
  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    messagesToAck = new ArrayList<>();
  }

  /**
   * Acknowledge all outstanding message. Since we believe that messages will be delivered in
   * timestamp order, and acknowledged messages will not be retried, the newest message in this
   * batch is a good bound for future messages.
   */
  @Override
  public void finalizeCheckpoint() throws IOException {
    try {
      LOG.debug("Finalize Checkpoint {} {}", reader, messagesToAck.size());
      drainMessages();
    } catch (JMSException e) {
      throw new IOException("Exception while finalizing message ", e);
    }
  }

  protected void drainMessages() throws JMSException {
    for (Message message : messagesToAck) {
      message.acknowledge();
      Instant currentMessageTimestamp = new Instant(message.getJMSTimestamp());
      reader.watermark.updateAndGet(prev -> Math.max(currentMessageTimestamp.getMillis(), prev));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JmsCheckpointMark)) {
      return false;
    }
    JmsCheckpointMark that = (JmsCheckpointMark) o;
    return readerHash == that.readerHash;
  }

  @Override
  public int hashCode() {
    return readerHash;
  }
}
