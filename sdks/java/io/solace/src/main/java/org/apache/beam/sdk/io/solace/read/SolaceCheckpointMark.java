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

import com.solacesystems.jcsmp.BytesXMLMessage;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checkpoint for an unbounded Solace source. Consists of the Solace messages waiting to be
 * acknowledged.
 */
@DefaultCoder(AvroCoder.class)
@Internal
@VisibleForTesting
public class SolaceCheckpointMark implements UnboundedSource.CheckpointMark {
  private static final Logger LOG = LoggerFactory.getLogger(SolaceCheckpointMark.class);
  private transient Map<Long, BytesXMLMessage> safeToAck;
  private transient Consumer<Long> confirmAckCallback;

  @SuppressWarnings("initialization") // Avro will set the fields by breaking abstraction
  private SolaceCheckpointMark() {}

  /**
   * Creates a new {@link SolaceCheckpointMark}.
   *
   * @param markAsAckedFn {@link Consumer<Long>} a reference to a method in the {@link
   *     UnboundedSolaceReader} that will mark the message as acknowledged.
   * @param safeToAck {@link Map<Long, BytesXMLMessage>} of {@link BytesXMLMessage} to be
   *     acknowledged.
   */
  SolaceCheckpointMark(Consumer<Long> markAsAckedFn, Map<Long, BytesXMLMessage> safeToAck) {
    this.confirmAckCallback = markAsAckedFn;
    this.safeToAck = safeToAck;
  }

  @Override
  public void finalizeCheckpoint() {
    if (safeToAck == null) {
      return;
    }

    for (Entry<Long, BytesXMLMessage> entry : safeToAck.entrySet()) {
      BytesXMLMessage msg = entry.getValue();
      if (msg != null) {
        try {
          msg.ackMessage();
        } catch (IllegalStateException e) {
          LOG.error(
              "SolaceIO.Read: cannot acknowledge the message with applicationMessageId={}, ackMessageId={} .",
              msg.getApplicationMessageId(),
              msg.getAckMessageId(),
              e);
        }
        confirmAckCallback.accept(entry.getKey());
      }
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof SolaceCheckpointMark)) {
      return false;
    }
    SolaceCheckpointMark that = (SolaceCheckpointMark) o;
    return Objects.equals(safeToAck, that.safeToAck)
        && Objects.equals(confirmAckCallback, that.confirmAckCallback);
  }

  @Override
  public int hashCode() {
    return Objects.hash(safeToAck, confirmAckCallback);
  }
}
