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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.solace.broker.MessageReceiver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checkpoint for an unbounded Solace source. Consists of the Solace messages waiting to be
 * acknowledged and oldest pending message timestamp.
 */
@Internal
@DefaultCoder(AvroCoder.class)
public class SolaceCheckpointMark implements UnboundedSource.CheckpointMark {
  private static final Logger LOG = LoggerFactory.getLogger(SolaceCheckpointMark.class);
  private transient AtomicBoolean activeReader;
  private ArrayDeque<Long> ackQueue;
  private AtomicReference<MessageReceiver> sessionServiceRef;

  @SuppressWarnings("initialization") // Avro will set the fields by breaking abstraction
  private SolaceCheckpointMark() {}

  public SolaceCheckpointMark(
      AtomicBoolean activeReader,
      List<Long> ackQueue,
      AtomicReference<MessageReceiver> messageReceiverRef) {
    this.activeReader = activeReader;
    this.ackQueue = new ArrayDeque<>(ackQueue);
    this.sessionServiceRef = messageReceiverRef;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    if (activeReader == null || !activeReader.get() || ackQueue == null) {
      return;
    }
    MessageReceiver receiver = sessionServiceRef.get();
    if (receiver == null) {
      LOG.warn(
          "SolaceIO: MessageReceiver is null, can't ack messages. They will" + " be redelivered.");
      return;
    }
    while (ackQueue.size() > 0) {
      Long ackId = ackQueue.poll();
      if (ackId != null) {
        receiver.ack(ackId);
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
    // Needed to convert to ArrayList because ArrayDeque.equals checks only for reference, not
    // content.
    ArrayList<Long> ackList = new ArrayList<>(ackQueue);
    ArrayList<Long> thatAckList = new ArrayList<>(that.ackQueue);
    return Objects.equals(activeReader, that.activeReader) && Objects.equals(ackList, thatAckList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(activeReader, ackQueue);
  }
}
