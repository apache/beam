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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Checkpoint for an unbounded Solace source. Consists of the Solace messages waiting to be
 * acknowledged.
 */
@DefaultCoder(AvroCoder.class)
@Internal
@VisibleForTesting
public class SolaceCheckpointMark implements UnboundedSource.CheckpointMark {
  private transient AtomicBoolean activeReader;
  // BytesXMLMessage is not serializable so if a job restarts from the checkpoint, we cannot retry
  // these messages here. We relay on Solace's retry mechanism.
  private transient ArrayDeque<BytesXMLMessage> ackQueue;

  @SuppressWarnings("initialization") // Avro will set the fields by breaking abstraction
  private SolaceCheckpointMark() {}

  /**
   * Creates a new {@link SolaceCheckpointMark}.
   *
   * @param activeReader {@link AtomicBoolean} indicating if the related reader is active. The
   *     reader creating the messages has to be active to acknowledge the messages.
   * @param ackQueue {@link List} of {@link BytesXMLMessage} to be acknowledged.
   */
  SolaceCheckpointMark(AtomicBoolean activeReader, List<BytesXMLMessage> ackQueue) {
    this.activeReader = activeReader;
    this.ackQueue = new ArrayDeque<>(ackQueue);
  }

  @Override
  public void finalizeCheckpoint() {
    if (activeReader == null || !activeReader.get() || ackQueue == null) {
      return;
    }

    while (!ackQueue.isEmpty()) {
      BytesXMLMessage msg = ackQueue.poll();
      if (msg != null) {
        msg.ackMessage();
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
    ArrayList<BytesXMLMessage> ackList = new ArrayList<>(ackQueue);
    ArrayList<BytesXMLMessage> thatAckList = new ArrayList<>(that.ackQueue);
    return Objects.equals(activeReader, that.activeReader) && Objects.equals(ackList, thatAckList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(activeReader, ackQueue);
  }
}
