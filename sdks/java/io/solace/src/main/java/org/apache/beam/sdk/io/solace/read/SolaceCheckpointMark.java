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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.UnboundedSource;
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
  @Nullable private transient ConcurrentLinkedDeque<BytesXMLMessage> ackQueue;

  @SuppressWarnings("initialization") // Avro will set the fields by breaking abstraction
  private SolaceCheckpointMark() {} // for Avro

  public SolaceCheckpointMark(
      @Nullable AtomicBoolean activeReader, List<BytesXMLMessage> ackQueue) {
    this.activeReader = activeReader;
    if (ackQueue != null) {
      this.ackQueue = new ConcurrentLinkedDeque<>(ackQueue);
    }
  }

  @Override
  public void finalizeCheckpoint() {
    if (activeReader == null || !activeReader.get() || ackQueue == null) {
      return;
    }

    LOG.debug(
        "SolaceIO.Read: SolaceCheckpointMark: Started to finalize {} with {}  messages.",
        this.getClass().getSimpleName(),
        ackQueue.size());

    while (ackQueue.size() > 0) {
      BytesXMLMessage msg = ackQueue.poll();
      if (msg != null) {
        msg.ackMessage();
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SolaceCheckpointMark)) {
      return false;
    }
    SolaceCheckpointMark that = (SolaceCheckpointMark) o;
    return Objects.equals(activeReader, that.activeReader)
        && Objects.equals(ackQueue, that.ackQueue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(activeReader, ackQueue);
  }
}
