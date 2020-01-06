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
package org.apache.beam.sdk.io.rabbitmq;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * RabbitMQ manages state based on a persistent Channel within a Connection. If the Channel is
 * broken, the state cannot be restored, and all un-acknowledged messages will be redelivered to
 * another consumer. This has important implications for the interactions between a Reader and the
 * CheckpointMark.
 *
 * <p>Importantly, for Beam:
 *
 * <ul>
 *   <li>CheckpointMarks may outlive Readers. A CheckpointMark may not be finalized until after a
 *       Reader is closed. Therefore any Channel with unacknowledged (non-"finalized") messages must
 *       remain open after the reader is closed so that the checkpoint mark can successfully
 *       acknowledge the messages later.
 *   <li>Because RabbitMQ requires a channel to be persisted to acknowledge/finalize state, a
 *       serialized/deserialized CheckpointMark with unacknowledged messages is inherently not
 *       re-usable. To accomodate this, RabbitMqUnoundedReader will not accept a CheckpointMark in
 *       its constructor and will always instantiate a new one. The RabbitIO "dedupe" (record id)
 *       strategy employed is thus mandatory for preventing redelivered messages from impacting the
 *       pipeline.
 * </ul>
 */
class RabbitMqCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
  private static final Instant MIN_WATERMARK_MILLIS = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private final UUID checkpointId;
  private final List<Long> deliveryTags = new ArrayList<>();
  private boolean reading;
  private Instant watermark = MIN_WATERMARK_MILLIS;

  private transient ChannelLeaser channelLeaser;

  public RabbitMqCheckpointMark(ChannelLeaser channelLeaser) {
    this.checkpointId = UUID.randomUUID();
    this.reading = false;
    this.channelLeaser = channelLeaser;
  }

  /**
   * Sets the internal state that the backing channel is in use by an object other than this
   * CheckpointMark. If 'is reading' is set, then the Channel will remain open after {@link
   * #finalizeCheckpoint()} is called, otherwise the Channel will be closed.
   */
  public void startReading() {
    reading = true;
  }

  /**
   * Sets the internal state that the backing channel is no longer in use by an object other than
   * this CheckpointMark. If 'is reading' is false, then the Channel will be closed after {@link
   * #finalizeCheckpoint()} is called.
   */
  public void stopReading() {
    reading = false;
  }

  @Nullable
  public Instant getWatermark() {
    return this.watermark;
  }

  public void setWatermark(Instant watermark) {
    this.watermark = watermark;
  }

  public void appendDeliveryTag(long deliveryTag) {
    deliveryTags.add(deliveryTag);
  }

  public UUID getCheckpointId() {
    return this.checkpointId;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    ChannelLeaser.UseChannelFunction<Void> finalizeFn =
        channel -> {
          for (Long deliveryTag : deliveryTags) {
            channel.basicAck(deliveryTag, false);
          }
          deliveryTags.clear();
          return null;
        };

    channelLeaser.useChannel(checkpointId, finalizeFn);

    if (!reading) {
      channelLeaser.closeChannel(checkpointId);
    }
  }
}
