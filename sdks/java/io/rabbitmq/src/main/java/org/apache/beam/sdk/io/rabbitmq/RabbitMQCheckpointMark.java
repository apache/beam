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
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

@DefaultCoder(SerializableCoder.class)
class RabbitMQCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
  private static final Instant MIN_WATERMARK_MILLIS = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private final UUID checkpointId;
  private final List<Long> sessionIds = new ArrayList<>();
  private boolean reading;
  private Instant watermark = MIN_WATERMARK_MILLIS;

  private transient ChannelLeaser channelLeaser;

  public RabbitMQCheckpointMark(ChannelLeaser channelLeaser) {
    this.checkpointId = UUID.randomUUID();
    this.reading = false;
    this.channelLeaser = channelLeaser;
  }

  public void startReading() {
    reading = true;
  }

  public void stopReading() {
    reading = false;
  }

  public void setChannelLeaser(ChannelLeaser leaser) {
    this.channelLeaser = leaser;
  }

  @Nullable
  public Instant getWatermark() {
    return this.watermark;
  }

  public void setWatermark(Instant watermark) {
    this.watermark = watermark;
  }

  public void appendSessionId(long sessionId) {
    sessionIds.add(sessionId);
  }

  public UUID getCheckpointId() {
    return this.checkpointId;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    ChannelLeaser.UseChannelFunction<Void> finalizeFn =
        (channel) -> {
          for (Long sessionId : sessionIds) {
            channel.basicAck(sessionId, false);
          }
          sessionIds.clear();
          return null;
        };

    channelLeaser.useChannel(checkpointId, finalizeFn);

    if (!reading) {
      channelLeaser.closeChannel(checkpointId);
    }
  }
}
