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

import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

class RabbitMqUnboundedReader extends UnboundedSource.UnboundedReader<RabbitMqMessage> {

  private final RabbitMqSource source;

  private TimestampPolicy timestampPolicy;
  private RecordIdPolicy recordIdPolicy;
  private TimestampPolicyContext context;
  private RabbitMqMessage currentRecord;
  private String queueName;
  private final RabbitMqCheckpointMark checkpointMark;
  private final ConnectionHandler connectionHandler;

  RabbitMqUnboundedReader(RabbitMqSource source, RabbitMqCheckpointMark checkpointMark) {
    this.source = source;
    this.currentRecord = null;
    this.recordIdPolicy = source.spec.recordIdPolicy();
    this.context =
        new TimestampPolicyContext(
            Long.valueOf(UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN).intValue(),
            BoundedWindow.TIMESTAMP_MIN_VALUE);
    this.connectionHandler = this.source.spec.connectionHandlerProviderFn().apply(null);

    RabbitMqCheckpointMark cpMark = checkpointMark;
    if (cpMark == null) {
      cpMark = new RabbitMqCheckpointMark(this.connectionHandler);
    } else {
      cpMark.setChannelLeaser(this.connectionHandler);
    }
    this.checkpointMark = cpMark;
    ensureTimestampPolicySet(
        Optional.ofNullable(checkpointMark).map(RabbitMqCheckpointMark::getWatermark));
  }

  private void ensureTimestampPolicySet(Optional<Instant> prevWatermark) {
    if (this.timestampPolicy == null) {
      this.timestampPolicy =
          source.spec.timestampPolicyFactory().createTimestampPolicy(prevWatermark);
    }
  }

  @Override
  public Instant getWatermark() {
    Instant watermark = timestampPolicy.getWatermark(context, currentRecord);
    checkpointMark.setWatermark(watermark);
    return watermark;
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return checkpointMark;
  }

  @Override
  public RabbitMqSource getCurrentSource() {
    return source;
  }

  @Override
  public byte[] getCurrentRecordId() {
    return Optional.ofNullable(currentRecord).map(recordIdPolicy).get();
  }

  @Override
  public Instant getCurrentTimestamp() {
    return Optional.ofNullable(currentRecord).map(timestampPolicy::getTimestampForRecord).get();
  }

  @Override
  public RabbitMqMessage getCurrent() {
    if (currentRecord == null) {
      throw new NoSuchElementException();
    }
    return currentRecord;
  }

  @Override
  public boolean start() throws IOException {
    try {
      checkpointMark.startReading();

      // TODO: rework all of this
      queueName = source.spec.queue();

      ChannelLeaser.UseChannelFunction<Void> setupFn =
          channel -> {
            if (source.spec.queueDeclare()) {
              // declare the queue (if not done by another application)
              // channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
              channel.queueDeclare(queueName, true, false, false, null);
            }

            if (queueName == null) {
              queueName = channel.queueDeclare().getQueue();
            }

            String routingKey = source.spec.routingKey();
            if ("direct".equalsIgnoreCase(source.spec.exchangeType())
                || "fanout".equalsIgnoreCase(source.spec.exchangeType())) {
              // pubsub and direct exchanges do not require a routing key to be defined as the
              // exchange type
              // dictates the routing semantics
              routingKey = null;
            }
            channel.queueBind(queueName, source.spec.exchange(), routingKey);

            return null;
          };

      connectionHandler.useChannel(checkpointMark.getCheckpointId(), setupFn);
    } catch (IOException e) {
      checkpointMark.stopReading();
      throw e;
    } catch (Exception e) {
      checkpointMark.stopReading();
      throw new IOException(e);
    }
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    try {
      // we consume message without autoAck (we want to do the ack ourselves)
      GetResponse delivery =
          connectionHandler.useChannel(
              checkpointMark.getCheckpointId(),
              channel -> channel.basicGet(queueName, /* autoAck: */ false));

      if (delivery == null) {
        currentRecord = null;
        // queue is empty, so there is no backlog
        context = new TimestampPolicyContext(0, Instant.now());
        return false;
      }

      currentRecord = RabbitMqMessage.fromGetResponse(delivery);
      context = new TimestampPolicyContext(delivery.getMessageCount(), Instant.now());
      long deliveryTag = delivery.getEnvelope().getDeliveryTag();
      checkpointMark.appendDeliveryTag(deliveryTag);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public void close() {
    checkpointMark.stopReading();
  }

  private static class TimestampPolicyContext extends TimestampPolicy.LastRead {
    private final int messageBacklog;
    private final Instant backlogCheckTime;

    public TimestampPolicyContext(int messageBacklog, Instant backlogCheckTime) {
      this.messageBacklog = messageBacklog;
      this.backlogCheckTime = backlogCheckTime;
    }

    @Override
    public int getMessageBacklog() {
      return messageBacklog;
    }

    @Override
    public Instant getBacklogCheckTime() {
      return backlogCheckTime;
    }
  }
}
