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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

class UnboundedRabbitMqReader extends UnboundedSource.UnboundedReader<RabbitMqMessage> {

  private final RabbitMQSource source;

  private TimestampPolicy timestampPolicy;
  private RecordIdPolicy recordIdPolicy;
  private TimestampPolicyContext context;
  private RabbitMqMessage currentRecord;
  private String queueName;
  private final RabbitMQCheckpointMark checkpointMark;
  private final ConnectionHandler connectionHandler;

  UnboundedRabbitMqReader(RabbitMQSource source, RabbitMQCheckpointMark checkpointMark) {
    this.source = source;
    this.currentRecord = null;
    this.recordIdPolicy = source.spec.recordIdPolicy();
    this.context = new TimestampPolicyContext(true, BoundedWindow.TIMESTAMP_MIN_VALUE);
    this.connectionHandler = this.source.spec.connectionHandlerProviderFn().apply(null);

    RabbitMQCheckpointMark cpMark = checkpointMark;
    if (cpMark == null) {
      cpMark = new RabbitMQCheckpointMark(this.connectionHandler);
    } else {
      cpMark.setChannelLeaser(this.connectionHandler);
    }
    this.checkpointMark = cpMark;
    mkTimestampPolicy(
        Optional.ofNullable(checkpointMark).map(RabbitMQCheckpointMark::getWatermark));
  }

  private TimestampPolicy mkTimestampPolicy(Optional<Instant> prevWatermark) {
    if (this.timestampPolicy == null) {
      this.timestampPolicy =
          source.spec.timestampPolicyFactory().createTimestampPolicy(prevWatermark);
    }
    return this.timestampPolicy;
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
  public RabbitMQSource getCurrentSource() {
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
      Channel channel = checkpointMark.getChannel();
      checkpointMark.startReading();

      // TODO: rework all of this
      queueName = source.spec.queue();
      if (source.spec.queueDeclare()) {
        // declare the queue (if not done by another application)
        // channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
        channel.queueDeclare(queueName, false, false, false, null);
      }
      if (source.spec.exchange() != null) {
        if (source.spec.exchangeDeclare()) {
          channel.exchangeDeclare(source.spec.exchange(), source.spec.exchangeType());
        }
        if (queueName == null) {
          queueName = channel.queueDeclare().getQueue();
        }
        channel.queueBind(queueName, source.spec.exchange(), source.spec.routingKey());
      }
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
      Channel channel = checkpointMark.getChannel();
      // we consume message without autoAck (we want to do the ack ourselves)
      GetResponse delivery = channel.basicGet(queueName, false);
      if (delivery == null) {
        currentRecord = null;
        // queue is empty, so there is no backlog
        context = new TimestampPolicyContext(false, Instant.now());
        return false;
      }

      currentRecord = new RabbitMqMessage(delivery);
      context = new TimestampPolicyContext(true, Instant.now());
      long deliveryTag = delivery.getEnvelope().getDeliveryTag();
      checkpointMark.appendSessionId(deliveryTag);
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
    private final boolean hasBacklog;
    private final Instant lastCheckedAt;

    public TimestampPolicyContext(boolean hasBacklog, Instant lastCheckedAt) {
      this.hasBacklog = hasBacklog;
      this.lastCheckedAt = lastCheckedAt;
    }

    @Override
    public boolean hasBacklog() {
      return hasBacklog;
    }

    @Override
    public Instant lastCheckedAt() {
      return lastCheckedAt;
    }
  }
}
