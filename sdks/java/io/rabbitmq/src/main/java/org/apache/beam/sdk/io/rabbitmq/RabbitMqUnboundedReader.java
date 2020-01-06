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
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
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
  private final MovingAvg averageRecordSize;

  RabbitMqUnboundedReader(RabbitMqSource source) {
    this.source = source;
    this.currentRecord = null;
    this.recordIdPolicy = source.spec.recordIdPolicy();
    this.context =
        new TimestampPolicyContext(
            Long.valueOf(UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN).intValue(),
            BoundedWindow.TIMESTAMP_MIN_VALUE);
    this.connectionHandler = this.source.spec.connectionHandlerProviderFn().apply(null);
    this.averageRecordSize = new MovingAvg();
    this.checkpointMark = new RabbitMqCheckpointMark(this.connectionHandler);
    ensureTimestampPolicySet(Optional.ofNullable(this.checkpointMark.getWatermark()));
  }

  private void ensureTimestampPolicySet(Optional<Instant> prevWatermark) {
    if (this.timestampPolicy == null) {
      this.timestampPolicy =
          source.spec.timestampPolicyFactory().createTimestampPolicy(prevWatermark);
    }
  }

  @Override
  public Instant getWatermark() {
    Instant watermark = timestampPolicy.getWatermark(context);
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
    return Optional.ofNullable(currentRecord).map(recordIdPolicy::apply).get();
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

      ChannelLeaser.UseChannelFunction<String> setupFn =
          channel -> {
            ReadParadigm paradigm = source.spec.readParadigm();
            String queueName = paradigm.queueName();
            if (paradigm instanceof ReadParadigm.NewQueue) {
              ReadParadigm.NewQueue newQueue = (ReadParadigm.NewQueue) paradigm;

              final boolean durable = true;
              final boolean autoDelete = false;
              final boolean exclusive = false;
              final Map<String, Object> arguments = Collections.emptyMap();

              // this may fail if the queue isn't actually new and has different properties
              channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);

              // the default exchange does not allow for explicitly binding a queue
              if (!newQueue.isDefaultExchange()) {
                channel.queueBind(queueName, newQueue.getExchange(), newQueue.getRoutingKey());
              }
            }

            return queueName;
          };

      queueName = connectionHandler.useChannel(checkpointMark.getCheckpointId(), setupFn);
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
      // counting 'size' in terms of raw bytes of the body only
      averageRecordSize.update(currentRecord.body().length);
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

  /**
   * {@inheritDoc}.
   *
   * <p>This implementation produces an estimate of the total backlog by taking the average size in
   * bytes of the past 1,000 messages seen by this reader and multiplying by the number of estimated
   * messages remaining in the queue as returned by {@link GetResponse#getMessageCount()}. If no
   * messages have been seen, {@link #BACKLOG_UNKNOWN} is returned. Note that amqp headers and
   * properties are not incorporated into the size estimate.
   */
  @Override
  public long getTotalBacklogBytes() {
    long backlogMessageCount = context.getMessageBacklog();
    if (backlogMessageCount == BACKLOG_UNKNOWN) {
      return BACKLOG_UNKNOWN;
    }

    return (long) (backlogMessageCount * averageRecordSize.get());
  }

  // Maintains approximate average over last 1000 elements
  // (Note: lifted directly from KafkaIO. Should consolidate.)
  private static class MovingAvg {
    private static final int MOVING_AVG_WINDOW = 1000;
    private double avg = 0;
    private long numUpdates = 0;

    void update(double quantity) {
      numUpdates++;
      avg += (quantity - avg) / Math.min(MOVING_AVG_WINDOW, numUpdates);
    }

    double get() {
      return avg;
    }
  }

  @VisibleForTesting
  static class TimestampPolicyContext extends TimestampPolicy.LastRead {
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
