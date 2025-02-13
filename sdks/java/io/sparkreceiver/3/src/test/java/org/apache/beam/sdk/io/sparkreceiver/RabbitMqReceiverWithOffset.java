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
package org.apache.beam.sdk.io.sparkreceiver;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imitation of Spark {@link Receiver} for RabbitMQ that implements {@link HasOffset} interface.
 * Used to test {@link SparkReceiverIO#read()}.
 */
class RabbitMqReceiverWithOffset extends Receiver<String> implements HasOffset {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMqReceiverWithOffset.class);
  private static final int MAX_PREFETCH_COUNT = 65535;

  private final String rabbitmqUrl;
  private final String streamName;
  private final long totalMessagesNumber;
  private long startOffset;
  private long recordsProcessed = 0L;

  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private transient Connection connection;
  private transient Channel channel;

  RabbitMqReceiverWithOffset(
      final String uri, final String streamName, final long totalMessagesNumber) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    rabbitmqUrl = uri;
    this.streamName = streamName;
    this.totalMessagesNumber = totalMessagesNumber;
  }

  @Override
  public void setStartOffset(Long startOffset) {
    this.startOffset = startOffset != null ? startOffset : 0;
  }

  @Override
  public Long getEndOffset() {
    return Long.MAX_VALUE;
  }

  @Override
  public void setCheckpoint(Long recordsProcessed) {
    this.recordsProcessed = recordsProcessed;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void onStart() {
    Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().build()).submit(this::receive);
  }

  @Override
  public void onStop() {}

  private void receive() {
    try {
      LOG.info("Starting receiver with offset {}", startOffset);
      final ConnectionFactory connectionFactory = new ConnectionFactory();
      connectionFactory.setUri(rabbitmqUrl);
      connectionFactory.setAutomaticRecoveryEnabled(true);
      connectionFactory.setConnectionTimeout(600000);
      connectionFactory.setNetworkRecoveryInterval(5000);
      connectionFactory.setRequestedHeartbeat(60);
      connectionFactory.setTopologyRecoveryEnabled(true);
      connectionFactory.setRequestedChannelMax(0);
      connectionFactory.setRequestedFrameMax(0);
      connection = connectionFactory.newConnection();

      channel = connection.createChannel();
      channel.queueDeclare(
          streamName, true, false, false, Collections.singletonMap("x-queue-type", "stream"));
      channel.basicQos(Math.min(MAX_PREFETCH_COUNT, (int) totalMessagesNumber));
      final TestConsumer testConsumer = new TestConsumer(channel, this::store, isStopped);

      channel.basicConsume(
          streamName,
          false,
          Collections.singletonMap("x-stream-offset", startOffset),
          testConsumer);
    } catch (Exception e) {
      LOG.error("Can not basic consume", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop(String message) {
    LOG.info(message);
    isStopped.set(true);
    super.stop(message);
    try {
      if (recordsProcessed != 0) {
        LOG.info("Try to multiple ack on {}", recordsProcessed);
        channel.basicAck(recordsProcessed, true);
      }
      channel.abort();
      connection.close();
      LOG.info("RabbitMQ channel and connection were closed");
    } catch (Exception e) {
      LOG.error("Exception during stopping of the RabbitMQ receiver", e);
    }
  }

  @Override
  public void stop(String message, Throwable error) {
    LOG.error(message, error);
    isStopped.set(true);
    super.stop(message, error);
    try {
      if (recordsProcessed != 0) {
        LOG.info("Try to multiple ack on {}", recordsProcessed);
        channel.basicAck(recordsProcessed, true);
      }
      channel.abort();
      connection.close();
      LOG.info("Closed RabbitMQ channel and connection");
    } catch (Exception e) {
      LOG.error("Can't close RabbitMQ channel and connection", e);
    }
  }

  /** A simple RabbitMQ {@code Consumer}. */
  static class TestConsumer extends DefaultConsumer {

    private final java.util.function.Consumer<String> messageConsumer;
    private final AtomicBoolean isReceiverStopped;
    private final Channel channel;

    public TestConsumer(
        Channel channel,
        java.util.function.Consumer<String> messageConsumer,
        AtomicBoolean isStopped) {
      super(channel);
      this.channel = channel;
      this.isReceiverStopped = isStopped;
      this.messageConsumer = messageConsumer;
    }

    @Override
    public void handleDelivery(
        String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
      try {
        final String sMessage = new String(body, StandardCharsets.UTF_8);
        if (channel.isOpen() && !isReceiverStopped.get()) {
          messageConsumer.accept(sMessage);
        }
      } catch (Exception e) {
        LOG.error("Can't read from RabbitMQ: {}", e.getMessage());
      }
    }
  }
}
