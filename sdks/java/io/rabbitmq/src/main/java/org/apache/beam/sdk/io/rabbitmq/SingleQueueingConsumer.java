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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An implementation of a Consumer (push-based api for rabbit) that accepts at most one message at a
 * time from the server and allows the delivered message to be polled by a caller with a timeout.
 *
 * <p>Because AMQP client 5.x removed the deprecated-in-4.x QueueingConsumer this is effectively a
 * re-implementation of the 4.x line's with a few notable changes:
 *
 * <ul>
 *   <li>The original was designed to solve a threading concern which no longer applies. The poison
 *       message and some other workarounds should not be relevant.
 *   <li>The original used an unbounded LinkedBlockingQueue. Delivery of messages among multiple
 *       Beam shards will be more 'fair' if each has only a single unprocessed message. This
 *       implementation limits enqueued messages to a single one.
 *   <li>Channel thread will block for up to a configurable length of time (default: 10 minutes) for
 *       the queue to have an open slot. Previous it would nominally fail immediately, which would
 *       never happen as the queue was unbounded.
 *   <li>Exceptions have been simplified to only expose IOException to callers.
 * </ul>
 *
 * <p>Note: setting amqp prefetch to 1 does not accomplish the same thing. Prefetch limits the
 * number of messages that will be sent to the consumer (good) *which have not been acknowledged
 * yet* (bad for Beam). Because acknowledgements happen during {@code finalizeCheckpoint}, prefetch
 * would have to be set to the maximum number of messages Beam *could* process before {@code
 * finalizeCheckpoint} is called, which is unknowable.
 *
 * <p>In effect, this version will use less memory than the original but result in lower throughput
 * among any single beam shard.
 *
 * @see
 *     "https://github.com/rabbitmq/rabbitmq-java-client/blob/4.x.x-stable/src/main/java/com/rabbitmq/client/QueueingConsumer.java"
 *     for the original QueuingConsumer
 */
public class SingleQueueingConsumer extends DefaultConsumer {
  private final BlockingDeque<Delivery> queue = new LinkedBlockingDeque<>(1);
  private final long offerTimeoutMillis;

  private static final int DEFAULT_DELIVER_TIMEOUT_MINUTES = 10;

  private volatile @Nullable ShutdownSignalException shutdown = null;
  private volatile boolean cancelled = false;

  /**
   * Default implementation which will block delivering messages for up to 10 minutes if the queue
   * is full.
   */
  public SingleQueueingConsumer(Channel channel) {
    this(channel, DEFAULT_DELIVER_TIMEOUT_MINUTES, TimeUnit.MINUTES);
  }

  /**
   * Blocks deliveries for the supplied length of time when the queue is full.
   *
   * @param channel
   * @param deliverTimeout
   * @param deliverTimeoutUnits
   */
  public SingleQueueingConsumer(
      Channel channel, long deliverTimeout, TimeUnit deliverTimeoutUnits) {
    super(channel);
    this.offerTimeoutMillis = deliverTimeoutUnits.toMillis(deliverTimeout);
  }

  @Override
  public void handleCancelOk(String consumerTag) {
    doCancel();
  }

  @Override
  public void handleCancel(String consumerTag) throws IOException {
    doCancel();
  }

  private void doCancel() {
    this.cancelled = true;
  }

  @Override
  public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
    this.shutdown = sig;
  }

  @Override
  public void handleDelivery(
      String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
      throws IOException {
    if (shutdown != null) {
      throw new IOException(shutdown);
    }

    Delivery delivery = new Delivery(envelope, properties, body);
    try {
      queue.offer(delivery, offerTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // clearing thread interrupt state should be acceptable here
      throw new IOException(e);
    }
  }

  /**
   * Retrieve the pending Delivery message, awaiting up to the specified timeout for it to become
   * available.
   *
   * @param timeout
   * @param unit
   * @return The enqueued Delivery if one is available or becomes available in the specified time,
   *     else {@code null}
   * @throws IOException If the Consumer has been shut down, the Channel cancelled, or an
   *     InterruptedException occurs attempting to poll the backing queue.
   */
  public Delivery poll(long timeout, TimeUnit unit) throws IOException {
    if (shutdown != null) {
      throw new IOException(shutdown);
    }
    if (cancelled) {
      throw new IOException(new ConsumerCancelledException());
    }

    try {
      return queue.poll(timeout, unit);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
