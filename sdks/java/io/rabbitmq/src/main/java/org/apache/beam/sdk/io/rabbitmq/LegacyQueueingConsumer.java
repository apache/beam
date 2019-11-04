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
import com.rabbitmq.utility.Utility;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Between amqp clients 4.x to 5.x the deprecated {@code QueueingConsumer} was removed from the code
 * base (technically it was moved to a test package).
 *
 * <p>The original upgrade approach was to switch to the pull-based {@code channel.basicGet} API,
 * however this by default blocks indefinitely (the value of {@code channel rpc timeout}). If left
 * blocking, a Beam Runner may consider the source functionality to be 'stuck' and take some
 * destructive action. If the rpc timeout is lowered, the default behavior is to terminate the
 * channel, which similarly breaks the reader shard. There is likely a path forward with the
 * pull-based api, rpc timeouts, and a more forgiving default {@code ExceptionHandler}, but the most
 * expedient option is to simply restore the 4.x functionality directly and make improvements later.
 *
 * <p>This is primarily a direct copy-paste of the 4.x QueueingConsumer for use in 5.x, with the
 * following differences:
 *
 * <ul>
 *   <li>{@link com.rabbitmq.client.Delivery} is now a top-level class rather than nested inside
 *       this class
 *   <li>It has been renamed to LegacyQueueingConsumer for disambiguation
 *   <li>{@code nextDelivery} without a timeout has been removed from the API
 *   <li>{@link #nextDelivery(long, TimeUnit)} now accepts a time unit to match {@code poll} for
 *       better clarity
 *   <li>{@link #nextDelivery(long, TimeUnit)} now only throws IOException, for simplicity of
 *       interacting with Beam
 *   <li>Names/formatting have been updated to comply with Beam coding practices
 * </ul>
 *
 * @see <a
 *     href="https://github.com/rabbitmq/rabbitmq-java-client/blob/4.x.x-stable/src/main/java/com/rabbitmq/client/QueueingConsumer.java">the
 *     original QueuingConsumer</a>
 */
public class LegacyQueueingConsumer extends DefaultConsumer {
  private final BlockingQueue<Delivery> queue;

  // When this is non-null the queue is in shutdown mode and nextDelivery should
  // throw a shutdown signal exception.
  private volatile ShutdownSignalException shutdown;
  private volatile ConsumerCancelledException cancelled;

  // Marker object used to signal the queue is in shutdown mode.
  // It is only there to wake up consumers. The canonical representation
  // of shutting down is the presence of _shutdown.
  // Invariant: This is never on _queue unless _shutdown != null.
  private static final Delivery POISON = new Delivery(null, null, null);

  public LegacyQueueingConsumer(Channel ch) {
    this(ch, new LinkedBlockingQueue<Delivery>());
  }

  public LegacyQueueingConsumer(Channel ch, BlockingQueue<Delivery> q) {
    super(ch);
    this.queue = q;
  }

  @Override
  public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
    shutdown = sig;
    queue.add(POISON);
  }

  @Override
  public void handleCancel(String consumerTag) throws IOException {
    cancelled = new ConsumerCancelledException();
    queue.add(POISON);
  }

  @Override
  public void handleDelivery(
      String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
      throws IOException {
    checkShutdown();
    this.queue.add(new Delivery(envelope, properties, body));
  }

  /** Check if we are in shutdown mode and if so throw an exception. */
  private void checkShutdown() {
    if (shutdown != null) {
      throw Utility.fixStackTrace(shutdown);
    }
  }

  /**
   * If delivery is not POISON nor null, return it.
   *
   * <p>If delivery, _shutdown and _cancelled are all null, return null.
   *
   * <p>If delivery is POISON re-insert POISON into the queue and throw an exception if POISONed for
   * no reason.
   *
   * <p>Otherwise, if we are in shutdown mode or cancelled, throw a corresponding exception.
   */
  private Delivery handle(Delivery delivery) {
    if (delivery == POISON || (delivery == null && (shutdown != null || cancelled != null))) {
      if (delivery == POISON) {
        queue.add(POISON);
        if (shutdown == null && cancelled == null) {
          throw new IllegalStateException(
              "POISON in queue, but null _shutdown and null _cancelled. "
                  + "This should never happen, please report as a BUG");
        }
      }
      if (null != shutdown) {
        throw Utility.fixStackTrace(shutdown);
      }
      if (null != cancelled) {
        throw Utility.fixStackTrace(cancelled);
      }
    }
    return delivery;
  }

  /**
   * Main application-side API: wait for the next message delivery and return it.
   *
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
   * @return the next message or {@code null} if timed out
   * @throws IOException if an interrupt occured while receive was waiting (will wrap an
   *     InterruptedException), or if the connection was shut down while waiting (will wrap a {@link
   *     ShutdownSignalException}), or if the consumer was cancelled while waiting (will wrap a
   *     {@link ConsumerCancelledException}.
   */
  public Delivery nextDelivery(long timeout, TimeUnit unit) throws IOException {
    try {
      return handle(queue.poll(timeout, unit));
    } catch (InterruptedException | ShutdownSignalException | ConsumerCancelledException e) {
      // Note: if InterruptedException, will clear current thread's
      // interrupt status. Should be fine for Beam's use case.
      throw new IOException(e);
    }
  }
}
