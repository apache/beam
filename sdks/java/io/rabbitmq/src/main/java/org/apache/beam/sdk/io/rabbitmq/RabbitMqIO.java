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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A IO to publish or consume messages with a RabbitMQ broker.
 *
 * <p>Documentation in this module tends to reference interacting with a "queue" vs interacting with
 * an "exchange". AMQP doesn't technically work this way. For readers, notes on reading from/writing
 * to a "queue" implies the "default exchange" is in use, operating as a "direct exchange". Notes on
 * interacting with an "exchange" are more flexible and are generally more applicable.
 *
 * <h3>Consuming messages from RabbitMQ server</h3>
 *
 * <p>{@link RabbitMqIO} {@link Read} returns an unbounded {@link PCollection} containing RabbitMQ
 * messages body (as {@code byte[]}) wrapped as {@link RabbitMqMessage}.
 *
 * <p>To configure a RabbitMQ source, you have to provide a RabbitMQ {@code URI} to connect to a
 * RabbitMQ broker. The following example illustrates various options for configuring the source,
 * reading from a named queue on the default exchange:
 *
 * <pre>{@code
 * PCollection<RabbitMqMessage> messages = pipeline.apply(
 *   RabbitMqIO.read().withUri("amqp://user:password@localhost:5672").withQueue("QUEUE"))
 *
 * }</pre>
 *
 * <p>Often one will want to read from an exchange. The exchange can be declared by Beam or can be
 * pre-existing. The supplied {@code routingKey} has variable functionality depending on the
 * exchange type. As examples:
 *
 * <pre>{@code
 *  // reading from an fanout (pubsub) exchange, declared (non-durable) by RabbitMqIO.
 *  // Note the routingKey is 'null' as a fanout exchange publishes all messages to
 *  // all queues, and the specified binding will be ignored
 * 	PCollection<RabbitMqMessage> messages = pipeline.apply(RabbitMqIO.read()
 * 			.withUri("amqp://user:password@localhost:5672").withExchange("EXCHANGE", "fanout", null));
 *
 * 	// reading from an existing topic exchange named 'EVENTS'
 * 	// this will use a dynamically-created, non-durable queue subscribing to all
 * 	// messages with a routing key beginning with 'users.'
 * 	PCollection<RabbitMqMessage> messages = pipeline.apply(RabbitMqIO.read()
 * 			.withUri("amqp://user:password@localhost:5672").withExchange("EVENTS", "users.#"));
 * }</pre>
 *
 * <h3>Publishing messages to RabbitMQ server</h3>
 *
 * <p>{@link RabbitMqIO} {@link Write} can send {@link RabbitMqMessage} to a RabbitMQ server queue
 * or exchange.
 *
 * <p>As for the {@link Read}, the {@link Write} is configured with a RabbitMQ URI.
 *
 * <p>Examples
 *
 * <pre>{@code
 * // Publishing to a named, non-durable exchange, declared by Beam:
 * pipeline
 *   .apply(...) // provide PCollection<RabbitMqMessage>
 *   .apply(RabbitMqIO.write().withUri("amqp://user:password@localhost:5672").withExchange("EXCHANGE", "fanout"));
 *
 * // Publishing to an existing exchange
 * pipeline
 *   .apply(...) // provide PCollection<RabbitMqMessage>
 *   .apply(RabbitMqIO.write().withUri("amqp://user:password@localhost:5672").withExchange("EXCHANGE"));
 *
 * // Publishing to a named queue in the default exchange:
 * pipeline
 *   .apply(...) // provide PCollection<RabbitMqMessage>
 *   .apply(RabbitMqIO.write().withUri("amqp://user:password@localhost:5672").withQueue("QUEUE"));
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RabbitMqIO {
  public static Read read() {
    return new AutoValue_RabbitMqIO_Read.Builder()
        .setQueueDeclare(false)
        .setExchangeDeclare(false)
        .setMaxReadTime(null)
        .setMaxNumRecords(Long.MAX_VALUE)
        .setUseCorrelationId(false)
        .build();
  }

  public static Write write() {
    return new AutoValue_RabbitMqIO_Write.Builder()
        .setExchangeDeclare(false)
        .setQueueDeclare(false)
        .build();
  }

  private RabbitMqIO() {}

  private static final class ConnectionHandler {

    private final ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;

    public ConnectionHandler(String uri)
        throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
      connectionFactory = new ConnectionFactory();
      connectionFactory.setUri(uri);
      connectionFactory.setAutomaticRecoveryEnabled(true);
      connectionFactory.setConnectionTimeout(60000);
      connectionFactory.setNetworkRecoveryInterval(5000);
      connectionFactory.setRequestedHeartbeat(60);
      connectionFactory.setTopologyRecoveryEnabled(true);
      connectionFactory.setRequestedChannelMax(0);
      connectionFactory.setRequestedFrameMax(0);
    }

    public void start() throws TimeoutException, IOException {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
      if (channel == null) {
        throw new IOException("No RabbitMQ channel available");
      }
    }

    public Channel getChannel() {
      return this.channel;
    }

    public void stop() throws IOException {
      if (channel != null) {
        try {
          channel.close();
        } catch (Exception e) {
          // ignore
        }
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  /** A {@link PTransform} to consume messages from RabbitMQ server. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<RabbitMqMessage>> {

    abstract @Nullable String uri();

    abstract @Nullable String queue();

    abstract boolean queueDeclare();

    abstract @Nullable String exchange();

    abstract @Nullable String exchangeType();

    abstract boolean exchangeDeclare();

    abstract @Nullable String routingKey();

    abstract boolean useCorrelationId();

    abstract long maxNumRecords();

    abstract @Nullable Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUri(String uri);

      abstract Builder setQueue(String queue);

      abstract Builder setQueueDeclare(boolean queueDeclare);

      abstract Builder setExchange(String exchange);

      abstract Builder setExchangeType(String exchangeType);

      abstract Builder setExchangeDeclare(boolean exchangeDeclare);

      abstract Builder setRoutingKey(String routingKey);

      abstract Builder setUseCorrelationId(boolean useCorrelationId);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
    }

    public Read withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return builder().setUri(uri).build();
    }

    /**
     * If you want to directly consume messages from a specific queue, you just have to specify the
     * queue name. Optionally, you can declare the queue using {@link
     * RabbitMqIO.Read#withQueueDeclare(boolean)}.
     */
    public Read withQueue(String queue) {
      checkArgument(queue != null, "queue can not be null");
      return builder().setQueue(queue).build();
    }

    /**
     * You can "force" the declaration of a queue on the RabbitMQ broker. Exchanges and queues are
     * the high-level building blocks of AMQP. These must be "declared" (created) before they can be
     * used. Declaring either type of object ensures that one of that name and of the specified
     * properties exists, creating it if necessary.
     *
     * <p>NOTE: When declaring a queue or exchange that already exists, the properties specified in
     * the declaration must match those of the existing queue or exchange. That is, if you declare a
     * queue to be non-durable but a durable queue already exists with the same name, the
     * declaration will fail. When declaring a queue, RabbitMqIO will declare it to be non-durable.
     *
     * @param queueDeclare If {@code true}, {@link RabbitMqIO} will declare a non-durable queue. If
     *     another application created the queue, this is not required and should be set to {@code
     *     false}
     */
    public Read withQueueDeclare(boolean queueDeclare) {
      return builder().setQueueDeclare(queueDeclare).build();
    }

    /**
     * In AMQP, messages are published to an exchange and routed to queues based on the exchange
     * type and a queue binding. Most exchange types utilize the routingKey to determine which
     * queues to deliver messages to. It is incumbent upon the developer to understand the paradigm
     * in place to determine whether to declare a queue, what the appropriate binding should be, and
     * what routingKey will be in use.
     *
     * <p>This function should be used if the Beam pipeline will be responsible for declaring the
     * exchange. As a result of calling this function, {@code exchangeDeclare} will be set to {@code
     * true} and the resulting exchange will be non-durable and of the supplied type. If an exchange
     * with the given name already exists but is durable or is of another type, exchange declaration
     * will fail.
     *
     * <p>To use an exchange without declaring it, especially for cases when the exchange is shared
     * with other applications or already exists, use {@link #withExchange(String, String)} instead.
     *
     * @see
     *     "https://www.cloudamqp.com/blog/2015-09-03-part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html"
     *     for a write-up on exchange types and routing semantics
     */
    public Read withExchange(String name, String type, @Nullable String routingKey) {
      checkArgument(name != null, "exchange name can not be null");
      checkArgument(type != null, "exchange type can not be null");
      return builder()
          .setExchange(name)
          .setExchangeType(type)
          .setRoutingKey(routingKey)
          .setExchangeDeclare(true)
          .build();
    }

    /**
     * In AMQP, messages are published to an exchange and routed to queues based on the exchange
     * type and a queue binding. Most exchange types utilize the routingKey to determine which
     * queues to deliver messages to. It is incumbent upon the developer to understand the paradigm
     * in place to determine whether to declare a queue, with the appropriate binding should be, and
     * what routingKey will be in use.
     *
     * <p>This function should be used if the Beam pipeline will be using an exchange that has
     * already been declared or when using an exchange shared by other applications, such as an
     * events bus or pubsub. As a result of calling this function, {@code exchangeDeclare} will be
     * set to {@code false}.
     */
    public Read withExchange(String name, @Nullable String routingKey) {
      checkArgument(name != null, "exchange name can not be null");
      return builder()
          .setExchange(name)
          .setExchangeDeclare(false)
          .setRoutingKey(routingKey)
          .build();
    }

    /**
     * Define the max number of records received by the {@link Read}. When this max number of
     * records is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
     * PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxReadTime() == null, "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages. When this
     * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(
          maxNumRecords() == Long.MAX_VALUE, "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    /**
     * Toggles deduplication of messages based on the amqp correlation-id property on incoming
     * messages.
     *
     * <p>When set to {@code true} all read messages will require the amqp correlation-id property
     * to be set.
     *
     * <p>When set to {@code false} the correlation-id property will not be used by the Reader and
     * no automatic deduplication will occur.
     */
    public Read withUseCorrelationId(boolean useCorrelationId) {
      return builder().setUseCorrelationId(useCorrelationId).build();
    }

    @Override
    public PCollection<RabbitMqMessage> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<RabbitMqMessage> unbounded =
          org.apache.beam.sdk.io.Read.from(new RabbitMQSource(this));

      PTransform<PBegin, PCollection<RabbitMqMessage>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }
  }

  @VisibleForTesting
  static class RabbitMQSource extends UnboundedSource<RabbitMqMessage, RabbitMQCheckpointMark> {
    final Read spec;

    RabbitMQSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public Coder<RabbitMqMessage> getOutputCoder() {
      return SerializableCoder.of(RabbitMqMessage.class);
    }

    @Override
    public List<RabbitMQSource> split(int desiredNumSplits, PipelineOptions options) {
      // RabbitMQ uses queue, so, we can have several concurrent consumers as source
      List<RabbitMQSource> sources = new ArrayList<>();
      for (int i = 0; i < desiredNumSplits; i++) {
        sources.add(this);
      }
      return sources;
    }

    @Override
    public UnboundedReader<RabbitMqMessage> createReader(
        PipelineOptions options, @Nullable RabbitMQCheckpointMark checkpointMark)
        throws IOException {
      if (checkpointMark != null) {
        // A runner resumes from a checkpoint only once it is committed, so its messages can be
        // acknowledged if the reader that took it lives in this JVM.
        checkpointMark.finalizeCheckpoint();
      }
      return new UnboundedRabbitMqReader(this, options);
    }

    @Override
    public Coder<RabbitMQCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(RabbitMQCheckpointMark.class);
    }

    @Override
    public boolean requiresDeduping() {
      return spec.useCorrelationId();
    }
  }

  @VisibleForTesting
  static class RabbitMQCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

    // Pinned to the value computed before readerId and sequence were added, so checkpoints written
    // by older versions can still be decoded.
    private static final long serialVersionUID = -961062677995876719L;

    private final @Nullable String readerId;
    private final long sequence;
    transient @Nullable Channel channel;
    Instant latestTimestamp = Instant.now();
    transient ConcurrentLinkedQueue<Long> sessionIds = new ConcurrentLinkedQueue<>();
    transient @Nullable PendingCheckpoints pendingCheckpoints;
    transient @Nullable AtomicInteger activeCheckpoints;
    transient @Nullable Runnable onCheckpointFinalized;

    RabbitMQCheckpointMark() {
      this(null, 0, null, Instant.now(), new ConcurrentLinkedQueue<>(), null, null);
    }

    private RabbitMQCheckpointMark(
        @Nullable PendingCheckpoints pendingCheckpoints,
        long sequence,
        @Nullable Channel channel,
        Instant latestTimestamp,
        ConcurrentLinkedQueue<Long> sessionIds,
        @Nullable AtomicInteger activeCheckpoints,
        @Nullable Runnable onCheckpointFinalized) {
      this.pendingCheckpoints = pendingCheckpoints;
      this.readerId = pendingCheckpoints == null ? null : pendingCheckpoints.readerId;
      this.sequence = sequence;
      this.channel = channel;
      this.latestTimestamp = latestTimestamp;
      this.sessionIds = sessionIds;
      this.activeCheckpoints = activeCheckpoints;
      this.onCheckpointFinalized = onCheckpointFinalized;
    }

    // this method is called after deserialization on the deserialized object
    private synchronized Object readResolve() {
      // (re-)initialize transient fields as required
      this.sessionIds = new ConcurrentLinkedQueue<>();
      return this;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      PendingCheckpoints pending = pendingCheckpoints;
      if (pending == null && readerId != null) {
        pending = PendingCheckpoints.LIVE.getIfPresent(readerId);
      }
      if (pending != null) {
        for (RabbitMQCheckpointMark checkpoint : pending.removeUpTo(sequence)) {
          checkpoint.acknowledge();
        }
        return;
      }
      if (readerId != null) {
        return;
      }
      acknowledge();
    }

    private synchronized void acknowledge() throws IOException {
      Channel ch = channel;
      if (ch != null) {
        Long sessionId = sessionIds.poll();
        while (sessionId != null) {
          ch.basicAck(sessionId, false);
          sessionId = sessionIds.poll();
        }
        if (ch.isOpen()) {
          ch.txCommit();
        }
      } else {
        sessionIds.clear();
      }
      channel = null;
      latestTimestamp = Instant.now();
      release(true);
    }

    private synchronized void abandon() {
      sessionIds.clear();
      channel = null;
      release(false);
    }

    private void release(boolean notifyIfDrained) {
      if (activeCheckpoints != null) {
        int remaining = activeCheckpoints.decrementAndGet();
        activeCheckpoints = null;
        if (notifyIfDrained && remaining == 0 && onCheckpointFinalized != null) {
          onCheckpointFinalized.run();
        }
      }
    }

    private static final class PendingCheckpoints {
      private static final Cache<String, PendingCheckpoints> LIVE =
          CacheBuilder.newBuilder().weakValues().build();

      private final String readerId = UUID.randomUUID().toString();
      private final List<RabbitMQCheckpointMark> checkpoints = new ArrayList<>();
      private long nextSequence;

      static PendingCheckpoints create() {
        PendingCheckpoints pending = new PendingCheckpoints();
        LIVE.put(pending.readerId, pending);
        return pending;
      }

      synchronized long nextSequence() {
        return nextSequence++;
      }

      synchronized void add(RabbitMQCheckpointMark checkpoint) {
        checkpoints.add(checkpoint);
      }

      synchronized List<RabbitMQCheckpointMark> removeUpTo(long sequence) {
        List<RabbitMQCheckpointMark> removed = new ArrayList<>();
        checkpoints.removeIf(
            checkpoint -> {
              if (checkpoint.sequence <= sequence) {
                removed.add(checkpoint);
                return true;
              }
              return false;
            });
        return removed;
      }

      synchronized List<RabbitMQCheckpointMark> removeAll() {
        LIVE.invalidate(readerId);
        List<RabbitMQCheckpointMark> removed = new ArrayList<>(checkpoints);
        checkpoints.clear();
        return removed;
      }
    }

    static class Preparer {
      private final PendingCheckpoints pendingCheckpoints = PendingCheckpoints.create();
      private final List<Long> sessionIds = new ArrayList<>();
      private volatile Instant latestTimestamp = Instant.now();
      private boolean discarded = false;

      synchronized void add(long deliveryTag) {
        if (discarded) {
          throw new IllegalStateException("Attempting to add delivery tag to discarded checkpoint");
        }
        sessionIds.add(deliveryTag);
      }

      /**
       * Advances the watermark to the provided time, provided said time is after the current
       * watermark. If the provided time is before the latest, this function no-ops.
       */
      synchronized void advanceWatermark(Instant time) {
        if (time.isAfter(latestTimestamp)) {
          latestTimestamp = time;
        }
      }

      Instant getLatestTimestamp() {
        return latestTimestamp;
      }

      synchronized void discard() {
        sessionIds.clear();
        discarded = true;
      }

      synchronized RabbitMQCheckpointMark newCheckpoint(
          @Nullable Channel channel,
          @Nullable AtomicInteger activeCheckpoints,
          @Nullable Runnable onCheckpointFinalized) {
        if (discarded || sessionIds.isEmpty()) {
          sessionIds.clear();
          return new RabbitMQCheckpointMark(
              pendingCheckpoints,
              pendingCheckpoints.nextSequence(),
              null,
              latestTimestamp,
              new ConcurrentLinkedQueue<>(),
              null,
              null);
        }
        ConcurrentLinkedQueue<Long> snapshot = new ConcurrentLinkedQueue<>(sessionIds);
        sessionIds.clear();
        Instant checkpointTimestamp = latestTimestamp;
        latestTimestamp = Instant.now();
        RabbitMQCheckpointMark mark =
            new RabbitMQCheckpointMark(
                pendingCheckpoints,
                pendingCheckpoints.nextSequence(),
                channel,
                checkpointTimestamp,
                snapshot,
                activeCheckpoints,
                onCheckpointFinalized);
        pendingCheckpoints.add(mark);
        if (activeCheckpoints != null) {
          activeCheckpoints.incrementAndGet();
        }
        return mark;
      }

      void abandonPendingCheckpoints() {
        for (RabbitMQCheckpointMark checkpoint : pendingCheckpoints.removeAll()) {
          checkpoint.abandon();
        }
      }
    }
  }

  private static class UnboundedRabbitMqReader
      extends UnboundedSource.UnboundedReader<RabbitMqMessage> {
    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.standardSeconds(60);

    private final RabbitMQSource source;
    private final PipelineOptions options;

    private @Nullable RabbitMqMessage current;
    private byte @Nullable [] currentRecordId;
    private @Nullable ConnectionHandler connectionHandler;
    private @Nullable Channel channel;
    private @Nullable String queueName;
    private @Nullable Instant currentTimestamp;
    private final RabbitMQCheckpointMark.Preparer checkpointPreparer =
        new RabbitMQCheckpointMark.Preparer();
    private final AtomicInteger activeCheckpoints = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object closeLock = new Object();

    UnboundedRabbitMqReader(RabbitMQSource source, @Nullable PipelineOptions options) {
      this.source = source;
      this.options = options == null ? PipelineOptionsFactory.create() : options;
      this.current = null;
    }

    @Override
    public Instant getWatermark() {
      return checkpointPreparer.getLatestTimestamp();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return checkpointPreparer.newCheckpoint(
          channel, activeCheckpoints, this::maybeStopConnection);
    }

    @Override
    public RabbitMQSource getCurrentSource() {
      return source;
    }

    @Override
    public byte[] getCurrentRecordId() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      if (currentRecordId != null) {
        return currentRecordId;
      } else {
        return "".getBytes(StandardCharsets.UTF_8);
      }
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (currentTimestamp == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public RabbitMqMessage getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public boolean start() throws IOException {
      try {
        connectionHandler = new ConnectionHandler(source.spec.uri());
        connectionHandler.start();

        Channel createdChannel = connectionHandler.getChannel();

        queueName = source.spec.queue();
        if (source.spec.queueDeclare()) {
          // declare the queue (if not done by another application)
          // channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
          createdChannel.queueDeclare(queueName, false, false, false, null);
        }
        if (source.spec.exchange() != null) {
          if (source.spec.exchangeDeclare()) {
            createdChannel.exchangeDeclare(source.spec.exchange(), source.spec.exchangeType());
          }
          if (queueName == null) {
            queueName = createdChannel.queueDeclare().getQueue();
          }
          createdChannel.queueBind(queueName, source.spec.exchange(), source.spec.routingKey());
        }
        channel = createdChannel;
        createdChannel.txSelect();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      try {
        Channel ch = channel;
        if (ch == null) {
          current = null;
          currentRecordId = null;
          currentTimestamp = null;
          return false;
        }
        // we consume message without autoAck (we want to do the ack ourselves)
        GetResponse delivery = ch.basicGet(queueName, false);
        if (delivery == null) {
          current = null;
          currentRecordId = null;
          currentTimestamp = null;
          checkpointPreparer.advanceWatermark(Instant.now());
          return false;
        }
        if (source.spec.useCorrelationId()) {
          String correlationId = delivery.getProps().getCorrelationId();
          if (correlationId == null) {
            throw new IOException(
                "RabbitMqIO.Read uses message correlation ID, but received "
                    + "message has a null correlation ID");
          }
          currentRecordId = correlationId.getBytes(StandardCharsets.UTF_8);
        }
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        checkpointPreparer.add(deliveryTag);

        current = new RabbitMqMessage(source.spec.routingKey(), delivery);
        Date deliveryTimestamp = delivery.getProps().getTimestamp();
        currentTimestamp =
            (deliveryTimestamp != null) ? new Instant(deliveryTimestamp) : Instant.now();
        checkpointPreparer.advanceWatermark(currentTimestamp);
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
      return true;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void close() throws IOException {
      closed.set(true);
      checkpointPreparer.discard();
      if (activeCheckpoints.get() == 0) {
        abandonPendingCheckpointsAndStopConnection();
      } else {
        ScheduledExecutorService executorService =
            options.as(ExecutorOptions.class).getScheduledExecutorService();
        executorService.schedule(
            this::abandonPendingCheckpointsAndStopConnection,
            DEFAULT_CLOSE_TIMEOUT.getMillis(),
            TimeUnit.MILLISECONDS);
      }
    }

    private void maybeStopConnection() {
      if (closed.get() && activeCheckpoints.get() == 0) {
        abandonPendingCheckpointsAndStopConnection();
      }
    }

    private void abandonPendingCheckpointsAndStopConnection() {
      synchronized (closeLock) {
        checkpointPreparer.abandonPendingCheckpoints();
        channel = null;
        if (connectionHandler != null) {
          try {
            connectionHandler.stop();
          } catch (IOException ignored) {
            // Best-effort cleanup on close.
          } finally {
            connectionHandler = null;
          }
        }
      }
    }
  }

  /** A {@link PTransform} to publish messages to a RabbitMQ server. */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<RabbitMqMessage>, PCollection<?>> {

    abstract @Nullable String uri();

    abstract @Nullable String exchange();

    abstract @Nullable String exchangeType();

    abstract boolean exchangeDeclare();

    abstract @Nullable String queue();

    abstract boolean queueDeclare();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUri(String uri);

      abstract Builder setExchange(String exchange);

      abstract Builder setExchangeType(String exchangeType);

      abstract Builder setExchangeDeclare(boolean exchangeDeclare);

      abstract Builder setQueue(String queue);

      abstract Builder setQueueDeclare(boolean queueDeclare);

      abstract Write build();
    }

    public Write withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return builder().setUri(uri).build();
    }

    /**
     * Defines the to-be-declared exchange where the messages will be sent. By defining the exchange
     * via this function, RabbitMqIO will be responsible for declaring this exchange, and will
     * declare it as non-durable. If an exchange with this name already exists but is non-durable or
     * of a different type, the declaration will fail.
     *
     * <p>By calling this function {@code exchangeDeclare} will be set to {@code true}.
     *
     * <p>To publish to an existing exchange, use {@link #withExchange(String)}
     */
    public Write withExchange(String exchange, String exchangeType) {
      checkArgument(exchange != null, "exchange can not be null");
      checkArgument(exchangeType != null, "exchangeType can not be null");
      return builder()
          .setExchange(exchange)
          .setExchangeType(exchangeType)
          .setExchangeDeclare(true)
          .build();
    }

    /**
     * Defines the existing exchange where the messages will be sent.
     *
     * <p>By calling this function {@code exchangeDeclare} will be set to {@code false}
     */
    public Write withExchange(String exchange) {
      checkArgument(exchange != null, "exchange can not be null");
      return builder().setExchange(exchange).setExchangeDeclare(false).build();
    }

    /**
     * Defines the queue where the messages will be sent. The queue has to be declared. It can be
     * done by another application or by {@link RabbitMqIO} if you set {@link
     * Write#withQueueDeclare} to {@code true}.
     */
    public Write withQueue(String queue) {
      checkArgument(queue != null, "queue can not be null");
      return builder().setQueue(queue).build();
    }

    /**
     * If the queue is not declared by another application, {@link RabbitMqIO} can declare the queue
     * itself.
     *
     * @param queueDeclare {@code true} to declare the queue, {@code false} else.
     */
    public Write withQueueDeclare(boolean queueDeclare) {
      return builder().setQueueDeclare(queueDeclare).build();
    }

    @Override
    public PCollection<?> expand(PCollection<RabbitMqMessage> input) {
      checkArgument(
          exchange() != null || queue() != null, "Either exchange or queue has to be specified");
      if (exchange() != null) {
        checkArgument(queue() == null, "Queue can't be set in the same time as exchange");
      }
      if (queue() != null) {
        checkArgument(exchange() == null, "Exchange can't be set in the same time as queue");
      }
      if (queueDeclare()) {
        checkArgument(queue() != null, "Queue is required for the queue declare");
      }
      if (exchangeDeclare()) {
        checkArgument(exchange() != null, "Exchange is required for the exchange declare");
      }
      return input.apply(ParDo.of(new WriteFn(this)));
    }

    private static class WriteFn extends DoFn<RabbitMqMessage, Void> {
      private final Write spec;

      private transient ConnectionHandler connectionHandler;

      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        connectionHandler = new ConnectionHandler(spec.uri());
        connectionHandler.start();

        Channel channel = connectionHandler.getChannel();

        if (spec.exchange() != null && spec.exchangeDeclare()) {
          channel.exchangeDeclare(spec.exchange(), spec.exchangeType());
        }
        if (spec.queue() != null && spec.queueDeclare()) {
          channel.queueDeclare(spec.queue(), true, false, false, null);
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        RabbitMqMessage message = c.element();
        Channel channel = connectionHandler.getChannel();

        if (spec.exchange() != null) {
          channel.basicPublish(
              spec.exchange(),
              message.getRoutingKey(),
              message.createProperties(),
              message.getBody());
        }
        if (spec.queue() != null) {
          channel.basicPublish(
              "", spec.queue(), MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBody());
        }
      }

      @Teardown
      public void teardown() throws Exception {
        if (connectionHandler != null) {
          connectionHandler.stop();
        }
      }
    }
  }
}
