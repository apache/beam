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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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
@Experimental(Experimental.Kind.SOURCE_SINK)
public class RabbitMqIO {
  public static Read read() {
    return new AutoValue_RabbitMqIO_Read.Builder()
        .setQueueDeclare(false)
        .setExchangeDeclare(false)
        // this policy is only appropriate for pipelines with at-least-once semantics
        // and capable of handling potentially a large number of repeatedly-delivered messages
        .setRecordIdPolicy(RecordIdPolicy.alwaysUnique())
        .setMaxReadTime(null)
        .setMaxNumRecords(Long.MAX_VALUE)
        .setTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime())
        .build();
  }

  public static Write write() {
    return new AutoValue_RabbitMqIO_Write.Builder()
        .setExchangeDeclare(false)
        .setQueueDeclare(false)
        .build();
  }

  private RabbitMqIO() {}

  /** A {@link PTransform} to consume messages from RabbitMQ server. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<RabbitMqMessage>> {

    @Nullable
    abstract String queue();

    abstract boolean queueDeclare();

    @Nullable
    abstract String exchange();

    @Nullable
    abstract String exchangeType();

    abstract boolean exchangeDeclare();

    @Nullable
    abstract String routingKey();

    abstract RecordIdPolicy recordIdPolicy();

    abstract long maxNumRecords();

    @Nullable
    abstract Duration maxReadTime();

    abstract TimestampPolicyFactory timestampPolicyFactory();

    abstract SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setQueue(String queue);

      abstract Builder setQueueDeclare(boolean queueDeclare);

      abstract Builder setExchange(String exchange);

      abstract Builder setExchangeType(String exchangeType);

      abstract Builder setExchangeDeclare(boolean exchangeDeclare);

      abstract Builder setRoutingKey(String routingKey);

      abstract Builder setRecordIdPolicy(RecordIdPolicy recordIdPolicy);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Builder setTimestampPolicyFactory(TimestampPolicyFactory timestampPolicyFactory);

      abstract Builder setConnectionHandlerProviderFn(
          SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn);

      abstract Read build();
    }

    /**
     * Defines a means of obtaining a connection to RabbitMQ in terms of the provided URI. This url
     * is expected to contain a UserInfo segment with username and password.
     *
     * @param uri URI to use to connect to rabbit
     * @see ConnectionProviderFromUri for how the URI will be used to obtain a Connection. Use
     *     {@link #withConnectionHandlerProviderFn(SerializableFunction)} if a different strategy is
     *     required.
     */
    public Read withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return withConnectionHandlerProviderFn(new ConnectionProviderFromUri(uri));
    }

    /**
     * Defines a means of obtaining a connection to RabbitMQ.
     *
     * <p>In most cases, {@link #withUri(String)} can be used instead of this more general
     * mechanism.
     */
    public Read withConnectionHandlerProviderFn(
        SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn) {
      checkArgument(
          connectionHandlerProviderFn != null, "connection handler provider can not be null");
      return builder().setConnectionHandlerProviderFn(connectionHandlerProviderFn).build();
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
     * @see <a
     *     href="https://www.cloudamqp.com/blog/2015-09-03-part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html"/>
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

    public Read withRecordIdPolicy(RecordIdPolicy policy) {
      return builder().setRecordIdPolicy(policy).build();
    }

    /**
     * Sets {@link TimestampPolicy} to {@link TimestampPolicyFactory.ProcessingTimePolicy}. This is
     * the default timestamp policy. It assigns processing time to each record. Specifically, this
     * is the timestamp when the record becomes 'current' in the reader. The watermark aways
     * advances to current time. If messages are delivered to the rabbit queue with the Timestamp
     * property set, {@link #withTimestampPluginCompatPolicyFactory(Duration)}} is recommended over
     * this.
     */
    public Read withProcessingTime() {
      return withTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime());
    }

    /**
     * Sets the timestamps policy based on an <a href
     * ="https://github.com/rabbitmq/rabbitmq-message-timestamp">RabbitMQ Message Timestamp
     * Plugin</a>-compatible message. It is an error if a record's {@code timestamp} property is not
     * populated and the {@code timestamp_in_ms} header is missing or malformed . The timestamps
     * within a queue are expected to be roughly monotonically increasing with a cap on out of order
     * delays (e.g. 'max delay' of 1 minute). The watermark at any time is '({@code earliest(now(),
     * latest(event timestamps seen so far)) - max delay})'.
     *
     * <p>However, the watermark is never set to a timestamp in the future and is capped to 'now -
     * max delay'. In addition, the watermark * is advanced to 'now - max delay' when the queue has
     * caught up (previous read attempt returned no * message and/or estimated backlog per {@code
     * GetResult} is zero)
     *
     * @param maxDelay For any record in the queue partition, the timestamp of any subsequent record
     *     is expected to be after {@code current record timestamp - maxDelay}.
     * @see <a href="https://github.com/rabbitmq/rabbitmq-message-timestamp">the RabbitMq Message
     *     Timestamp Plugin</a>
     * @see <a href="https://www.rabbitmq.com/amqp-0-9-1-reference.html#class.basic">basic Timestamp
     *     property</a>
     * @see CustomTimestampPolicyWithLimitedDelay for how the extracted timestamp will be used in
     *     Beam
     */
    public Read withTimestampPluginCompatPolicyFactory(Duration maxDelay) {
      return withTimestampPolicyFactory(TimestampPolicyFactory.withTimestampPluginCompat(maxDelay));
    }

    /**
     * Sets a timestamp policy based on a custom timestamp extration strategy where the timestamps
     * within a queue are expected to be roughly monotonically increasing with a cap on out of order
     * delays (e.g. 'max delay' of 1 minute). The watermark at any time is '({@code earliest(now(),
     * latest(event timestamps seen so far)) - max delay})'.
     *
     * <p>However, the watermark is never set to a timestamp in the future and is capped to 'now -
     * max delay'. In addition, the watermark * is advanced to 'now - max delay' when the queue has
     * caught up (previous read attempt returned no * message and/or estimated backlog per {@code
     * GetResult} is zero).
     *
     * <p>If your timestamp approach is compatible with the <a href
     * ="https://github.com/rabbitmq/rabbitmq-message-timestamp">RabbitMQ Message Timestamp
     * Plugin</a>, use {@link #withTimestampPluginCompatPolicyFactory(Duration)} instead.
     *
     * @param maxDelay For any record in the queue partition, the timestamp of any subsequent record
     *     is expected to be after {@code current record timestamp - maxDelay}.
     * @param timestampExtractor a means of extracting the event time from a rabbitmq message
     * @see CustomTimestampPolicyWithLimitedDelay for how the extracted timestamp will be used in
     *     Beam
     */
    public Read withTimestampPluginCompatPolicyFactory(
        Duration maxDelay, SerializableFunction<RabbitMqMessage, Instant> timestampExtractor) {
      return withTimestampPolicyFactory(
          TimestampPolicyFactory.withTimestamp(maxDelay, timestampExtractor));
    }

    public Read withTimestampPolicyFactory(TimestampPolicyFactory timestampPolicyFactory) {
      return builder().setTimestampPolicyFactory(timestampPolicyFactory).build();
    }

    @Override
    public PCollection<RabbitMqMessage> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<RabbitMqMessage> unbounded =
          org.apache.beam.sdk.io.Read.from(new RabbitMqSource(this));

      PTransform<PBegin, PCollection<RabbitMqMessage>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }
  }

  /** A {@link PTransform} to publish messages to a RabbitMQ server. */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<RabbitMqMessage>, PCollection<?>> {

    @Nullable
    abstract String exchange();

    @Nullable
    abstract String exchangeType();

    abstract boolean exchangeDeclare();

    @Nullable
    abstract String queue();

    abstract boolean queueDeclare();

    abstract SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setExchange(String exchange);

      abstract Builder setExchangeType(String exchangeType);

      abstract Builder setExchangeDeclare(boolean exchangeDeclare);

      abstract Builder setQueue(String queue);

      abstract Builder setQueueDeclare(boolean queueDeclare);

      abstract Builder setConnectionHandlerProviderFn(
          SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn);

      abstract Write build();
    }

    /**
     * Defines a means of obtaining a connection to RabbitMQ in terms of the provided URI. This url
     * is expected to contain a UserInfo segment with username and password.
     *
     * @param uri URI to use to connect to rabbit
     * @see ConnectionProviderFromUri for how the URI will be used to obtain a Connection. Use
     *     {@link #withConnectionHandlerProviderFn(SerializableFunction)} if a different strategy is
     *     required.
     */
    public Write withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return withConnectionHandlerProviderFn(new ConnectionProviderFromUri(uri));
    }

    /**
     * Defines a means of obtaining a connection to RabbitMQ.
     *
     * <p>In most cases, {@link #withUri(String)} can be used instead of this more general
     * mechanism.
     */
    public Write withConnectionHandlerProviderFn(
        SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn) {
      checkArgument(
          connectionHandlerProviderFn != null, "connection handler provider can not be null");
      return builder().setConnectionHandlerProviderFn(connectionHandlerProviderFn).build();
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

      private UUID writerId;
      private transient ChannelLeaser channelLeaser;

      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        if (writerId == null) {
          writerId = UUID.randomUUID();
        }
        if (channelLeaser == null) {
          channelLeaser = spec.connectionHandlerProviderFn().apply(null);
        }

        ChannelLeaser.UseChannelFunction<Void> setupFn =
            channel -> {
              if (spec.exchange() != null && spec.exchangeDeclare()) {
                channel.exchangeDeclare(spec.exchange(), spec.exchangeType());
              }
              if (spec.queue() != null && spec.queueDeclare()) {
                channel.queueDeclare(
                    spec.queue(), /* durable */
                    true, /* exclusive */
                    false, /* auto-delete */
                    false, /* arguments */
                    null);
              }
              return null;
            };

        channelLeaser.useChannel(writerId, setupFn);
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        RabbitMqMessage message = c.element();

        // TODO: get rid of 'exchange' vs 'queue'
        if (spec.exchange() != null) {
          ChannelLeaser.UseChannelFunction<Void> basicPublishFn =
              channel -> {
                channel.basicPublish(
                    spec.exchange(),
                    message.getRoutingKey(),
                    message.createProperties(),
                    message.getBody());
                return null;
              };

          channelLeaser.useChannel(writerId, basicPublishFn);
        }

        if (spec.queue() != null) {

          ChannelLeaser.UseChannelFunction<Void> basicPublishFn =
              channel -> {
                // TODO: what's with this ridiculous hard-coding of message props?
                channel.basicPublish(
                    "", spec.queue(), MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBody());
                return null;
              };

          channelLeaser.useChannel(writerId, basicPublishFn);
        }
      }

      @Teardown
      public void teardown() throws Exception {
        if (writerId != null && channelLeaser != null) {
          channelLeaser.closeChannel(writerId);
        }
      }
    }
  }
}
