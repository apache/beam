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
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nonnull;
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
 * <h3>Consuming messages from RabbitMQ server</h3>
 *
 * <p>{@link RabbitMqIO} {@link Read} returns an unbounded {@link PCollection} containing RabbitMQ
 * messages body (as {@code byte[]}) wrapped as {@link RabbitMqMessage}.
 *
 * <p>To configure a RabbitMQ source, you have to provide a RabbitMQ {@code URI} to connect to a
 * RabbitMQ broker. From there, the various configurable parameters center around the type of
 * exchange to be used and how to bind messages to the expected queue.
 *
 * <p>For unbounded readers, it is suggested that a durable, non-auto-delete queue be created
 * outside of this runtime. Having an intermittent-connected client declare exchanges and queues is
 * fickle as declarations fail if any properties don't match how they were initially declared. If an
 * ephemeral queue is used, it will not be amenable to use with more than one reader at a time, and
 * all messages will be dropped when the Beam client disconnects, which is generally unsafe.
 *
 * <p>Because AMQP is a multi-paradigm messaging protocol, learning how to configure RabbitMqIO in
 * Beam for different use cases is perhaps best learned by exeample:
 *
 * <pre>{@code
 * // for clarity of example params, as a context-less 'true' is confusing
 * boolean doNotDeclare = false;
 * boolean declare = true;
 *
 * // EXAMPLE 1: TOPIC EXCHANGES:
 * //
 * // read all messages to a topic exchange where topics are of the form
 * // "appname.environment.loglevel", e.g. "foo.prod.info" or "bar.stg.debug",
 * // looking to process all messages for any application in production
 *
 * PCollection<RabbitMqMessage> messages = pipeline.apply(
 *   RabbitMqIO.read()
 *     .withUri("amqp://user:password@localhost:5672")
 *     .withTopicExchange("my-existing-topic-exchange", "*.prod.*")
 *     .withQueue("my-existing-beam-queue", doNotDeclare));
 *
 * // EXAMPLE 2: DEFAULT EXCHANGE / DIRECT EXCHANGES:
 * // in a direct exchange, including the default, messages are delivered
 * // using a routing key with value exactly equal to a queue name.
 * // in this example, the queue will be declared, which means it either must not
 * // already exist, or must already exist with the same properties (durable, non-auto-delete)
 *
 * // all messages must be published with routing key "my-existing-beam-queue"
 * PCollection<RabbitMqMessage> messages = pipeline.apply(
 *   RabbitMqIO.read()
 *     .withUri("amqp://user:password@localhost:5672")
 *     .withDefaultExchange()
 *     .withQueue("my-existing-beam-queue", declare));
 *
 * // EXAMPLE 3: FANOUT EXCHANGES:
 * // also known as 'pubsub' this type of exchange routes all incoming messages to
 * // all queues bound to it, regardless of routing key
 *
 * PCollection<RabbitMqMessage> messages = pipeline.apply(
 *   RabbitMqIO.read()
 *     .withUri("amqp://user:password@localhost:5672")
 *     .withFanoutExchange("some-pubsub-exchange")
 *     .withQueue("my-existing-beam-queue", declare));
 *
 * }</pre>
 *
 * General guidelines:
 *
 * <ul>
 *   <li>Use a previously-declared exchange; Beam is not a great tool for managing your rabbitmq
 *       broker.
 *   <li>Use a durable, non-auto-delete queue. This will allow safe parallelization of reads, and
 *       when used along with persistent messages will ensure no messages are lost when clients
 *       disconnect or on rabbitmq server crash.
 *   <li>A Direct Exchange, including the default exchange, is a reasonable use case if the queue is
 *       populated in advance and will be read in a batch-style Beam job. Otherwise, you'll likely
 *       want an exchange that routes all desired messages to an already-declared, durable queue
 *       such that Beam reads can pick up from wherever they left off without having lost any
 *       messages.
 * </ul>
 *
 * <h3>Publishing messages to RabbitMQ server</h3>
 *
 * <p>{@link RabbitMqIO} {@link Write} can send {@link RabbitMqMessage} to a RabbitMQ server queue
 * or exchange.
 *
 * <p>As for the {@link Read}, the {@link Write} is configured with a RabbitMQ URI. Unlike reading,
 * however, writing is not really multi-paradigm; all that's required is the name of an exchange and
 * the routing key of the message.
 *
 * <p>Example
 *
 * <pre>{@code
 * // Publishing to any existing exchange
 * pipeline
 *   .apply(...) // provide PCollection<RabbitMqMessage> with non-null routingKey
 *   .apply(
 *     RabbitMqIO.write()
 *       .withUri("amqp://user:password@localhost:5672")
 *       .withExchange("EXCHANGE"));
 * }</pre>
 *
 * NOTE: "headers" exchanges are not currently supported by this IO.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class RabbitMqIO {
  public static Read read() {
    return new AutoValue_RabbitMqIO_Read.Builder()
        // until a queue is specified, this will have to be ephemeral
        .setQueueDeclare(false)
        // default exchange
        .setExchange("")
        .setExchangeType("direct")
        // this policy is only appropriate for pipelines with at-least-once semantics
        // and capable of handling potentially a large number of repeatedly-delivered messages
        .setRecordIdPolicy(RecordIdPolicy.alwaysUnique())
        // unbounded reads
        .setMaxReadTime(null)
        .setMaxNumRecords(Long.MAX_VALUE)
        // processing-time only policy by default; often not a great choice
        .setTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime())
        .build();
  }

  public static Write write() {
    return new AutoValue_RabbitMqIO_Write.Builder().build();
  }

  private RabbitMqIO() {}

  /** A {@link PTransform} to consume messages from RabbitMQ server. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<RabbitMqMessage>> {

    @Nullable
    abstract String queue();

    abstract boolean queueDeclare();

    abstract String exchange();

    abstract String exchangeType();

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
     * Reads messages from the specified queue. If {@code declareQueue} is {@code true} then an
     * attempt will be made to declare this queue when connecting to Rabbit. If using a non-direct
     * or non-default exchange type, you must specify an appropriate routing key when specifying the
     * exchange.
     *
     * <p>NOTE: When declaring a queue or exchange that already exists, the properties specified in
     * the declaration must match those of the existing queue or exchange. That is, if you declare a
     * queue to be non-durable but a durable queue already exists with the same name, the
     * declaration will fail. When declaring a queue, RabbitMqIO will declare it to be non-durable.
     *
     * @param queue the name of the queue to bind this reader to
     * @param declareQueue If {@code true}, {@link RabbitMqIO} will declare a durable,
     *     non-auto-deleted queue. If another application created the queue, this is not required
     *     and should be set to {@code false}
     */
    public Read withQueue(String queue, boolean declareQueue) {
      checkArgument(queue != null, "queue name can not be null");
      return builder().setQueue(queue).setQueueDeclare(declareQueue).build();
    }

    /**
     * Configures this Reader to use an anonymously-named, non-durable queue that will be
     * automatically deleted upon disconnect. This is not a useful setting in production when any
     * individual reader node may disconnect at any time, but can be useful for testing.
     */
    public Read withEphemeralQueue() {
      return builder().setQueue(null).setQueueDeclare(false).build();
    }

    /**
     * In AMQP, messages are published to an exchange and routed to queues based on the exchange
     * type and a queue binding. Most exchange types utilize the routingKey to determine which
     * queues to deliver messages to. It is incumbent upon the developer to understand the paradigm
     * in place to determine whether to declare a queue, what the appropriate binding should be, and
     * what routingKey will be in use.
     *
     * @see <a
     *     href="https://www.cloudamqp.com/blog/2015-09-03-part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html"/>
     *     for a write-up on exchange types and routing semantics
     */
    private Read.Builder withExchange(String name, String type, @Nullable String routingKey) {
      checkArgument(name != null, "exchange name can not be null");
      checkArgument(type != null, "exchange type can not be null");
      return builder().setExchange(name).setExchangeType(type).setRoutingKey(routingKey);
    }

    /**
     * Declares the use of a fanout exchange with the given name.
     *
     * <p>Routing key is not accepted as all messages are published to all bound queues when using a
     * fanout exchange.
     *
     * @param name the name of the exchange
     */
    public Read withFanoutExchange(String name) {
      return withExchange(name, "fanout", null).build();
    }

    /**
     * Declares the use of a topic exchange with the given name and mandatory routing key.
     *
     * @param name the name of the topic exchange to use
     * @param routingKey the routing key / pattern to be used for routing incoming messages to the
     *     bound queue.
     */
    public Read withTopicExchange(String name, @Nonnull String routingKey) {
      String key =
          Optional.ofNullable(routingKey).map(String::trim).filter(s -> !s.isEmpty()).orElse(null);
      checkArgument(key != null, "routing key must be defined when using a topic exchange");

      return withExchange(name, "topic", key).build();
    }

    /**
     * Declares the use of a non-default direct exchange with the given name.
     *
     * <p>Routing key is not supplied here as the routing key used must exactly match the queue name
     * specified in {@link #withQueue(String, boolean)}
     *
     * @param name the name of the direct exchange to use. If the desired exchange is the default
     *     exchange, use {@link #withDefaultExchange()} instead.
     */
    public Read withDirectExchange(String name) {
      checkArgument(
          name != null && !"".equals(name.trim()),
          "Direct exchange requires a name. If the default exchange was intended, use withDefaultExchange()");

      return withExchange(name, "direct", null).build();
    }

    /**
     * In RabbitMQ, there is a pre-defined instance of a direct exchange known as the 'default
     * exchange', whose name is the empty string. Calling this specifies that this exchange should
     * be used. Note that when using a direct exchange, you'll most likely want to set {@link
     * #withQueue(String, boolean)} as messages will only be delivered to a queue read by Beam if
     * they were published with a routing key whose name matches the queue name.
     */
    public Read withDefaultExchange() {
      return builder().setExchange("").setExchangeType("direct").setRoutingKey(null).build();
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

    abstract SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setExchange(String exchange);

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
     * Defines the existing exchange where the messages will be sent.
     *
     * <p>By calling this function {@code exchangeDeclare} will be set to {@code false}
     */
    public Write withExchange(String exchange) {
      checkArgument(exchange != null, "exchange can not be null");
      return builder().setExchange(exchange).build();
    }

    @Override
    public PCollection<?> expand(PCollection<RabbitMqMessage> input) {
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

        channelLeaser.useChannel(writerId, ch -> null);
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        RabbitMqMessage message = c.element();

        if (spec.exchange() != null) {
          ChannelLeaser.UseChannelFunction<Void> basicPublishFn =
              channel -> {
                channel.basicPublish(
                    spec.exchange(),
                    message.routingKey(),
                    message.createProperties(),
                    message.body());
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
