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
import java.net.URI;
import java.net.URISyntaxException;
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
 * <h3>Consuming messages from RabbitMQ server</h3>
 *
 * <p>{@link RabbitMqIO} {@link Read} returns an unbounded {@link PCollection} containing RabbitMQ
 * messages body (as {@code byte[]}) wrapped as {@link RabbitMqMessage}.
 *
 * <p>To configure a RabbitMQ source, you have to provide a RabbitMQ {@code URI} to connect to a
 * RabbitMQ broker. From there, the various configurable parameters center around {@link
 * ReadParadigm} used.
 *
 * <p>The safest way to use this IO is to use a pre-existing queue. This is analogous to how one
 * might set up a PubSubIO subscription before use. In general, some system or user will have
 * declared an exchange and a durable queue, and RabbitMqIO will read from this queue directly. This
 * is the {@link ReadParadigm.ExistingQueue} approach. However this is not always appropriate and
 * RabbitMqIO can declare a queue and bind it to incoming messages on an exchange via {@link
 * ReadParadigm.NewQueue}. Despite the name, the queue isn't *necessarily* new, but it will be
 * re-declared and the routing key will overwrite any previous settings. This is safe even if the
 * queue already exists, provided it was declared as durable, non-autodelete, and non-exclusive and
 * the newly specified routing key is appropriate (if using a fanout or direct exchange, this is a
 * non-issue).
 *
 * <p>Because Beam runners are free to close readers and start new ones, it's *highly* likely,
 * especially in an unbounded streaming reader, that messages will be re-delivered (e.g. channel was
 * closed before the messages were acknowledged and were re-read on a new reader). For this reason,
 * it's *highly* encouraged callers specify an appropriate {@link RecordIdPolicy}. The default is to
 * allow for messages to be replayed, which may be acceptable if your pipeline is built for
 * at-least-once data processing, but is likely undesirable. The safest approach would be to utilize
 * the amqp property {@code messageId}, ensure it's set to a globally unique value such as a UUID,
 * and use {@link RecordIdPolicy#messageId()}.
 *
 * <p>Because AMQP is a multi-paradigm messaging protocol, learning how to configure RabbitMqIO in
 * Beam for different use cases is perhaps best learned by example:
 *
 * <pre>{@code
 * // EXAMPLE 1: READ FROM AN EXISTING QUEUE
 * // (Secondary: using the amqp messageId property for deduping messages)
 *
 * // this approach does not require the caller to know anything about the exchange
 * // or queue bindings and will simply read any messages delivered to the named queue.
 * // that is, this approach is appropriate for any exchange(s) in use
 *
 * RabbitMqIO.read()
 *   .withUri("amqp://user:password@localhost:5672")
 *   .withReadParadigm(ReadParadigm.ExistingQueue.of("my-existing-queue"))
 *   .withRecordIdPolicy(RecordIdPolicy.messageId())
 *
 * // ----------
 *
 * // EXAMPLE 2: READ FROM A NEW QUEUE ON A FANOUT EXCHANGE (pubsub)
 * // (Secondary: using a combination of message body hash and timestamp for deduping)
 *
 * // in a fanout exchange, all messages are delivered to all queues
 *
 * RabbitMqIO.read()
 *   .withUri("amqp://user:password@localhost:5672")
 *   .withReadParadigm(
 *     ReadParadigm.NewQueue.fanoutExchange("some-fanout-exchange", "my-beam-queue"))
 *   .withRecordIdPolicy(RecordIdPolicy.bodyWithTimestamp())
 *
 * // ----------
 *
 * // EXAMPLE 3: READ FROM A NEW QUEUE ON THE DEFAULT EXCHANGE
 * // (Secondary: deduping using a hash of the body; appropriate if all messages
 *     contain some unique data, such as as UUID or are effectively unique by a
 *     some data like an embedded timestamp along with relatively distinct message
 *     bodies)
 *
 * // the default exchange is a built-in implementation of a direct exchange, named ""
 * RabbitMqIO.read()
 *   .withUri("amqp://user:password@localhost:5672")
 *   .withReadParadigm(ReadParadigm.NewQueue.defaultExchange("my-beam-queue"))
 *   .withRecordIdPolicy(RecordIdPolicy.bodySha256())
 *
 * // ----------
 *
 * // EXAMPLE 4: READ FROM A NEW QUEUE ON A TOPIC EXCHANGE
 * // (Secondary: do not dedupe messages; redeliver any that were not ack'd)
 *
 * // reads all messages to a topic exchange where topics are of the form
 * // "appname.environment.loglevel", e.g. "foo.prod.info" or "bar.stg.debug".
 * // in this example, the goal is to process all messages for any application
 * // in production
 *
 * RabbitMqIO.read()
 *   .withUri("amqp://user:password@localhost:5672")
 *   .withReadParadigm(ReadParadigm.NewQueue.topicExchange(
 *     "some-topic-exchange", "my-beam-queue", "*.prod.*"
 *   ))
 *   .withRecordIdPolicy(RecordIdPolicy.alwaysUnique())
 * }</pre>
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
        // should force the user to define this, as this queue won't already exist
        .setReadParadigm(
            ReadParadigm.ExistingQueue.of("beam-unconfigured-read-queue-" + UUID.randomUUID()))
        // this policy is only appropriate for pipelines with at-least-once semantics
        // and capable of handling potentially a large number of repeatedly-delivered messages
        .setRecordIdPolicy(RecordIdPolicy.alwaysUnique())
        // unbounded reads
        .setMaxReadTime(null)
        .setMaxNumRecords(Long.MAX_VALUE)
        // processing-time only policy by default; often not a great choice
        .setTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime())
        // only reasonable for local development
        .setConnectionHandlerProviderFn(
            new ConnectionProviderFromUri("amqp://guest:guest@localhost:5672/"))
        .build();
  }

  public static Write write() {
    return new AutoValue_RabbitMqIO_Write.Builder()
        .setConnectionHandlerProviderFn(
            new ConnectionProviderFromUri("amqp://guest:guest@localhost:5672/"))
        .build();
  }

  private RabbitMqIO() {}

  /** A {@link PTransform} to consume messages from RabbitMQ server. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<RabbitMqMessage>> {

    abstract ReadParadigm readParadigm();

    abstract RecordIdPolicy recordIdPolicy();

    abstract long maxNumRecords();

    @Nullable
    abstract Duration maxReadTime();

    abstract TimestampPolicyFactory timestampPolicyFactory();

    abstract SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setReadParadigm(ReadParadigm paradigm);

      abstract Builder setRecordIdPolicy(RecordIdPolicy recordIdPolicy);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Builder setTimestampPolicyFactory(TimestampPolicyFactory timestampPolicyFactory);

      abstract Builder setConnectionHandlerProviderFn(
          SerializableFunction<Void, ConnectionHandler> connectionHandlerProviderFn);

      abstract Read autoBuild();

      public Read build() {
        Read built = autoBuild();

        // validation

        ReadParadigm paradigm = built.readParadigm();
        checkArgument(
            paradigm instanceof ReadParadigm.ExistingQueue
                || paradigm instanceof ReadParadigm.NewQueue,
            "Illegal instance of ReadParadigm given. Must be a non-null instance of NewQueue or ExistingQueue");

        return built;
      }
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
      try {
        new URI(uri);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(
            "Malformed rabbit mq supplied (omitted here lest it contain username/password)");
      }
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

    public Read withReadParadigm(ReadParadigm paradigm) {
      return builder().setReadParadigm(paradigm).build();
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
