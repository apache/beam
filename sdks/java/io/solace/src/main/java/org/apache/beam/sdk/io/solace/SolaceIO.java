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
package org.apache.beam.sdk.io.solace;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.GCPSecretSessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.SempClientFactory;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.Solace.SolaceRecordMapper;
import org.apache.beam.sdk.io.solace.read.UnboundedSolaceSource;
import org.apache.beam.sdk.io.solace.write.AddShardKeyDoFn;
import org.apache.beam.sdk.io.solace.write.SolaceOutput;
import org.apache.beam.sdk.io.solace.write.UnboundedBatchedSolaceWriter;
import org.apache.beam.sdk.io.solace.write.UnboundedStreamingSolaceWriter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} to read and write from/to <a href="https://solace.com/">Solace</a> event
 * broker.
 *
 * <p>Note: Internal use only; this API is beta and subject to change.
 *
 * <h2>Reading from Solace</h2>
 *
 * To read from Solace, use the {@link SolaceIO#read()} or {@link SolaceIO#read(TypeDescriptor,
 * SerializableFunction, SerializableFunction)}.
 *
 * <h3>No-argument {@link SolaceIO#read()} top-level method</h3>
 *
 * <p>This method returns a PCollection of {@link Solace.Record} objects. It uses a default mapper
 * ({@link SolaceRecordMapper#map(BytesXMLMessage)}) to map from the received {@link
 * BytesXMLMessage} from Solace, to the {@link Solace.Record} objects.
 *
 * <p>By default, it also uses a {@link BytesXMLMessage#getSenderTimestamp()} for watermark
 * estimation. This {@link SerializableFunction} can be overridden with {@link
 * Read#withTimestampFn(SerializableFunction)} method.
 *
 * <p>When using this method, the Coders are inferred automatically.
 *
 * <h3>Advanced {@link SolaceIO#read(TypeDescriptor, SerializableFunction, SerializableFunction)}
 * top-level method</h3>
 *
 * <p>With this method, the user can:
 *
 * <ul>
 *   <li>specify a custom output type for the PTransform (for example their own class consisting
 *       only of the relevant fields, optimized for their use-case), and
 *   <li>create a custom mapping between {@link BytesXMLMessage} and their output type and
 *   <li>specify what field to use for watermark estimation from their mapped field (for example, in
 *       this method the user can use a field which is encoded in the payload as a timestamp, which
 *       cannot be done with the {@link SolaceIO#read()} method.
 * </ul>
 *
 * <h3>Reading from a queue ({@link Read#from(Solace.Queue)}} or a topic ({@link
 * Read#from(Solace.Topic)})</h3>
 *
 * <p>Regardless of the top-level read method choice, the user can specify whether to read from a
 * Queue - {@link Read#from(Solace.Queue)}, or a Topic {@link Read#from(Solace.Topic)}.
 *
 * <p>Note: when a user specifies to read from a Topic, the connector will create a matching Queue
 * and a Subscription. The user must ensure that the SEMP API is reachable from the driver program
 * and must provide credentials that have `write` permission to the <a
 * href="https://docs.solace.com/Admin/SEMP/Using-SEMP.htm">SEMP Config API</a>. The created Queue
 * will be non-exclusive. The Queue will not be deleted when the pipeline is terminated.
 *
 * <p>Note: If the user specifies to read from a Queue, <a
 * href="https://beam.apache.org/documentation/programming-guide/#overview">the driver program</a>
 * will execute a call to the SEMP API to check if the Queue is `exclusive` or `non-exclusive`. The
 * user must ensure that the SEMP API is reachable from the driver program and provide credentials
 * with `read` permission to the {@link Read#withSempClientFactory(SempClientFactory)}.
 *
 * <h3>Usage example</h3>
 *
 * <h4>The no-arg {@link SolaceIO#read()} method</h4>
 *
 * <p>The minimal example - reading from an existing Queue, using the no-arg {@link SolaceIO#read()}
 * method, with all the default configuration options.
 *
 * <pre>{@code
 * PCollection<Solace.Record> events =
 *   pipeline.apply(
 *     SolaceIO.read()
 *         .from(Queue.fromName("your-queue-name"))
 *         .withSempClientFactory(
 *                 BasicAuthSempClientFactory.builder()
 *                         .host("your-host-name-with-protocol") // e.g. "http://12.34.56.78:8080"
 *                         .username("semp-username")
 *                         .password("semp-password")
 *                         .vpnName("vpn-name")
 *                         .build())
 *         .withSessionServiceFactory(
 *                 BasicAuthJcsmpSessionServiceFactory.builder()
 *                         .host("your-host-name")
 *                               // e.g. "12.34.56.78", or "[fe80::1]", or "12.34.56.78:4444"
 *                         .username("username")
 *                         .password("password")
 *                         .vpnName("vpn-name")
 *                         .build()));
 * }</pre>
 *
 * <h4>The advanced {@link SolaceIO#read(TypeDescriptor, SerializableFunction,
 * SerializableFunction)} method</h4>
 *
 * <p>When using this method you can specify a custom output PCollection type and a custom timestamp
 * function.
 *
 * <pre>{@code
 * {@literal @}DefaultSchema(JavaBeanSchema.class)
 * public static class SimpleRecord {
 *    public String payload;
 *    public String messageId;
 *    public Instant timestamp;
 *
 *    public SimpleRecord() {}
 *
 *    public SimpleRecord(String payload, String messageId, Instant timestamp) {
 *        this.payload = payload;
 *        this.messageId = messageId;
 *        this.timestamp = timestamp;
 *    }
 * }
 *
 * private static SimpleRecord toSimpleRecord(BytesXMLMessage record) {
 *    if (record == null) {
 *        return null;
 *    }
 *    return new SimpleRecord(
 *            new String(record.getBytes(), StandardCharsets.UTF_8),
 *            record.getApplicationMessageId(),
 *            record.getSenderTimestamp() != null
 *                    ? Instant.ofEpochMilli(record.getSenderTimestamp())
 *                    : Instant.now());
 * }
 *
 * PCollection<SimpleRecord> events =
 *  pipeline.apply(
 *      SolaceIO.read(
 *                      TypeDescriptor.of(SimpleRecord.class),
 *                      record -> toSimpleRecord(record),
 *                      record -> record.timestamp)
 *              .from(Topic.fromName("your-topic-name"))
 *              .withSempClientFactory(...)
 *              .withSessionServiceFactory(...);
 *
 *
 * }</pre>
 *
 * <h3>Authentication</h3>
 *
 * <p>When reading from Solace, the user must use {@link
 * Read#withSessionServiceFactory(SessionServiceFactory)} to create a JCSMP session and {@link
 * Read#withSempClientFactory(SempClientFactory)} to authenticate to the SEMP API.
 *
 * <p>See {@link Read#withSessionServiceFactory(SessionServiceFactory)} for session authentication.
 * The connector provides implementation of the {@link SessionServiceFactory} using the Basic
 * Authentication: {@link org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionService}.
 *
 * <p>For the authentication to the SEMP API ({@link Read#withSempClientFactory(SempClientFactory)})
 * the connector provides {@link org.apache.beam.sdk.io.solace.broker.BasicAuthSempClientFactory} to
 * authenticate using the Basic Authentication.
 *
 * <h2>Writing</h2>
 *
 * <p>To write to Solace, use {@link #write()} with a {@link PCollection<Solace.Record>}. You can
 * also use {@link #write(SerializableFunction)} to specify a format function to convert the input
 * type to {@link Solace.Record}.
 *
 * <h3>Writing to a static topic or queue</h3>
 *
 * <p>The connector uses the <a href=
 * "https://docs.solace.com/API/Messaging-APIs/JCSMP-API/jcsmp-api-home.htm">Solace JCSMP API</a>.
 * The clients will write to a <a href="https://docs.solace.com/Messaging/SMF-Topics.htm">SMF
 * topic</a> to the port 55555 of the host. If you want to use a different port, specify it in the
 * host property with the format "X.X.X.X:PORT".
 *
 * <p>Once you have a {@link PCollection} of {@link Solace.Record}, you can write to Solace using:
 *
 * <pre>{@code
 * PCollection<Solace.Record> solaceRecs = ...;
 *
 * PCollection<Solace.PublishResult> results =
 *         solaceRecs.apply(
 *                 "Write to Solace",
 *                 SolaceIO.write()
 *                         .to(SolaceIO.topicFromName("some-topic"))
 *                         .withSessionServiceFactory(
 *                            BasicAuthJcsmpSessionServiceFactory.builder()
 *                              .username("username")
 *                              .password("password")
 *                              .host("host:port")
 *                              .build()));
 * }</pre>
 *
 * <p>The above code snippet will write to the VPN named "default", using 4 clients per worker (VM
 * in Dataflow), and a maximum of 20 workers/VMs for writing (default values). You can change the
 * default VPN name by setting the required JCSMP property in the session factory (in this case,
 * with {@link BasicAuthJcsmpSessionServiceFactory#vpnName()}), the number of clients per worker
 * with {@link Write#withNumberOfClientsPerWorker(int)} and the number of parallel write clients
 * using {@link Write#withNumShards(int)}.
 *
 * <h3>Writing to dynamic destinations</h3>
 *
 * To write to dynamic destinations, don't set the {@link Write#to(Solace.Queue)} or {@link
 * Write#to(Solace.Topic)} property and make sure that all the {@link Solace.Record}s have their
 * destination field set to either a topic or a queue. You can do this prior to calling the write
 * connector, or by using a format function and {@link #write(SerializableFunction)}.
 *
 * <p>For instance, you can create a function like the following:
 *
 * <pre>{@code
 * // Generate Record with different destinations
 * SerializableFunction<MyType, Solace.Record> formatFn =
 *    (MyType msg) -> {
 *       int queue = ... // some random number
 *       return Solace.Record.builder()
 *         .setDestination(Solace.Destination.builder()
 *                        .setName(String.format("q%d", queue))
 *                        .setType(Solace.DestinationType.QUEUE)
 *                        .build())
 *         .setMessageId(msg.getMessageId())
 *         .build();
 * };
 * }</pre>
 *
 * And then use the connector as follows:
 *
 * <pre>{@code
 * // Ignore "to" method to use dynamic destinations
 * SolaceOutput solaceResponses = msgs.apply("Write to Solace",
 *   SolaceIO.<MyType>write(formatFn)
 *        .withDeliveryMode(DeliveryMode.PERSISTENT)
 *        .withWriterType(SolaceIO.WriterType.STREAMING)
 * ...
 * }</pre>
 *
 * <h3>Direct and persistent messages, and latency metrics</h3>
 *
 * <p>The connector can write either direct or persistent messages. The default mode is DIRECT.
 *
 * <p>The connector returns a {@link PCollection} of {@link Solace.PublishResult}, that you can use
 * to get a confirmation of messages that have been published, or rejected, but only if it is
 * publishing persistent messages.
 *
 * <p>If you are publishing persistent messages, then you can have some feedback about whether the
 * messages have been published, and some publishing latency metrics. If the message has been
 * published, {@link Solace.PublishResult#getPublished()} will be true. If it is false, it means
 * that the message could not be published, and {@link Solace.PublishResult#getError()} will contain
 * more details about why the message could not be published. To get latency metrics as well as the
 * results, set the property {@link Write#publishLatencyMetrics()}.
 *
 * <h3>Throughput and latency</h3>
 *
 * <p>This connector can work in two main modes: high latency or high throughput. The default mode
 * favors high throughput over high latency. You can control this behavior with the methods {@link
 * Write#withSubmissionMode(SubmissionMode)} and {@link Write#withWriterType(WriterType)}.
 *
 * <p>The default mode works like the following options:
 *
 * <pre>{@code
 * PCollection<Solace.Record> solaceRecs = ...;
 *
 * PCollection<Solace.PublishResult> results =
 *         solaceRecs.apply(
 *                 "Write to Solace",
 *                 SolaceIO.write()
 *                         .to(SolaceIO.topicFromName("some-topic"))
 *                         .withSessionServiceFactory(
 *                            BasicAuthJcsmpSessionServiceFactory.builder()
 *                              .username("username")
 *                              .password("password")
 *                              .host("host:port")
 *                              .build())
 *                         .withSubmissionMode(SubmissionMode.HIGHER_THROUGHPUT)
 *                         .withWriterType(WriterType.BATCHED));
 * }</pre>
 *
 * <p>{@link SubmissionMode#HIGHER_THROUGHPUT} and {@link WriterType#BATCHED} are the default
 * values, and offer the higher possible throughput, and the lowest usage of resources in the runner
 * side (due to the lower backpressure).
 *
 * <p>This connector writes bundles of 50 messages, using a bulk publish JCSMP method. This will
 * increase the latency, since a message needs to "wait" until 50 messages are accumulated, before
 * they are submitted to Solace.
 *
 * <p>For the lowest latency possible, use {@link SubmissionMode#LOWER_LATENCY} and {@link
 * WriterType#STREAMING}.
 *
 * <pre>{@code
 * PCollection<Solace.PublishResult> results =
 *         solaceRecs.apply(
 *                 "Write to Solace",
 *                 SolaceIO.write()
 *                         .to(SolaceIO.topicFromName("some-topic"))
 *                         .withSessionServiceFactory(
 *                            BasicAuthJcsmpSessionServiceFactory.builder()
 *                              .username("username")
 *                              .password("password")
 *                              .host("host:port")
 *                              .build())
 *                         .withSubmissionMode(SubmissionMode.LOWER_LATENCY)
 *                         .withWriterType(WriterType.STREAMING));
 * }</pre>
 *
 * <p>The streaming connector publishes each message individually, without holding up or batching
 * before the message is sent to Solace. This will ensure the lowest possible latency, but it will
 * offer a much lower throughput. The streaming connector does not use state and timers.
 *
 * <p>Both connectors uses state and timers to control the level of parallelism. If you are using
 * Cloud Dataflow, it is recommended that you enable <a
 * href="https://cloud.google.com/dataflow/docs/streaming-engine">Streaming Engine</a> to use this
 * connector.
 *
 * <p>For full control over all the properties, use {@link SubmissionMode#CUSTOM}. The connector
 * will not override any property that you set, and you will have full control over all the JCSMP
 * properties.
 *
 * <h3>Authentication</h3>
 *
 * <p>When writing to Solace, the user must use {@link
 * Write#withSessionServiceFactory(SessionServiceFactory)} to create a JCSMP session.
 *
 * <p>See {@link Write#withSessionServiceFactory(SessionServiceFactory)} for session authentication.
 * The connector provides implementation of the {@link SessionServiceFactory} using basic
 * authentication ({@link BasicAuthJcsmpSessionServiceFactory}), and another implementation using
 * basic authentication but with a password stored as a secret in Google Cloud Secret Manager
 * ({@link GCPSecretSessionServiceFactory})
 *
 * <h2>Connector retries</h2>
 *
 * <p>When the worker using the connector is created, the connector will attempt to connect to
 * Solace.
 *
 * <p>If the client cannot connect to Solace for whatever reason, the connector will retry the
 * connections using the following strategy. There will be a maximum of 4 retries. The first retry
 * will be attempted 1 second after the first connection attempt. Every subsequent retry will
 * multiply that time by a factor of two, with a maximum of 10 seconds.
 *
 * <p>If after those retries the client is still unable to connect to Solace, the connector will
 * attempt to reconnect using the same strategy repeated for every single incoming message. If for
 * some reason, there is a persistent issue that prevents the connection (e.g. client quota
 * exhausted), you will need to stop your job manually, or the connector will keep retrying.
 *
 * <p>This strategy is applied to all the remote calls sent to Solace, either to connect, pull
 * messages, push messages, etc.
 */
@Internal
public class SolaceIO {

  public static final SerializableFunction<Solace.Record, Instant> SENDER_TIMESTAMP_FUNCTION =
      (record) -> {
        Long senderTimestamp = record != null ? record.getSenderTimestamp() : null;
        if (senderTimestamp != null) {
          return Instant.ofEpochMilli(senderTimestamp);
        } else {
          return Instant.now();
        }
      };
  private static final boolean DEFAULT_DEDUPLICATE_RECORDS = false;
  private static final Duration DEFAULT_WATERMARK_IDLE_DURATION_THRESHOLD =
      Duration.standardSeconds(30);
  public static final int DEFAULT_WRITER_NUM_SHARDS = 20;
  public static final int DEFAULT_WRITER_CLIENTS_PER_WORKER = 4;
  public static final Boolean DEFAULT_WRITER_PUBLISH_LATENCY_METRICS = false;
  public static final SubmissionMode DEFAULT_WRITER_SUBMISSION_MODE =
      SubmissionMode.HIGHER_THROUGHPUT;
  public static final DeliveryMode DEFAULT_WRITER_DELIVERY_MODE = DeliveryMode.DIRECT;
  public static final WriterType DEFAULT_WRITER_TYPE = WriterType.BATCHED;

  /** Get a {@link Topic} object from the topic name. */
  static Topic topicFromName(String topicName) {
    return JCSMPFactory.onlyInstance().createTopic(topicName);
  }

  /** Get a {@link Queue} object from the queue name. */
  static Queue queueFromName(String queueName) {
    return JCSMPFactory.onlyInstance().createQueue(queueName);
  }

  /**
   * Convert to a JCSMP destination from a schema-enabled {@link
   * org.apache.beam.sdk.io.solace.data.Solace.Destination}.
   *
   * <p>This method returns a {@link Destination}, which may be either a {@link Topic} or a {@link
   * Queue}
   */
  public static Destination convertToJcsmpDestination(Solace.Destination destination) {
    if (destination.getType().equals(Solace.DestinationType.TOPIC)) {
      return topicFromName(checkNotNull(destination.getName()));
    } else if (destination.getType().equals(Solace.DestinationType.QUEUE)) {
      return queueFromName(checkNotNull(destination.getName()));
    } else {
      throw new IllegalArgumentException(
          "SolaceIO.Write: Unknown destination type: " + destination.getType());
    }
  }

  /**
   * Create a {@link Read} transform, to read from Solace. The ingested records will be mapped to
   * the {@link Solace.Record} objects.
   */
  public static Read<Solace.Record> read() {
    return new Read<Solace.Record>(
        Read.Configuration.<Solace.Record>builder()
            .setTypeDescriptor(TypeDescriptor.of(Solace.Record.class))
            .setParseFn(SolaceRecordMapper::map)
            .setTimestampFn(SENDER_TIMESTAMP_FUNCTION)
            .setDeduplicateRecords(DEFAULT_DEDUPLICATE_RECORDS)
            .setWatermarkIdleDurationThreshold(DEFAULT_WATERMARK_IDLE_DURATION_THRESHOLD));
  }

  /**
   * Create a {@link Read} transform, to read from Solace. Specify a {@link SerializableFunction} to
   * map incoming {@link BytesXMLMessage} records, to the object of your choice. You also need to
   * specify a {@link TypeDescriptor} for your class and the timestamp function which returns an
   * {@link Instant} from the record.
   *
   * <p>The type descriptor will be used to infer a coder from CoderRegistry or Schema Registry. You
   * can initialize a new TypeDescriptor in the following manner:
   *
   * <pre>{@code
   * TypeDescriptor<T> typeDescriptor = TypeDescriptor.of(YourOutputType.class);
   * }</pre>
   */
  public static <T> Read<T> read(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<@Nullable BytesXMLMessage, @Nullable T> parseFn,
      SerializableFunction<T, Instant> timestampFn) {
    checkState(typeDescriptor != null, "SolaceIO.Read: typeDescriptor must not be null");
    checkState(parseFn != null, "SolaceIO.Read: parseFn must not be null");
    checkState(timestampFn != null, "SolaceIO.Read: timestampFn must not be null");
    return new Read<T>(
        Read.Configuration.<T>builder()
            .setTypeDescriptor(typeDescriptor)
            .setParseFn(parseFn)
            .setTimestampFn(timestampFn)
            .setDeduplicateRecords(DEFAULT_DEDUPLICATE_RECORDS)
            .setWatermarkIdleDurationThreshold(DEFAULT_WATERMARK_IDLE_DURATION_THRESHOLD));
  }

  /**
   * Create a {@link Write} transform, to write to Solace with a custom type.
   *
   * <p>If you are using a custom data class, the format function should return a {@link
   * Solace.Record} corresponding to your custom data class instance.
   *
   * <p>If you are using this formatting function with dynamic destinations, you must ensure that
   * you set the right value in the destination value of the {@link Solace.Record} messages.
   */
  public static <T> Write<T> write(SerializableFunction<T, Solace.Record> formatFunction) {
    return Write.<T>builder().setFormatFunction(formatFunction).build();
  }

  /** Create a {@link Write} transform, to write to Solace using {@link Solace.Record} objects. */
  public static Write<Solace.Record> write() {
    return Write.<Solace.Record>builder().build();
  }

  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    @VisibleForTesting final Configuration.Builder<T> configurationBuilder;

    private Read(Configuration.Builder<T> configurationBuilder) {
      this.configurationBuilder = configurationBuilder;
    }

    /** Set the queue name to read from. Use this or the `from(Topic)` method. */
    public Read<T> from(Solace.Queue queue) {
      configurationBuilder.setQueue(queueFromName(queue.getName()));
      return this;
    }

    /** Set the topic name to read from. Use this or the `from(Queue)` method. */
    public Read<T> from(Solace.Topic topic) {
      configurationBuilder.setTopic(topicFromName(topic.getName()));
      return this;
    }

    /**
     * The timestamp function, used for estimating the watermark, mapping the record T to an {@link
     * Instant}
     *
     * <p>Optional when using the no-arg {@link SolaceIO#read()} method. Defaults to {@link
     * SolaceIO#SENDER_TIMESTAMP_FUNCTION}. When using the {@link SolaceIO#read(TypeDescriptor,
     * SerializableFunction, SerializableFunction)} method, the function mapping from T to {@link
     * Instant} has to be passed as an argument.
     */
    public Read<T> withTimestampFn(SerializableFunction<T, Instant> timestampFn) {
      checkState(
          timestampFn != null,
          "SolaceIO.Read: timestampFn must not be null. This function must be set or "
              + "use the no-argument `Read.read()` method");
      configurationBuilder.setTimestampFn(timestampFn);
      return this;
    }

    /**
     * Optional. Sets the maximum number of connections to the broker. The actual number of sessions
     * is determined by this and the number set by the runner. If not set, the number of sessions is
     * determined by the runner. The number of connections created follows this logic:
     * `numberOfConnections = min(maxNumConnections, desiredNumberOfSplits)`, where the
     * `desiredNumberOfSplits` is set by the runner.
     */
    public Read<T> withMaxNumConnections(Integer maxNumConnections) {
      configurationBuilder.setMaxNumConnections(maxNumConnections);
      return this;
    }

    /**
     * Optional. Denotes the duration for which the watermark can be idle. If there are no incoming
     * messages for this ‘idle’ period of time, the watermark is set to a timestamp representing a
     * time earlier than now by the ‘idle’ period of time (e.g. if the ‘idle’ period of time is set
     * to 30 seconds, and there is no new data incoming for 30 seconds, the watermark will be set to
     * max(currentWatermark, now() - 30 seconds). The default watermark idle duration threshold is
     * {@link #DEFAULT_WATERMARK_IDLE_DURATION_THRESHOLD}.
     */
    public Read<T> withWatermarkIdleDurationThreshold(Duration idleDurationThreshold) {
      configurationBuilder.setWatermarkIdleDurationThreshold(idleDurationThreshold);
      return this;
    }

    /**
     * Optional, default: false. Set to deduplicate messages based on the {@link
     * BytesXMLMessage#getApplicationMessageId()} of the incoming {@link BytesXMLMessage}. If the
     * field is null, then the {@link BytesXMLMessage#getReplicationGroupMessageId()} will be used,
     * which is always set by Solace.
     */
    public Read<T> withDeduplicateRecords(boolean deduplicateRecords) {
      configurationBuilder.setDeduplicateRecords(deduplicateRecords);
      return this;
    }

    /**
     * Set a factory that creates a {@link org.apache.beam.sdk.io.solace.broker.SempClientFactory}.
     *
     * <p>The factory `create()` method is invoked in each instance of an {@link
     * org.apache.beam.sdk.io.solace.read.UnboundedSolaceReader}. Created {@link
     * org.apache.beam.sdk.io.solace.broker.SempClient} has to communicate with broker management
     * API. It must support operations such as:
     *
     * <ul>
     *   <li>query for outstanding backlog bytes in a Queue,
     *   <li>query for metadata such as access-type of a Queue,
     *   <li>requesting creation of new Queues.
     * </ul>
     *
     * <p>An existing implementation of the SempClientFactory includes {@link
     * org.apache.beam.sdk.io.solace.broker.BasicAuthSempClientFactory} which implements connection
     * to the SEMP with the Basic Authentication method.
     *
     * <p>To use it, specify the credentials with the builder methods.
     *
     * <p>The format of the host is `[Protocol://]Host[:Port]`
     *
     * <pre>{@code
     * .withSempClientFactory(
     *         BasicAuthSempClientFactory.builder()
     *               .host("your-host-name-with-protocol") // e.g. "http://12.34.56.78:8080"
     *               .username("username")
     *               .password("password")
     *               .vpnName("vpn-name")
     *               .build())
     * }</pre>
     */
    public Read<T> withSempClientFactory(SempClientFactory sempClientFactory) {
      checkState(sempClientFactory != null, "SolaceIO.Read: sempClientFactory must not be null.");
      configurationBuilder.setSempClientFactory(sempClientFactory);
      return this;
    }

    /**
     * Set a factory that creates a {@link SessionService}.
     *
     * <p>The factory `create()` method is invoked in each instance of an {@link
     * org.apache.beam.sdk.io.solace.read.UnboundedSolaceReader}. Created {@link SessionService} has
     * to be able to:
     *
     * <ul>
     *   <li>initialize a connection with the broker,
     *   <li>check liveliness of the connection,
     *   <li>close the connection,
     *   <li>create a {@link org.apache.beam.sdk.io.solace.broker.MessageReceiver}.
     * </ul>
     *
     * <p>An existing implementation of the SempClientFactory includes {@link
     * org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionService} which implements the Basic
     * Authentication to Solace. *
     *
     * <p>To use it, specify the credentials with the builder methods. *
     *
     * <p>The host is the IPv4 or IPv6 or host name of the appliance. IPv6 addresses must be encoded
     * in brackets ([]). For example, "12.34.56.78", or "[fe80::1]". If connecting to a non-default
     * port, it can be specified here using the "Host:Port" format. For example, "12.34.56.78:4444",
     * or "[fe80::1]:4444".
     *
     * <pre>{@code
     * BasicAuthJcsmpSessionServiceFactory.builder()
     *     .host("your-host-name")
     *           // e.g. "12.34.56.78", or "[fe80::1]", or "12.34.56.78:4444"
     *     .username("semp-username")
     *     .password("semp-password")
     *     .vpnName("vpn-name")
     *     .build()));
     * }</pre>
     */
    public Read<T> withSessionServiceFactory(SessionServiceFactory sessionServiceFactory) {
      checkState(
          sessionServiceFactory != null, "SolaceIO.Read: sessionServiceFactory must not be null.");
      configurationBuilder.setSessionServiceFactory(sessionServiceFactory);
      return this;
    }

    @AutoValue
    abstract static class Configuration<T> {

      abstract @Nullable Queue getQueue();

      abstract @Nullable Topic getTopic();

      abstract SerializableFunction<T, Instant> getTimestampFn();

      abstract @Nullable Integer getMaxNumConnections();

      abstract boolean getDeduplicateRecords();

      abstract SerializableFunction<@Nullable BytesXMLMessage, @Nullable T> getParseFn();

      abstract SempClientFactory getSempClientFactory();

      abstract SessionServiceFactory getSessionServiceFactory();

      abstract TypeDescriptor<T> getTypeDescriptor();

      abstract Duration getWatermarkIdleDurationThreshold();

      public static <T> Builder<T> builder() {
        Builder<T> builder =
            new org.apache.beam.sdk.io.solace.AutoValue_SolaceIO_Read_Configuration.Builder<T>();
        return builder;
      }

      @AutoValue.Builder
      public abstract static class Builder<T> {

        abstract Builder<T> setQueue(Queue queue);

        abstract Builder<T> setTopic(Topic topic);

        abstract Builder<T> setTimestampFn(SerializableFunction<T, Instant> timestampFn);

        abstract Builder<T> setMaxNumConnections(Integer maxNumConnections);

        abstract Builder<T> setDeduplicateRecords(boolean deduplicateRecords);

        abstract Builder<T> setParseFn(
            SerializableFunction<@Nullable BytesXMLMessage, @Nullable T> parseFn);

        abstract Builder<T> setSempClientFactory(SempClientFactory brokerServiceFactory);

        abstract Builder<T> setSessionServiceFactory(SessionServiceFactory sessionServiceFactory);

        abstract Builder<T> setTypeDescriptor(TypeDescriptor<T> typeDescriptor);

        abstract Builder<T> setWatermarkIdleDurationThreshold(Duration idleDurationThreshold);

        abstract Configuration<T> build();
      }
    }

    @Override
    public void validate(@Nullable PipelineOptions options) {
      Configuration<T> configuration = configurationBuilder.build();
      checkState(
          (configuration.getQueue() == null ^ configuration.getTopic() == null),
          "SolaceIO.Read: One of the Solace {Queue, Topic} must be set.");
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      Configuration<T> configuration = configurationBuilder.build();
      SempClientFactory sempClientFactory = configuration.getSempClientFactory();
      String jobName = input.getPipeline().getOptions().getJobName();
      Queue initializedQueue =
          initializeQueueForTopicIfNeeded(
              configuration.getQueue(), configuration.getTopic(), jobName, sempClientFactory);

      SessionServiceFactory sessionServiceFactory = configuration.getSessionServiceFactory();
      sessionServiceFactory.setQueue(initializedQueue);

      Coder<T> coder = inferCoder(input.getPipeline(), configuration.getTypeDescriptor());

      return input.apply(
          org.apache.beam.sdk.io.Read.from(
              new UnboundedSolaceSource<>(
                  initializedQueue,
                  sempClientFactory,
                  sessionServiceFactory,
                  configuration.getMaxNumConnections(),
                  configuration.getDeduplicateRecords(),
                  coder,
                  configuration.getTimestampFn(),
                  configuration.getWatermarkIdleDurationThreshold(),
                  configuration.getParseFn())));
    }

    @VisibleForTesting
    Coder<T> inferCoder(Pipeline pipeline, TypeDescriptor<T> typeDescriptor) {
      Coder<T> coderFromCoderRegistry = getFromCoderRegistry(pipeline, typeDescriptor);
      if (coderFromCoderRegistry != null) {
        return coderFromCoderRegistry;
      }

      Coder<T> coderFromSchemaRegistry = getFromSchemaRegistry(pipeline, typeDescriptor);
      if (coderFromSchemaRegistry != null) {
        return coderFromSchemaRegistry;
      }

      throw new RuntimeException(
          "SolaceIO.Read: Cannot infer a coder for the TypeDescriptor. Annotate your"
              + " output class with @DefaultSchema annotation or create a coder manually"
              + " and register it in the CoderRegistry.");
    }

    private @Nullable Coder<T> getFromSchemaRegistry(
        Pipeline pipeline, TypeDescriptor<T> typeDescriptor) {
      try {
        return pipeline.getSchemaRegistry().getSchemaCoder(typeDescriptor);
      } catch (NoSuchSchemaException e) {
        return null;
      }
    }

    private @Nullable Coder<T> getFromCoderRegistry(
        Pipeline pipeline, TypeDescriptor<T> typeDescriptor) {
      try {
        return pipeline.getCoderRegistry().getCoder(typeDescriptor);
      } catch (CannotProvideCoderException e) {
        return null;
      }
    }

    private Queue initializeQueueForTopicIfNeeded(
        @Nullable Queue queue,
        @Nullable Topic topic,
        String jobName,
        SempClientFactory sempClientFactory) {
      Queue initializedQueue;
      if (queue != null) {
        return queue;
      } else {
        String queueName = String.format("queue-%s-%s", topic, jobName);
        try {
          String topicName = checkNotNull(topic).getName();
          initializedQueue = sempClientFactory.create().createQueueForTopic(queueName, topicName);
          LOG.warn(
              "SolaceIO.Read: A new queue {} was created. The Queue will not be"
                  + " deleted when this job finishes. Make sure to remove it yourself"
                  + " when not needed.",
              initializedQueue.getName());
          return initializedQueue;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public enum SubmissionMode {
    HIGHER_THROUGHPUT,
    LOWER_LATENCY,
    CUSTOM, // Don't override any property set by the user
    TESTING // Send acks 1 by 1, this will be very slow, never use this in an actual pipeline!
  }

  public enum WriterType {
    STREAMING,
    BATCHED
  }

  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, SolaceOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(Write.class);

    public static final TupleTag<BadRecord> FAILED_PUBLISH_TAG = new TupleTag<BadRecord>() {};
    public static final TupleTag<Solace.PublishResult> SUCCESSFUL_PUBLISH_TAG =
        new TupleTag<Solace.PublishResult>() {};

    /**
     * Write to a Solace topic.
     *
     * <p>The topic does not need to exist before launching the pipeline.
     *
     * <p>This will write all records to the same topic, ignoring their destination field.
     *
     * <p>Optional. If not specified, the connector will use dynamic destinations based on the
     * destination field of {@link Solace.Record}.
     */
    public Write<T> to(Solace.Topic topic) {
      return toBuilder().setDestination(topicFromName(topic.getName())).build();
    }

    /**
     * Write to a Solace queue.
     *
     * <p>The queue must exist prior to launching the pipeline.
     *
     * <p>This will write all records to the same queue, ignoring their destination field.
     *
     * <p>Optional. If not specified, the connector will use dynamic destinations based on the
     * destination field of {@link Solace.Record}.
     */
    public Write<T> to(Solace.Queue queue) {
      return toBuilder().setDestination(queueFromName(queue.getName())).build();
    }

    /**
     * The number of workers used by the job to write to Solace.
     *
     * <p>This is optional, the default value is 20.
     *
     * <p>This is the maximum value that the job would use, but depending on the amount of data, the
     * actual number of writers may be lower than this value. With the Dataflow runner, the
     * connector will as maximum this number of VMs in the job (but the job itself may use more
     * VMs).
     *
     * <p>Set this number taking into account the limits in the number of clients in your Solace
     * cluster, and the need for performance when writing to Solace (more workers will achieve
     * higher throughput).
     */
    public Write<T> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * The number of clients that each worker will create.
     *
     * <p>This is optional, the default number is 4.
     *
     * <p>The number of clients is per worker. If there are more than one worker, the number of
     * clients will be multiplied by the number of workers. With the Dataflow runner, this will be
     * the number of clients created per VM. The clients will be re-used across different threads in
     * the same worker.
     *
     * <p>Set this number in combination with {@link #withNumShards}, to ensure that the limit for
     * number of clients in your Solace cluster is not exceeded.
     *
     * <p>Normally, using a higher number of clients with fewer workers will achieve better
     * throughput at a lower cost, since the workers are better utilized. A good rule of thumb to
     * use is setting as many clients per worker as vCPUs the worker has.
     */
    public Write<T> withNumberOfClientsPerWorker(int numberOfClientsPerWorker) {
      return toBuilder().setNumberOfClientsPerWorker(numberOfClientsPerWorker).build();
    }

    /**
     * Set the delivery mode. This is optional, the default value is DIRECT.
     *
     * <p>For more details, see <a
     * href="https://docs.solace.com/API/API-Developer-Guide/Message-Delivery-Modes.htm">https://docs.solace.com/API/API-Developer-Guide/Message-Delivery-Modes.htm</a>
     */
    public Write<T> withDeliveryMode(DeliveryMode deliveryMode) {
      return toBuilder().setDeliveryMode(deliveryMode).build();
    }

    /**
     * Publish latency metrics using Beam metrics.
     *
     * <p>Latency metrics are only available if {@link #withDeliveryMode(DeliveryMode)} is set to
     * PERSISTENT. In that mode, latency is measured for each single message, as the time difference
     * between the message creation and the reception of the publishing confirmation.
     *
     * <p>For the batched writer, the creation time is set for every message in a batch shortly
     * before the batch is submitted. So the latency is very close to the actual publishing latency,
     * and it does not take into account the time spent waiting for the batch to be submitted.
     *
     * <p>This is optional, the default value is false (don't publish latency metrics).
     */
    public Write<T> publishLatencyMetrics() {
      return toBuilder().setPublishLatencyMetrics(true).build();
    }

    /**
     * This setting controls the JCSMP property MESSAGE_CALLBACK_ON_REACTOR. Optional.
     *
     * <p>For full details, please check <a
     * href="https://docs.solace.com/API/API-Developer-Guide/Java-API-Best-Practices.htm">https://docs.solace.com/API/API-Developer-Guide/Java-API-Best-Practices.htm</a>.
     *
     * <p>The Solace JCSMP client libraries can dispatch messages using three different modes:
     *
     * <p>One of the modes dispatches messages directly from the same thread that is doing the rest
     * of I/O work. This mode favors lower latency but lower throughput. Set this to LOWER_LATENCY
     * to use that mode (MESSAGE_CALLBACK_ON_REACTOR set to True).
     *
     * <p>Another mode uses a parallel thread to accumulate and dispatch messages. This mode favors
     * higher throughput but also has higher latency. Set this to HIGHER_THROUGHPUT to use that
     * mode. This is the default mode (MESSAGE_CALLBACK_ON_REACTOR set to False).
     *
     * <p>If you prefer to have full control over all the JCSMP properties, set this to CUSTOM, and
     * override the classes {@link SessionServiceFactory} and {@link SessionService} to have full
     * control on how to create the JCSMP sessions and producers used by the connector.
     *
     * <p>This is optional, the default value is HIGHER_THROUGHPUT.
     */
    public Write<T> withSubmissionMode(SubmissionMode submissionMode) {
      return toBuilder().setDispatchMode(submissionMode).build();
    }

    /**
     * Set the type of writer used by the connector. Optional.
     *
     * <p>The Solace writer can either use the JCSMP modes in streaming or batched.
     *
     * <p>In streaming mode, the publishing latency will be lower, but the throughput will also be
     * lower. todo validate the constant sec
     *
     * <p>With the batched mode, messages are accumulated until a batch size of 50 is reached, or
     * {@link UnboundedBatchedSolaceWriter#ACKS_FLUSHING_INTERVAL_SECS} seconds have elapsed since
     * the first message in the batch was received. The 50 messages are sent to Solace in a single
     * batch. This writer offers higher throughput but higher publishing latency, as messages can be
     * held up for up to {@link UnboundedBatchedSolaceWriter#ACKS_FLUSHING_INTERVAL_SECS}5seconds
     * until they are published.
     *
     * <p>Notice that this is the message publishing latency, not the end-to-end latency. For very
     * large scale pipelines, you will probably prefer to use the HIGHER_THROUGHPUT mode, as with
     * lower throughput messages will accumulate in the pipeline, and the end-to-end latency may
     * actually be higher.
     *
     * <p>This is optional, the default is the BATCHED writer.
     */
    public Write<T> withWriterType(WriterType writerType) {
      return toBuilder().setWriterType(writerType).build();
    }

    /**
     * Set the provider used to obtain the properties to initialize a new session in the broker.
     *
     * <p>This provider should define the destination host where the broker is listening, and all
     * the properties related to authentication (base auth, client certificate, etc.).
     */
    public Write<T> withSessionServiceFactory(SessionServiceFactory factory) {
      return toBuilder().setSessionServiceFactory(factory).build();
    }

    /** todo docs */
    public Write<T> withErrorHandler(ErrorHandler<BadRecord, ?> errorHandler) {
      return toBuilder().setErrorHandler(errorHandler).build();
    }

    abstract int getNumShards();

    abstract int getNumberOfClientsPerWorker();

    abstract @Nullable Destination getDestination();

    abstract DeliveryMode getDeliveryMode();

    abstract boolean getPublishLatencyMetrics();

    abstract SubmissionMode getDispatchMode();

    abstract WriterType getWriterType();

    abstract @Nullable SerializableFunction<T, Solace.Record> getFormatFunction();

    abstract @Nullable SessionServiceFactory getSessionServiceFactory();

    abstract @Nullable ErrorHandler<BadRecord, ?> getErrorHandler();

    static <T> Builder<T> builder() {
      return new AutoValue_SolaceIO_Write.Builder<T>()
          .setDeliveryMode(DEFAULT_WRITER_DELIVERY_MODE)
          .setNumShards(DEFAULT_WRITER_NUM_SHARDS)
          .setNumberOfClientsPerWorker(DEFAULT_WRITER_CLIENTS_PER_WORKER)
          .setPublishLatencyMetrics(DEFAULT_WRITER_PUBLISH_LATENCY_METRICS)
          .setDispatchMode(DEFAULT_WRITER_SUBMISSION_MODE)
          .setWriterType(DEFAULT_WRITER_TYPE);
    }

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setNumShards(int numShards);

      abstract Builder<T> setNumberOfClientsPerWorker(int numberOfClientsPerWorker);

      abstract Builder<T> setDestination(Destination topicOrQueue);

      abstract Builder<T> setDeliveryMode(DeliveryMode deliveryMode);

      abstract Builder<T> setPublishLatencyMetrics(Boolean publishLatencyMetrics);

      abstract Builder<T> setDispatchMode(SubmissionMode submissionMode);

      abstract Builder<T> setWriterType(WriterType writerType);

      abstract Builder<T> setFormatFunction(SerializableFunction<T, Solace.Record> formatFunction);

      abstract Builder<T> setSessionServiceFactory(SessionServiceFactory factory);

      abstract Builder<T> setErrorHandler(ErrorHandler<BadRecord, ?> errorHandler);

      abstract Write<T> build();
    }

    @Override
    public SolaceOutput expand(PCollection<T> input) {
      boolean usingSolaceRecord =
          TypeDescriptor.of(Solace.Record.class)
              .isSupertypeOf(checkNotNull(input.getTypeDescriptor()));

      validateWriteTransform(usingSolaceRecord);

      boolean usingDynamicDestinations = getDestination() == null;
      SerializableFunction<Solace.Record, Destination> destinationFn;
      if (usingDynamicDestinations) {
        destinationFn = x -> SolaceIO.convertToJcsmpDestination(checkNotNull(x.getDestination()));
      } else {
        // Constant destination for all messages (same topic or queue)
        // This should not be non-null, as nulls would have been flagged by the
        // validateWriteTransform method
        destinationFn = x -> checkNotNull(getDestination());
      }

      @SuppressWarnings("unchecked")
      PCollection<Solace.Record> records =
          usingSolaceRecord
              ? (PCollection<Solace.Record>) input
              : input.apply(
                  "Format records",
                  MapElements.into(TypeDescriptor.of(Solace.Record.class))
                      .via(checkNotNull(getFormatFunction())));

      PCollection<Solace.Record> withGlobalWindow =
          records.apply("Global window", Window.into(new GlobalWindows()));

      PCollection<KV<Integer, Solace.Record>> withShardKeys =
          withGlobalWindow.apply("Add shard key", ParDo.of(new AddShardKeyDoFn(getNumShards())));

      String label =
          getWriterType() == WriterType.STREAMING ? "Publish (streaming)" : "Publish (batched)";

      PCollectionTuple solaceOutput = withShardKeys.apply(label, getWriterTransform(destinationFn));

      SolaceOutput output;
      if (getDeliveryMode() == DeliveryMode.PERSISTENT) {
        if (getErrorHandler() != null) {
          checkNotNull(getErrorHandler()).addErrorCollection(solaceOutput.get(FAILED_PUBLISH_TAG));
        }
        output = SolaceOutput.in(input.getPipeline(), solaceOutput.get(SUCCESSFUL_PUBLISH_TAG));
      } else {
        LOG.info(
            "Solace.Write: omitting writer output because delivery mode is {}", getDeliveryMode());
        output = SolaceOutput.in(input.getPipeline(), null);
      }

      return output;
    }

    private ParDo.MultiOutput<KV<Integer, Solace.Record>, Solace.PublishResult> getWriterTransform(
        SerializableFunction<Record, Destination> destinationFn) {

      ParDo.SingleOutput<KV<Integer, Solace.Record>, Solace.PublishResult> writer =
          ParDo.of(
              getWriterType() == WriterType.STREAMING
                  ? new UnboundedStreamingSolaceWriter(
                      destinationFn,
                      checkNotNull(getSessionServiceFactory()),
                      getDeliveryMode(),
                      getDispatchMode(),
                      getNumberOfClientsPerWorker(),
                      getPublishLatencyMetrics())
                  : new UnboundedBatchedSolaceWriter(
                      destinationFn,
                      checkNotNull(getSessionServiceFactory()),
                      getDeliveryMode(),
                      getDispatchMode(),
                      getNumberOfClientsPerWorker(),
                      getPublishLatencyMetrics()));

      return writer.withOutputTags(SUCCESSFUL_PUBLISH_TAG, TupleTagList.of(FAILED_PUBLISH_TAG));
    }

    /**
     * Called before running the Pipeline to verify this transform is fully and correctly specified.
     */
    private void validateWriteTransform(boolean usingSolaceRecords) {
      if (!usingSolaceRecords) {
        checkNotNull(
            getFormatFunction(),
            "SolaceIO.Write: If you are not using Solace.Record as the input type, you"
                + " must set a format function using withFormatFunction().");
      }

      checkArgument(
          getNumShards() > 0, "SolaceIO.Write: The number of used workers must be positive.");
      checkArgument(
          getNumberOfClientsPerWorker() > 0,
          "SolaceIO.Write: The number of clients per worker must be positive.");
      checkArgument(
          getDeliveryMode() == DeliveryMode.DIRECT || getDeliveryMode() == DeliveryMode.PERSISTENT,
          String.format(
              "SolaceIO.Write: Delivery mode must be either DIRECT or PERSISTENT. %s"
                  + " not supported",
              getDeliveryMode()));
      if (getPublishLatencyMetrics()) {
        checkArgument(
            getDeliveryMode() == DeliveryMode.PERSISTENT,
            "SolaceIO.Write: Publish latency metrics can only be enabled for PERSISTENT"
                + " delivery mode.");
      }
      checkNotNull(
          getSessionServiceFactory(),
          "SolaceIO: You need to pass a session service factory. For basic"
              + " authentication, you can use BasicAuthJcsmpSessionServiceFactory.");
    }
  }
}
