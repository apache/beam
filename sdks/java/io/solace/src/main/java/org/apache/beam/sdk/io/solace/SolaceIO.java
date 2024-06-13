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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.solace.broker.SempClientFactory;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.SolaceRecordMapper;
import org.apache.beam.sdk.io.solace.data.SolaceRecordCoder;
import org.apache.beam.sdk.io.solace.read.UnboundedSolaceSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * @DefaultSchema(JavaBeanSchema.class)
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
    return Read.<Solace.Record>builder()
        .setTypeDescriptor(TypeDescriptor.of(Solace.Record.class))
        .setParseFn(SolaceRecordMapper::map)
        .setTimestampFn(SENDER_TIMESTAMP_FUNCTION)
        .build();
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
    return Read.<T>builder()
        .setTypeDescriptor(typeDescriptor)
        .setParseFn(parseFn)
        .setTimestampFn(timestampFn)
        .build();
  }

  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    /** Set the queue name to read from. Use this or the `from(Topic)` method. */
    public Read<T> from(Solace.Queue queue) {
      return toBuilder().setQueue(queueFromName(queue.getName())).build();
    }

    /** Set the topic name to read from. Use this or the `from(Queue)` method. */
    public Read<T> from(Solace.Topic topic) {
      return toBuilder().setTopic(topicFromName(topic.getName())).build();
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
          "SolaceIO.Read: timestamp function must be set or use the"
              + " `Read.readSolaceRecords()` method");
      return toBuilder().setTimestampFn(timestampFn).build();
    }

    /**
     * Optional. Sets the maximum number of connections to the broker. The actual number of sessions
     * is determined by this and the number set by the runner. If not set, the number of sessions is
     * determined by the runner. The number of connections created follows this logic:
     * `numberOfConnections = min(maxNumConnections, desiredNumberOfSplits)`, where the
     * `desiredNumberOfSplits` is set by the runner.
     */
    public Read<T> withMaxNumConnections(Integer maxNumConnections) {
      return toBuilder().setMaxNumConnections(maxNumConnections).build();
    }

    /**
     * Optional, default: false. Set to deduplicate messages based on the {@link
     * BytesXMLMessage#getApplicationMessageId()} of the incoming {@link BytesXMLMessage}. If the
     * field is null, then the {@link BytesXMLMessage#getReplicationGroupMessageId()} will be used,
     * which is always set by Solace.
     */
    public Read<T> withDeduplicateRecords(boolean deduplicateRecords) {
      return toBuilder().setDeduplicateRecords(deduplicateRecords).build();
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
      return toBuilder().setSempClientFactory(sempClientFactory).build();
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
     * <p>The host is the IPv4 or IPv6 or host name of the appliance. IPv5 addresses must be encoded
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
      return toBuilder().setSessionServiceFactory(sessionServiceFactory).build();
    }

    abstract @Nullable Queue getQueue();

    abstract @Nullable Topic getTopic();

    abstract @Nullable SerializableFunction<T, Instant> getTimestampFn();

    abstract @Nullable Integer getMaxNumConnections();

    abstract boolean getDeduplicateRecords();

    abstract SerializableFunction<@Nullable BytesXMLMessage, @Nullable T> getParseFn();

    abstract @Nullable SempClientFactory getSempClientFactory();

    abstract @Nullable SessionServiceFactory getSessionServiceFactory();

    abstract TypeDescriptor<T> getTypeDescriptor();

    public static <T> Builder<T> builder() {
      Builder<T> builder = new org.apache.beam.sdk.io.solace.AutoValue_SolaceIO_Read.Builder<T>();
      builder.setDeduplicateRecords(DEFAULT_DEDUPLICATE_RECORDS);
      return builder;
    }

    abstract Builder<T> toBuilder();

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

      abstract Read<T> build();
    }

    @Override
    public void validate(@Nullable PipelineOptions options) {
      checkState(
          (getQueue() == null ^ getTopic() == null),
          "SolaceIO.Read: One of the Solace {Queue, Topic} must be set.");
      checkNotNull(getSempClientFactory(), "SolaceIO: sempClientFactory is null.");
      checkNotNull(getSessionServiceFactory(), "SolaceIO: sessionServiceFactory is null.");
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      SempClientFactory sempClientFactory = checkNotNull(getSempClientFactory());
      String jobName = input.getPipeline().getOptions().getJobName();
      Queue queueFromOptions = getQueue();
      Queue initializedQueue =
          queueFromOptions != null
              ? queueFromOptions
              : initializeQueueForTopic(jobName, sempClientFactory);

      SessionServiceFactory sessionServiceFactory = checkNotNull(getSessionServiceFactory());
      sessionServiceFactory.setQueue(initializedQueue);

      registerDefaultCoder(input.getPipeline());
      // Infer the actual coder
      Coder<T> coder = inferCoder(input.getPipeline());

      return input.apply(
          org.apache.beam.sdk.io.Read.from(
              new UnboundedSolaceSource<>(
                  initializedQueue,
                  sempClientFactory,
                  sessionServiceFactory,
                  getMaxNumConnections(),
                  getDeduplicateRecords(),
                  coder,
                  checkNotNull(getTimestampFn()),
                  checkNotNull(getParseFn()))));
    }

    private static void registerDefaultCoder(Pipeline pipeline) {
      pipeline
          .getCoderRegistry()
          .registerCoderForType(TypeDescriptor.of(Solace.Record.class), SolaceRecordCoder.of());
    }

    @VisibleForTesting
    Coder<T> inferCoder(Pipeline pipeline) {
      Coder<T> coderFromCoderRegistry = getFromCoderRegistry(pipeline);
      if (coderFromCoderRegistry != null) {
        return coderFromCoderRegistry;
      }

      Coder<T> coderFromSchemaRegistry = getFromSchemaRegistry(pipeline);
      if (coderFromSchemaRegistry != null) {
        return coderFromSchemaRegistry;
      }

      throw new RuntimeException(
          "SolaceIO.Read: Cannot infer a coder for the TypeDescriptor. Annotate your"
              + " output class with @DefaultSchema annotation or create a coder manually"
              + " and register it in the CoderRegistry.");
    }

    private @Nullable Coder<T> getFromSchemaRegistry(Pipeline pipeline) {
      try {
        return pipeline.getSchemaRegistry().getSchemaCoder(getTypeDescriptor());
      } catch (NoSuchSchemaException e) {
        return null;
      }
    }

    private @Nullable Coder<T> getFromCoderRegistry(Pipeline pipeline) {
      try {
        return pipeline.getCoderRegistry().getCoder(getTypeDescriptor());
      } catch (CannotProvideCoderException e) {
        return null;
      }
    }

    Queue initializeQueueForTopic(String jobName, SempClientFactory sempClientFactory) {
      Queue initializedQueue;
      Queue solaceQueue = getQueue();
      if (solaceQueue != null) {
        return solaceQueue;
      } else {
        String queueName = String.format("queue-%s-%s", getTopic(), jobName);
        try {
          String topicName = checkNotNull(getTopic()).getName();
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
}
