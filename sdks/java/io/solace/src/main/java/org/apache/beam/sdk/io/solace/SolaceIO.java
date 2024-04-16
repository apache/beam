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

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import java.io.IOException;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.solace.broker.SempClientFactory;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.SolaceRecordMapper;
import org.apache.beam.sdk.io.solace.data.SolaceRecordCoder;
import org.apache.beam.sdk.io.solace.read.UnboundedSolaceSource;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} to read and write from/to Solace.
 *
 * <h3>Authentication</h3>
 *
 * TODO: Auth for the read connector
 *
 * <h3>Reading</h3>
 *
 * TODO
 */
public class SolaceIO {

  public static final Logger LOG = LoggerFactory.getLogger(SolaceIO.class);
  public static final SerializableFunction<Solace.Record, Instant> SENDER_TIMESTAMP_FUNCTION =
      (record) -> new Instant(record.getSenderTimestamp());
  private static final boolean DEFAULT_DEDUPLICATE_RECORDS = true;

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
      return topicFromName(destination.getName());
    } else if (destination.getType().equals(Solace.DestinationType.QUEUE)) {
      return queueFromName(destination.getName());
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
   * Create a {@link Read} transform, to read from Solace. Specify a {@link SerializableFunction}
   * to map incoming {@link BytesXMLMessage} records, to the object of your choice. You also need
   * to specify a {@link TypeDescriptor} for your class and the timestamp function which returns
   * an {@link Instant} from the record.
   *
   * <p> The type descriptor will be used to infer a coder from CoderRegistry or Schema Registry.
   * You can initialize a new TypeDescriptor in the following manner:
   *
   * <pre>{@code
   *   TypeDescriptor<T> typeDescriptor = TypeDescriptor.of(YourOutputType.class);
   * }
   */
  public static <T> Read<T> read(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<BytesXMLMessage, T> parseFn,
      SerializableFunction<T, Instant> timestampFn) {
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
     * Set the timestamp function. This serializable has to output an {@link Instant}. This will be
     * used to calculate watermark and define record's timestamp.
     */
    public Read<T> withTimestampFn(SerializableFunction<T, Instant> timestampFn) {
      return toBuilder().setTimestampFn(timestampFn).build();
    }

    /**
     * Maximum number of read connections created to Solace cluster. This is optional, leave out to
     * let the Runner decide.
     */
    public Read<T> withMaxNumConnections(Integer maxNumConnections) {
      return toBuilder().setMaxNumConnections(maxNumConnections).build();
    }

    /**
     * Set if the read records should be deduplicated. True by default. It will use the
     * `applicationMessageId` attribute to identify duplicates.
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
     */
    public Read<T> withSempClientFactory(SempClientFactory sempClientFactory) {
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
     */
    public Read<T> withSessionServiceFactory(SessionServiceFactory sessionServiceFactory) {
      return toBuilder().setSessionServiceFactory(sessionServiceFactory).build();
    }

    abstract @Nullable Queue getQueue();

    abstract @Nullable Topic getTopic();

    abstract @Nullable SerializableFunction<T, Instant> getTimestampFn();

    abstract @Nullable Integer getMaxNumConnections();

    abstract boolean getDeduplicateRecords();

    abstract SerializableFunction<BytesXMLMessage, T> getParseFn();

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

      abstract Builder<T> setParseFn(SerializableFunction<BytesXMLMessage, T> parseFn);

      abstract Builder<T> setSempClientFactory(SempClientFactory brokerServiceFactory);

      abstract Builder<T> setSessionServiceFactory(SessionServiceFactory sessionServiceFactory);

      abstract Builder<T> setTypeDescriptor(TypeDescriptor<T> typeDescriptor);

      abstract Read<T> build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      validate();

      SempClientFactory sempClientFactory = getSempClientFactory();
      String jobName = input.getPipeline().getOptions().getJobName();
      Queue queue =
          getQueue() != null ? getQueue() : initializeQueueForTopic(jobName, sempClientFactory);

      SessionServiceFactory sessionServiceFactory = getSessionServiceFactory();
      sessionServiceFactory.setQueue(queue);

      registerDefaultCoder(input.getPipeline());
      // Infer the actual coder
      Coder<T> coder = inferCoder(input.getPipeline());

      return input.apply(
          org.apache.beam.sdk.io.Read.from(
              new UnboundedSolaceSource<>(
                  queue,
                  sempClientFactory,
                  sessionServiceFactory,
                  getMaxNumConnections(),
                  getDeduplicateRecords(),
                  coder,
                  getTimestampFn(),
                  getParseFn())));
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
          "SolaceIO.Read: Cannot infer a coder for the TypeDescriptor. Annotate you"
              + " output class with @DefaultSchema annotation or create a coder manually"
              + " and register it in the CoderRegistry.");
    }

    private Coder<T> getFromSchemaRegistry(Pipeline pipeline) {
      try {
        return pipeline.getSchemaRegistry().getSchemaCoder(getTypeDescriptor());
      } catch (NoSuchSchemaException e) {
        return null;
      }
    }

    private Coder<T> getFromCoderRegistry(Pipeline pipeline) {
      try {
        return pipeline.getCoderRegistry().getCoder(getTypeDescriptor());
      } catch (CannotProvideCoderException e) {
        return null;
      }
    }

    // FIXME: this is public only for the sake of testing, TODO: redesign test so this is
    // private
    public Queue initializeQueueForTopic(String jobName, SempClientFactory sempClientFactory) {
      Queue q;
      if (getQueue() != null) {
        q = getQueue();
      } else {
        String queueName = String.format("queue-%s-%s", getTopic(), jobName);
        try {
          String topicName = Objects.requireNonNull(getTopic()).getName();
          q = sempClientFactory.create().createQueueForTopic(queueName, topicName);
          LOG.info(
              "SolaceIO.Read: A new queue {} was created. The Queue will not be"
                  + " deleted when this job finishes. Make sure to remove it yourself"
                  + " when not needed.",
              q.getName());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return q;
    }

    private void validate() {
      Preconditions.checkState(
          getSempClientFactory() != null, "SolaceIO.Read: brokerServiceFactory must not be null.");
      Preconditions.checkState(
          getSessionServiceFactory() != null,
          "SolaceIO.Read: SessionServiceFactory must not be null.");
      Preconditions.checkState(
          getParseFn() != null,
          "SolaceIO.Read: parseFn must be set or use the `Read.readSolaceRecords()`" + " method");
      Preconditions.checkState(
          getTimestampFn() != null,
          "SolaceIO.Read: timestamp function must be set or use the"
              + " `Read.readSolaceRecords()` method");
      Preconditions.checkState(
          (getQueue() == null ^ getTopic() == null),
          "SolaceIO.Read: One of the Solace {Queue, Topic} must be set.");
    }
  }
}
