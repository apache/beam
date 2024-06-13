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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import org.apache.beam.sdk.io.solace.broker.SempClientFactory;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class SolaceIO {

  private static final boolean DEFAULT_DEDUPLICATE_RECORDS = false;

  /** Get a {@link Topic} object from the topic name. */
  static Topic topicFromName(String topicName) {
    return JCSMPFactory.onlyInstance().createTopic(topicName);
  }

  /** Get a {@link Queue} object from the queue name. */
  static Queue queueFromName(String queueName) {
    return JCSMPFactory.onlyInstance().createQueue(queueName);
  }

  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

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
    public PCollection<T> expand(PBegin input) {
      throw new UnsupportedOperationException("");
    }
  }
}
