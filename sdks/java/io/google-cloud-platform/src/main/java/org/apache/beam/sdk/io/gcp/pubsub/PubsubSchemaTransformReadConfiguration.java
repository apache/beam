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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Configuration for reading from Pub/Sub.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@Experimental
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PubsubSchemaTransformReadConfiguration {

  /** Instantiates a {@link PubsubSchemaTransformReadConfiguration.Builder}. */
  public static Builder builder() {
    return new AutoValue_PubsubSchemaTransformReadConfiguration.Builder();
  }

  /** The expected schema of the Pub/Sub message. */
  public abstract Schema getDataSchema();

  /**
   * The Pub/Sub topic path to write failures.
   *
   * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the dead
   * letter queue topic string.
   */
  @Nullable
  public abstract String getDeadLetterQueue();

  /**
   * The expected format of the Pub/Sub message.
   *
   * <p>Used to retrieve the {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer} from
   * {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers}.
   */
  @Nullable
  public abstract String getFormat();

  /** Used by the ProtoPayloadSerializerProvider when serializing from a Pub/Sub message. */
  @Nullable
  public abstract String getProtoClass();

  /**
   * The subscription from which to read Pub/Sub messages.
   *
   * <p>See {@link PubsubIO.PubsubSubscription#fromPath(String)} for more details on the format of
   * the subscription string.
   */
  @Nullable
  public abstract String getSubscription();

  /** Used by the ThriftPayloadSerializerProvider when serializing from a Pub/Sub message. */
  @Nullable
  public abstract String getThriftClass();

  /** Used by the ThriftPayloadSerializerProvider when serializing from a Pub/Sub message. */
  @Nullable
  public abstract String getThriftProtocolFactoryClass();

  /**
   * When reading from Cloud Pub/Sub where record timestamps are provided as Pub/Sub message
   * attributes, specifies the name of the attribute that contains the timestamp.
   */
  @Nullable
  public abstract String getTimestampAttribute();

  /**
   * When reading from Cloud Pub/Sub where unique record identifiers are provided as Pub/Sub message
   * attributes, specifies the name of the attribute containing the unique identifier.
   */
  @Nullable
  public abstract String getIdAttribute();

  /**
   * The topic from which to read Pub/Sub messages.
   *
   * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
   * topic string.
   */
  @Nullable
  public abstract String getTopic();

  @AutoValue.Builder
  public abstract static class Builder {

    /** The expected schema of the Pub/Sub message. */
    public abstract Builder setDataSchema(Schema value);

    /**
     * The Pub/Sub topic path to write failures.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * dead letter queue topic string.
     */
    public abstract Builder setDeadLetterQueue(String value);

    /**
     * The expected format of the Pub/Sub message.
     *
     * <p>Used to retrieve the {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer}
     * from {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers}.
     */
    public abstract Builder setFormat(String value);

    /** Used by the ProtoPayloadSerializerProvider when serializing from a Pub/Sub message. */
    public abstract Builder setProtoClass(String value);

    /**
     * The subscription from which to read Pub/Sub messages.
     *
     * <p>See {@link PubsubIO.PubsubSubscription#fromPath(String)} for more details on the format of
     * the subscription string.
     */
    public abstract Builder setSubscription(String value);

    /** Used by the ThriftPayloadSerializerProvider when serializing from a Pub/Sub message. */
    public abstract Builder setThriftClass(String value);

    /** Used by the ThriftPayloadSerializerProvider when serializing from a Pub/Sub message. */
    public abstract Builder setThriftProtocolFactoryClass(String value);

    /**
     * When reading from Cloud Pub/Sub where record timestamps are provided as Pub/Sub message
     * attributes, specifies the name of the attribute that contains the timestamp.
     */
    public abstract Builder setTimestampAttribute(String value);

    /**
     * When reading from Cloud Pub/Sub where unique record identifiers are provided as Pub/Sub
     * message attributes, specifies the name of the attribute containing the unique identifier.
     */
    public abstract Builder setIdAttribute(String value);

    /**
     * The topic from which to read Pub/Sub messages.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * topic string.
     */
    public abstract Builder setTopic(String value);

    /** Builds a {@link PubsubSchemaTransformReadConfiguration} instance. */
    public abstract PubsubSchemaTransformReadConfiguration build();
  }
}
