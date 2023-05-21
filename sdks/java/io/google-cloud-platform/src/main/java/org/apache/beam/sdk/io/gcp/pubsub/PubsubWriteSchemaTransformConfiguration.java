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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Configuration for writing to Pub/Sub.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PubsubWriteSchemaTransformConfiguration {

  public static final String DEFAULT_TIMESTAMP_ATTRIBUTE = "event_timestamp";

  public static Builder builder() {
    return new AutoValue_PubsubWriteSchemaTransformConfiguration.Builder();
  }

  public static TargetConfiguration.Builder targetConfigurationBuilder() {
    return new AutoValue_PubsubWriteSchemaTransformConfiguration_TargetConfiguration.Builder()
        .setTimestampAttributeKey(DEFAULT_TIMESTAMP_ATTRIBUTE);
  }

  public static SourceConfiguration.Builder sourceConfigurationBuilder() {
    return new AutoValue_PubsubWriteSchemaTransformConfiguration_SourceConfiguration.Builder();
  }

  /**
   * Configuration details of the source {@link org.apache.beam.sdk.values.Row} {@link
   * org.apache.beam.sdk.schemas.Schema}.
   */
  @Nullable
  public abstract SourceConfiguration getSource();

  /** Configuration details of the target {@link PubsubMessage}. */
  public abstract TargetConfiguration getTarget();

  /**
   * The topic to which to write Pub/Sub messages.
   *
   * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
   * topic string.
   */
  public abstract String getTopic();

  /**
   * The expected format of the Pub/Sub message.
   *
   * <p>Used to retrieve the {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer} from
   * {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers}. See list of supported
   * values by invoking {@link org.apache.beam.sdk.schemas.io.Providers#loadProviders(Class)}.
   *
   * <pre>{@code Providers.loadProviders(PayloadSerializer.class).keySet()}</pre>
   */
  @Nullable
  public abstract String getFormat();

  /**
   * When writing to Cloud Pub/Sub where unique record identifiers are provided as Pub/Sub message
   * attributes, specifies the name of the attribute containing the unique identifier.
   */
  @Nullable
  public abstract String getIdAttribute();

  /** Builder for {@link PubsubWriteSchemaTransformConfiguration}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Configuration details of the source {@link org.apache.beam.sdk.values.Row} {@link
     * org.apache.beam.sdk.schemas.Schema}.
     */
    public abstract Builder setSource(SourceConfiguration value);

    /** Configuration details of the target {@link PubsubMessage}. */
    public abstract Builder setTarget(TargetConfiguration value);

    /**
     * The topic to which to write Pub/Sub messages.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * topic string.
     */
    public abstract Builder setTopic(String value);

    /**
     * The expected format of the Pub/Sub message.
     *
     * <p>Used to retrieve the {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer}
     * from {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers}. See list of
     * supported values by invoking {@link
     * org.apache.beam.sdk.schemas.io.Providers#loadProviders(Class)}.
     *
     * <pre>{@code Providers.loadProviders(PayloadSerializer.class).keySet()}</pre>
     */
    public abstract Builder setFormat(String value);

    /**
     * When reading from Cloud Pub/Sub where unique record identifiers are provided as Pub/Sub
     * message attributes, specifies the name of the attribute containing the unique identifier.
     */
    public abstract Builder setIdAttribute(String value);

    public abstract PubsubWriteSchemaTransformConfiguration build();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SourceConfiguration {
    /**
     * The attributes field name of the source {@link org.apache.beam.sdk.values.Row}. {@link
     * org.apache.beam.sdk.schemas.Schema.FieldType} must be a <code>Map&lt;String, String&gt;
     * </code>
     */
    @Nullable
    public abstract String getAttributesFieldName();

    /**
     * The timestamp field name of the source {@link org.apache.beam.sdk.values.Row}. {@link
     * org.apache.beam.sdk.schemas.Schema.FieldType} must be a {@link
     * org.apache.beam.sdk.schemas.Schema.FieldType#DATETIME}.
     */
    @Nullable
    public abstract String getTimestampFieldName();

    /**
     * The payload field name of the source {@link org.apache.beam.sdk.values.Row}. {@link
     * org.apache.beam.sdk.schemas.Schema.FieldType} must be either {@link
     * org.apache.beam.sdk.schemas.Schema.FieldType#BYTES} or a {@link
     * org.apache.beam.sdk.values.Row}. If null, payload serialized from user fields other than
     * attributes. Not compatible with other payload intended fields.
     */
    @Nullable
    public abstract String getPayloadFieldName();

    @AutoValue.Builder
    public abstract static class Builder {
      /**
       * The attributes field name of the source {@link org.apache.beam.sdk.values.Row}. {@link
       * org.apache.beam.sdk.schemas.Schema.FieldType} must be a <code>Map&lt;String, String&gt;
       * </code>
       */
      public abstract Builder setAttributesFieldName(String value);

      /**
       * The timestamp field name of the source {@link org.apache.beam.sdk.values.Row}. {@link
       * org.apache.beam.sdk.schemas.Schema.FieldType} must be a {@link
       * org.apache.beam.sdk.schemas.Schema.FieldType#DATETIME}.
       */
      public abstract Builder setTimestampFieldName(String value);

      /**
       * The payload field name of the source {@link org.apache.beam.sdk.values.Row}. {@link
       * org.apache.beam.sdk.schemas.Schema.FieldType} must be either {@link
       * org.apache.beam.sdk.schemas.Schema.FieldType#BYTES} or a {@link
       * org.apache.beam.sdk.values.Row}. If null, payload serialized from user fields other than
       * attributes. Not compatible with other payload intended fields.
       */
      public abstract Builder setPayloadFieldName(String value);

      public abstract SourceConfiguration build();
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class TargetConfiguration {

    /**
     * The attribute key to assign the {@link PubsubMessage} stringified timestamp value. {@link
     * #builder()} method defaults value to {@link #DEFAULT_TIMESTAMP_ATTRIBUTE}.
     */
    public abstract String getTimestampAttributeKey();

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * The attribute key to assign the {@link PubsubMessage} stringified timestamp value. Defaults
       * to {@link #DEFAULT_TIMESTAMP_ATTRIBUTE}.
       */
      public abstract Builder setTimestampAttributeKey(String value);

      public abstract TargetConfiguration build();
    }
  }
}
