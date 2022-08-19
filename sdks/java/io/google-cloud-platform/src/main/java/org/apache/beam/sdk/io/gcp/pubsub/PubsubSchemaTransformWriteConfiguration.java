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
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Configuration for writing to Pub/Sub.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@Experimental
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PubsubSchemaTransformWriteConfiguration {

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
   * {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers}.
   */
  @Nullable
  public abstract String getFormat();

  /**
   * When writing to Cloud Pub/Sub where record timestamps are configured as Pub/Sub message
   * attributes, specifies the name of the attribute that contains the timestamp.
   */
  @Nullable
  public abstract String getTimestampAttribute();

  /**
   * When writing to Cloud Pub/Sub where unique record identifiers are provided as Pub/Sub message
   * attributes, specifies the name of the attribute containing the unique identifier.
   */
  @Nullable
  public abstract String getIdAttribute();

  /** Builder for {@link PubsubSchemaTransformWriteConfiguration}. */
  @AutoValue.Builder
  public abstract static class Builder {

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
     * from {@link org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers}.
     */
    public abstract Builder setFormat(String value);

    /**
     * When writing to Cloud Pub/Sub where record timestamps are configured as Pub/Sub message
     * attributes, specifies the name of the attribute that contains the timestamp.
     */
    public abstract Builder setTimestampAttribute(String value);

    /**
     * When reading from Cloud Pub/Sub where unique record identifiers are provided as Pub/Sub
     * message attributes, specifies the name of the attribute containing the unique identifier.
     */
    public abstract Builder setIdAttribute(String value);

    /** Builds a {@link PubsubSchemaTransformWriteConfiguration} instance. */
    public abstract PubsubSchemaTransformWriteConfiguration build();
  }
}
