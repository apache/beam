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

import com.google.api.client.util.Clock;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;

/**
 * Configuration for reading from Pub/Sub.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PubsubReadSchemaTransformConfiguration {
  @SchemaFieldDescription(
      "The name of the topic to consume data from. If a topic is specified, "
          + " will create a new subscription for that topic and start consuming from that point. "
          + "Either a topic or a subscription must be provided. "
          + "Format: projects/${PROJECT}/topics/${TOPIC}")
  public abstract @Nullable String getTopic();

  @SchemaFieldDescription(
      "The name of the subscription to consume data. "
          + "Either a topic or subscription must be provided. "
          + "Format: projects/${PROJECT}/subscriptions/${SUBSCRIPTION}")
  public abstract @Nullable String getSubscription();

  @SchemaFieldDescription(
      "The encoding format for the data stored in Pubsub. Valid options are: "
          + PubsubReadSchemaTransformProvider.VALID_FORMATS_STR)
  public abstract String getFormat(); // AVRO, JSON

  @SchemaFieldDescription(
      "The schema in which the data is encoded in the Pubsub topic. "
          + "For AVRO data, this is a schema defined with AVRO schema syntax "
          + "(https://avro.apache.org/docs/1.10.2/spec.html#schemas). "
          + "For JSON data, this is a schema defined with JSON-schema syntax (https://json-schema.org/).")
  public abstract String getSchema();

  // Used for testing only.
  public abstract @Nullable PubsubTestClientFactory getClientFactory();

  // Used for testing only.
  public abstract @Nullable Clock getClock();

  public static Builder builder() {
    return new AutoValue_PubsubReadSchemaTransformConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTopic(@Nullable String topic);

    public abstract Builder setSubscription(@Nullable String subscription);

    public abstract Builder setFormat(String format);

    public abstract Builder setSchema(String schema);

    // Used for testing only.
    public abstract Builder setClientFactory(@Nullable PubsubTestClientFactory clientFactory);

    // Used for testing only.
    public abstract Builder setClock(@Nullable Clock clock);

    public abstract PubsubReadSchemaTransformConfiguration build();
  }
}
