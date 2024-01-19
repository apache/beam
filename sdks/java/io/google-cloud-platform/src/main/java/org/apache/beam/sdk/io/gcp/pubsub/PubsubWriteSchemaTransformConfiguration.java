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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;

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
  @SchemaFieldDescription(
      "The encoding format for the data stored in Pubsub. Valid options are: "
          + PubsubWriteSchemaTransformProvider.VALID_FORMATS_STR)
  public abstract String getFormat();

  @SchemaFieldDescription(
      "The name of the topic to write data to. " + "Format: projects/${PROJECT}/topics/${TOPIC}")
  public abstract String getTopic();

  @SchemaFieldDescription(
      "The set of fields to write as PubSub attributes instead of part of the payload.")
  public abstract @Nullable List<String> getAttributes();

  @SchemaFieldDescription(
      "A map field to write as PubSub attributes instead of part of the payload.")
  public abstract @Nullable String getAttributesMap();

  @SchemaFieldDescription(
      "If set, will set an attribute for each Cloud Pub/Sub message with the given name and a unique value. "
          + "This attribute can then be used in a ReadFromPubSub PTransform to deduplicate messages.")
  public abstract @Nullable String getIdAttribute();

  @SchemaFieldDescription(
      "If set, will set an attribute for each Cloud Pub/Sub message with the given name and the message's "
          + "publish time as the value.")
  public abstract @Nullable String getTimestampAttribute();

  @SchemaFieldDescription("Specifies how to handle errors.")
  public abstract @Nullable ErrorHandling getErrorHandling();

  @AutoValue
  public abstract static class ErrorHandling {
    @SchemaFieldDescription("The name of the output PCollection containing failed writes.")
    public abstract String getOutput();

    public static PubsubWriteSchemaTransformConfiguration.ErrorHandling.Builder builder() {
      return new AutoValue_PubsubWriteSchemaTransformConfiguration_ErrorHandling.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract PubsubWriteSchemaTransformConfiguration.ErrorHandling.Builder setOutput(
          String output);

      public abstract PubsubWriteSchemaTransformConfiguration.ErrorHandling build();
    }
  }

  public static Builder builder() {
    return new AutoValue_PubsubWriteSchemaTransformConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFormat(String format);

    public abstract Builder setTopic(String topic);

    public abstract Builder setAttributes(@Nullable List<String> attributes);

    public abstract Builder setAttributesMap(@Nullable String attributesMap);

    public abstract Builder setIdAttribute(@Nullable String idAttribute);

    public abstract Builder setTimestampAttribute(@Nullable String timestampAttribute);

    public abstract Builder setErrorHandling(@Nullable ErrorHandling errorHandling);

    public abstract PubsubWriteSchemaTransformConfiguration build();
  }
}
