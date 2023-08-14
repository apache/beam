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

  public static Builder builder() {
    return new AutoValue_PubsubWriteSchemaTransformConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFormat(String format);

    public abstract Builder setTopic(String topic);

    public abstract PubsubWriteSchemaTransformConfiguration build();
  }
}
