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
package org.apache.beam.sdk.io.mongodb;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration class for the MongoDB Write transform. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class MongoDbWriteSchemaTransformConfiguration implements Serializable {

  @SchemaFieldDescription("The connection URI for the MongoDB server.")
  public abstract String getUri();

  @SchemaFieldDescription("The MongoDB database to write to.")
  public abstract String getDatabase();

  @SchemaFieldDescription("The MongoDB collection to write to.")
  public abstract String getCollection();

  @SchemaFieldDescription("The number of documents to include in each batch write.")
  @Nullable
  public abstract Long getBatchSize();

  @SchemaFieldDescription("Whether the writes should be performed in an ordered manner.")
  @Nullable
  public abstract Boolean getOrdered();

  @SchemaFieldDescription("Maximum connection idle time in milliseconds.")
  @Nullable
  public abstract Integer getMaxConnectionIdleTime();

  @SchemaFieldDescription("Enable SSL for the connection.")
  @Nullable
  public abstract Boolean getSslEnabled();

  @SchemaFieldDescription("Allow invalid hostnames for SSL connections.")
  @Nullable
  public abstract Boolean getSslInvalidHostNameAllowed();

  @SchemaFieldDescription("Ignore SSL certificate validation.")
  @Nullable
  public abstract Boolean getIgnoreSSLCertificate();

  @SchemaFieldDescription("Configuration for Update and Upsert operations.")
  @Nullable
  public abstract MongoDbUpdateConfiguration getUpdateConfiguration();

  @SchemaFieldDescription(
      "This option specifies whether and where to output unwritable rows. Note: Error handling is currently limited to data conversion failures before sending to the MongoDB driver, as the underlying MongoDbIO does not yet support dead-letter queues for write failures.")
  @Nullable
  public abstract ErrorHandling getErrorHandling();

  public void validate() {
    checkArgument(getUri() != null && !getUri().isEmpty(), "MongoDB URI must be specified.");
    checkArgument(
        getDatabase() != null && !getDatabase().isEmpty(), "MongoDB database must be specified.");
    checkArgument(
        getCollection() != null && !getCollection().isEmpty(),
        "MongoDB collection must be specified.");

    Long batchSize = getBatchSize();
    if (batchSize != null) {
      checkArgument(batchSize > 0, "Batch size must be positive.");
    }
  }

  public static Builder builder() {
    return new AutoValue_MongoDbWriteSchemaTransformConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUri(String uri);

    public abstract Builder setDatabase(String database);

    public abstract Builder setCollection(String collection);

    public abstract Builder setBatchSize(Long batchSize);

    public abstract Builder setOrdered(Boolean ordered);

    public abstract Builder setMaxConnectionIdleTime(Integer maxConnectionIdleTime);

    public abstract Builder setSslEnabled(Boolean sslEnabled);

    public abstract Builder setSslInvalidHostNameAllowed(Boolean sslInvalidHostNameAllowed);

    public abstract Builder setIgnoreSSLCertificate(Boolean ignoreSSLCertificate);

    public abstract Builder setUpdateConfiguration(MongoDbUpdateConfiguration updateConfiguration);

    public abstract Builder setErrorHandling(ErrorHandling errorHandling);

    public abstract MongoDbWriteSchemaTransformConfiguration build();
  }
}
