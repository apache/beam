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

/** Configuration class for the MongoDB Read transform. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class MongoDbReadSchemaTransformConfiguration implements Serializable {

  @SchemaFieldDescription("The connection URI for the MongoDB server.")
  public abstract String getUri();

  @SchemaFieldDescription("The MongoDB database to read from.")
  public abstract String getDatabase();

  @SchemaFieldDescription("The MongoDB collection to read from.")
  public abstract String getCollection();

  @SchemaFieldDescription(
      "The schema in which the data is encoded, defined with JSON-schema syntax (https://json-schema.org/).")
  public abstract String getSchema();

  @SchemaFieldDescription(
      "An optional BSON filter to apply to the read. This should be a valid JSON string.")
  @Nullable
  public abstract String getFilter();

  @SchemaFieldDescription(
      "This option specifies whether and where to output rows that failed to be read.")
  @Nullable
  public abstract ErrorHandling getErrorHandling();

  public void validate() {
    checkArgument(getUri() != null && !getUri().isEmpty(), "MongoDB URI must be specified.");
    checkArgument(
        getDatabase() != null && !getDatabase().isEmpty(), "MongoDB database must be specified.");
    checkArgument(
        getCollection() != null && !getCollection().isEmpty(),
        "MongoDB collection must be specified.");
    checkArgument(
        getSchema() != null && !getSchema().isEmpty(), "MongoDB schema must be specified.");
  }

  public static Builder builder() {
    return new AutoValue_MongoDbReadSchemaTransformConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUri(String uri);

    public abstract Builder setDatabase(String database);

    public abstract Builder setCollection(String collection);

    public abstract Builder setSchema(String schema);

    public abstract Builder setFilter(String filter);

    public abstract Builder setErrorHandling(ErrorHandling errorHandling);

    public abstract MongoDbReadSchemaTransformConfiguration build();
  }
}
