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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/** Represents a BigQuery source description used for dynamic read. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQueryDynamicReadDescriptor implements Serializable {
  @SchemaFieldName("query")
  @Pure
  abstract @Nullable String getQuery();

  @SchemaFieldName("table")
  @Pure
  abstract @Nullable String getTable();

  @SchemaCreate
  @SuppressWarnings("all")
  public static BigQueryDynamicReadDescriptor create(
      @Nullable String query, @Nullable String table) {
    return new AutoValue_BigQueryDynamicReadDescriptor(query, table);
  }
}
