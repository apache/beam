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
package org.apache.beam.sdk.io.iceberg;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;

/**
 * As much as one manifest file can hold: entries for a single table, all written against a single
 * partition spec. Data files are batched by this before a manifest is written for each batch.
 *
 * <p>A schema type rather than a {@code KV} of the two. The field numbers pin each field's place in
 * the encoding, so a third field can be added after these two without moving them.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class ManifestKey {

  static ManifestKey of(String table, int partitionSpecId) {
    return new AutoValue_ManifestKey(table, partitionSpecId);
  }

  @SchemaFieldNumber("0")
  abstract String getTable();

  @SchemaFieldNumber("1")
  abstract int getPartitionSpecId();
}
