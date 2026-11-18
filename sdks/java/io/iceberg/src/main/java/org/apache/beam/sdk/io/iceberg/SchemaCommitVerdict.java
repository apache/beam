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
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * What the schema pre-pass decided for one table: a schema id when it committed, or a reason when
 * it refused. Exactly one of the two is set.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class SchemaCommitVerdict {

  /** The table's schema id after the commit; null when refused. */
  @SchemaFieldNumber("0")
  abstract @Nullable Long getSchemaId();

  /** Why the table was refused; null when committed. */
  @SchemaFieldNumber("1")
  abstract @Nullable String getRefusal();

  static SchemaCommitVerdict committed(long schemaId) {
    return new AutoValue_SchemaCommitVerdict(schemaId, null);
  }

  static SchemaCommitVerdict refused(String reason) {
    return new AutoValue_SchemaCommitVerdict(null, reason);
  }

  boolean refused() {
    return getRefusal() != null;
  }
}
