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
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/** Describes the table a {@link ReadTask} belongs to. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class ReadTaskDescriptor {
  private static @MonotonicNonNull SchemaCoder<ReadTaskDescriptor> coder;

  static SchemaCoder<ReadTaskDescriptor> getCoder() {
    if (coder == null) {
      try {
        coder = SchemaRegistry.createDefault().getSchemaCoder(ReadTaskDescriptor.class);
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }
    return coder;
  }

  static Builder builder() {
    return new AutoValue_ReadTaskDescriptor.Builder();
  }

  abstract String getTableIdentifierString();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTableIdentifierString(String table);

    abstract ReadTaskDescriptor build();
  }
}
