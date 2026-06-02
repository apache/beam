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
package org.apache.beam.sdk.io.iceberg.cdc;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Descriptor for a set of {@link SerializableChangelogTask}s. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ChangelogDescriptor {
  public static Builder builder() {
    return new AutoValue_ChangelogDescriptor.Builder();
  }

  @SuppressWarnings("nullness")
  public static SchemaCoder<ChangelogDescriptor> coder(Schema overlapSchema) {
    Schema descriptorSchema =
        Schema.builder()
            .addStringField("tableIdentifierString")
            .addInt64Field("sequenceNumber")
            .addNullableField("overlapLower", Schema.FieldType.row(overlapSchema))
            .addNullableField("overlapUpper", Schema.FieldType.row(overlapSchema))
            .build();

    return SchemaCoder.of(
        descriptorSchema,
        TypeDescriptor.of(ChangelogDescriptor.class),
        descriptor ->
            Row.withSchema(descriptorSchema)
                .addValues(
                    descriptor.getTableIdentifierString(),
                    descriptor.getSequenceNumber(),
                    descriptor.getOverlapLower(),
                    descriptor.getOverlapUpper())
                .build(),
        row ->
            ChangelogDescriptor.builder()
                .setTableIdentifierString(row.getString("tableIdentifierString"))
                .setSequenceNumber(row.getInt64("sequenceNumber"))
                .setOverlapLower(row.getRow("overlapLower"))
                .setOverlapUpper(row.getRow("overlapUpper"))
                .build());
  }

  @SchemaFieldNumber("0")
  public abstract String getTableIdentifierString();

  @SchemaFieldNumber("1")
  public abstract long getSequenceNumber();

  @SchemaFieldNumber("2")
  public abstract @Nullable Row getOverlapLower();

  @SchemaFieldNumber("3")
  public abstract @Nullable Row getOverlapUpper();

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setTableIdentifierString(String table);

    abstract Builder setSequenceNumber(long sequenceNumber);

    abstract Builder setOverlapLower(@Nullable Row overlapLower);

    abstract Builder setOverlapUpper(@Nullable Row overlapUpper);

    abstract ChangelogDescriptor build();
  }
}
