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

/**
 * Shuffle key for bidirectional CDC rows.
 *
 * <p>The primary key isolates rows for update resolution. The snapshot sequence number and commit
 * snapshot id carry commit-sourced metadata through {@link ResolveChanges}, where they can be
 * appended to final output rows if requested.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class CdcRowDescriptor {
  @SuppressWarnings("nullness")
  public static SchemaCoder<CdcRowDescriptor> coder(Schema identifierSchema) {
    Schema descriptorSchema =
        Schema.builder()
            .addInt64Field("snapshotSequenceNumber")
            .addInt64Field("commitSnapshotId")
            .addRowField("primaryKey", identifierSchema)
            .build();

    return SchemaCoder.of(
        descriptorSchema,
        TypeDescriptor.of(CdcRowDescriptor.class),
        descriptor ->
            Row.withSchema(descriptorSchema)
                .addValues(
                    descriptor.getSnapshotSequenceNumber(),
                    descriptor.getCommitSnapshotId(),
                    descriptor.getPrimaryKey())
                .build(),
        row ->
            CdcRowDescriptor.builder()
                .setSnapshotSequenceNumber(row.getInt64("snapshotSequenceNumber"))
                .setCommitSnapshotId(row.getInt64("commitSnapshotId"))
                .setPrimaryKey(row.getRow("primaryKey"))
                .build());
  }

  public static Builder builder() {
    return new AutoValue_CdcRowDescriptor.Builder();
  }

  @SchemaFieldNumber("0")
  public abstract long getSnapshotSequenceNumber();

  @SchemaFieldNumber("1")
  public abstract long getCommitSnapshotId();

  @SchemaFieldNumber("2")
  public abstract Row getPrimaryKey();

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setSnapshotSequenceNumber(long sequenceNumber);

    abstract Builder setCommitSnapshotId(long snapshotId);

    abstract Builder setPrimaryKey(Row primaryKey);

    abstract CdcRowDescriptor build();
  }
}
