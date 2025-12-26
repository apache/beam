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
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ChangelogDescriptor {
  public static Builder builder() {
    return new AutoValue_ChangelogDescriptor.Builder();
  }

  public static SchemaCoder<ChangelogDescriptor> coder() {
    try {
      return SchemaRegistry.createDefault().getSchemaCoder(ChangelogDescriptor.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  @SchemaFieldNumber("0")
  abstract String getTableIdentifierString();

  @SchemaFieldNumber("1")
  abstract long getStartSnapshotId();

  @SchemaFieldNumber("2")
  abstract long getEndSnapshotId();

  @SchemaFieldNumber("3")
  abstract int getChangeOrdinal();

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setTableIdentifierString(String table);

    abstract Builder setStartSnapshotId(long snapshotId);

    abstract Builder setEndSnapshotId(long snapshotId);

    abstract Builder setChangeOrdinal(int ordinal);

    abstract ChangelogDescriptor build();
  }
}
