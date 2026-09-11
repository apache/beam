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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

/**
 * Holds the serialized metadata for {@link DataFile}s and {@link DeleteFile}s of one {@code
 * (destination, shard, window)} group, plus the source sequence range the bundle covers.
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ShardDeltaFiles {

  @SchemaFieldNumber("0")
  public abstract String getTableIdentifierString();

  /** Reconstructed into live {@link DataFile}s at commit time. */
  @SchemaFieldNumber("1")
  public abstract List<SerializableDataFile> getDataFiles();

  /** Reconstructed into live {@link DeleteFile}s at commit time. */
  @SchemaFieldNumber("2")
  public abstract List<SerializableDeleteFile> getDeleteFiles();

  /** The minimum source sequence number covered by this bundle. */
  @SchemaFieldNumber("3")
  public abstract long getMinSequenceNumber();

  /** The maximum source sequence number covered by this bundle. */
  @SchemaFieldNumber("4")
  public abstract long getMaxSequenceNumber();

  public static Builder builder() {
    return new AutoValue_ShardDeltaFiles.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableIdentifierString(String tableIdString);

    public abstract Builder setDataFiles(List<SerializableDataFile> dataFiles);

    public abstract Builder setDeleteFiles(List<SerializableDeleteFile> deleteFiles);

    public abstract Builder setMinSequenceNumber(long minSequenceNumber);

    public abstract Builder setMaxSequenceNumber(long maxSequenceNumber);

    public abstract ShardDeltaFiles build();
  }
}
