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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.util.JsonUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A lightweight serializable descriptor of a {@link FileScanTask}, dropping the table schema,
 * partition spec and residual that the full task JSON carries, and the data file's column metrics.
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class TaskDescriptor {
  @SchemaFieldNumber("0")
  public abstract String getDataFileJson();

  @SchemaFieldNumber("1")
  public abstract int getSpecId();

  @SchemaFieldNumber("2")
  public abstract long getStart();

  @SchemaFieldNumber("3")
  public abstract long getLength();

  /**
   * The input file's data sequence number, carried alongside the file JSON: v3 row lineage derives
   * {@code _last_updated_sequence_number} from it on rewrite.
   */
  @SchemaFieldNumber("4")
  public abstract long getDataSequenceNumber();

  @SchemaFieldNumber("5")
  public abstract List<String> getDeleteFileJsons();

  static Builder builder() {
    return new AutoValue_TaskDescriptor.Builder();
  }

  /** Builds a compact descriptor from one planned range task. */
  static TaskDescriptor from(FileScanTask task, Map<Integer, PartitionSpec> specs) {
    PartitionSpec dataSpec =
        checkStateNotNull(
            specs.get(task.file().specId()),
            "Data file spec id %s not found in table specs %s",
            task.file().specId(),
            specs.keySet());
    List<String> deleteJsons = new ArrayList<>(task.deletes().size());
    for (DeleteFile delete : task.deletes()) {
      PartitionSpec deleteSpec =
          checkStateNotNull(
              specs.get(delete.specId()),
              "Delete file spec id %s not found in table specs %s",
              delete.specId(),
              specs.keySet());
      deleteJsons.add(ContentFileParser.toJson(delete, deleteSpec));
    }
    @Nullable Long seq = task.file().dataSequenceNumber();
    return builder()
        .setDataFileJson(ContentFileParser.toJson(task.file().copyWithoutStats(), dataSpec))
        .setSpecId(task.file().specId())
        .setStart(task.start())
        .setLength(task.length())
        .setDataSequenceNumber(seq != null ? seq : 0L)
        .setDeleteFileJsons(deleteJsons)
        .build();
  }

  /** Rebuilds the worker-side {@link FileScanTask} for this range. */
  FileScanTask toScanTask(Map<Integer, PartitionSpec> specs) {
    DataFile file =
        (DataFile)
            JsonUtil.parse(getDataFileJson(), node -> ContentFileParser.fromJson(node, specs));
    List<DeleteFile> deletes = new ArrayList<>(getDeleteFileJsons().size());
    for (String deleteJson : getDeleteFileJsons()) {
      deletes.add(
          (DeleteFile) JsonUtil.parse(deleteJson, node -> ContentFileParser.fromJson(node, specs)));
    }
    PartitionSpec spec =
        checkStateNotNull(
            specs.get(getSpecId()),
            "Spec id %s not found in table specs %s",
            getSpecId(),
            specs.keySet());
    return new RangeFileScanTask(file, deletes, getStart(), getLength(), spec);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setDataFileJson(String dataFileJson);

    abstract Builder setSpecId(int specId);

    abstract Builder setStart(long start);

    abstract Builder setLength(long length);

    abstract Builder setDataSequenceNumber(long dataSequenceNumber);

    abstract Builder setDeleteFileJsons(List<String> deleteFileJsons);

    abstract TaskDescriptor build();
  }
}
