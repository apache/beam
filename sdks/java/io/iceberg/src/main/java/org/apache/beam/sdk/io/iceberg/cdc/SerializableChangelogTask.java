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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SerializableChangelogTask {
  public enum Type {
    ADDED_ROWS,
    DELETED_ROWS,
    DELETED_FILE
  }

  public static SchemaCoder<SerializableChangelogTask> coder() {
    try {
      return SchemaRegistry.createDefault().getSchemaCoder(SerializableChangelogTask.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  public static SerializableChangelogTask.Builder builder() {
    return new AutoValue_SerializableChangelogTask.Builder()
        .setExistingDeletes(Collections.emptyList())
        .setAddedDeletes(Collections.emptyList());
  }

  @SchemaFieldNumber("0")
  public abstract Type getType();

  @SchemaFieldNumber("1")
  public abstract SerializableDataFile getDataFile();

  @SchemaFieldNumber("2")
  public abstract List<SerializableDeleteFile> getExistingDeletes();

  @SchemaFieldNumber("3")
  public abstract List<SerializableDeleteFile> getAddedDeletes();

  @SchemaFieldNumber("4")
  public abstract int getSpecId();

  @SchemaFieldNumber("5")
  public abstract ChangelogOperation getOperation();

  @SchemaFieldNumber("6")
  public abstract int getOrdinal();

  @SchemaFieldNumber("7")
  public abstract long getCommitSnapshotId();

  @SchemaFieldNumber("8")
  public abstract long getStart();

  @SchemaFieldNumber("9")
  public abstract long getLength();

  @SchemaFieldNumber("10")
  public abstract String getJsonExpression();

  @SchemaIgnore
  public Expression getExpression(Schema schema) {
    return ExpressionParser.fromJson(getJsonExpression(), schema);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setType(Type type);

    abstract Builder setDataFile(SerializableDataFile dataFile);

    @SchemaIgnore
    public Builder setDataFile(DataFile df, String partitionPath, boolean includeMetrics) {
      return setDataFile(SerializableDataFile.from(df, partitionPath, includeMetrics));
    }

    abstract Builder setExistingDeletes(List<SerializableDeleteFile> existingDeletes);

    abstract Builder setAddedDeletes(List<SerializableDeleteFile> addedDeletes);

    abstract Builder setSpecId(int specId);

    abstract Builder setOperation(ChangelogOperation operation);

    abstract Builder setOrdinal(int ordinal);

    abstract Builder setCommitSnapshotId(long commitSnapshotId);

    abstract Builder setStart(long start);

    abstract Builder setLength(long length);

    abstract Builder setJsonExpression(String expression);

    abstract SerializableChangelogTask build();
  }

  public static SerializableChangelogTask from(
      ChangelogScanTask task, Map<Integer, PartitionSpec> specs) {
    return from(task, specs, false);
  }

  public static SerializableChangelogTask from(
      ChangelogScanTask task, Map<Integer, PartitionSpec> specs, boolean includeMetrics) {
    checkState(
        task instanceof ContentScanTask, "Expected ChangelogScanTask to also be a ContentScanTask");
    ContentScanTask<DataFile> contentScanTask = (ContentScanTask<DataFile>) task;
    PartitionSpec spec = contentScanTask.spec();
    SerializableChangelogTask.Builder builder =
        SerializableChangelogTask.builder()
            .setOperation(task.operation())
            .setOrdinal(task.changeOrdinal())
            .setCommitSnapshotId(task.commitSnapshotId())
            .setDataFile(
                contentScanTask.file(),
                spec.partitionToPath(contentScanTask.partition()),
                includeMetrics)
            .setSpecId(spec.specId())
            .setStart(contentScanTask.start())
            .setLength(contentScanTask.length())
            .setJsonExpression(ExpressionParser.toJson(contentScanTask.residual()));

    if (task instanceof AddedRowsScanTask) {
      AddedRowsScanTask addedRowsTask = (AddedRowsScanTask) task;
      builder =
          builder
              .setType(Type.ADDED_ROWS)
              .setAddedDeletes(
                  toSerializableDeletes(addedRowsTask.deletes(), specs, includeMetrics));
    } else if (task instanceof DeletedRowsScanTask) {
      DeletedRowsScanTask deletedRowsTask = (DeletedRowsScanTask) task;
      builder =
          builder
              .setType(Type.DELETED_ROWS)
              .setAddedDeletes(
                  toSerializableDeletes(deletedRowsTask.addedDeletes(), specs, includeMetrics))
              .setExistingDeletes(
                  toSerializableDeletes(deletedRowsTask.existingDeletes(), specs, includeMetrics));
    } else if (task instanceof DeletedDataFileScanTask) {
      DeletedDataFileScanTask deletedFileTask = (DeletedDataFileScanTask) task;
      builder =
          builder
              .setType(Type.DELETED_FILE)
              .setExistingDeletes(
                  toSerializableDeletes(deletedFileTask.existingDeletes(), specs, includeMetrics));
    } else {
      throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
    }
    return builder.build();
  }

  static Type getType(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return Type.ADDED_ROWS;
    } else if (task instanceof DeletedRowsScanTask) {
      return Type.DELETED_ROWS;
    } else if (task instanceof DeletedDataFileScanTask) {
      return Type.DELETED_FILE;
    } else {
      throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
    }
  }

  static long getTotalLength(List<ChangelogScanTask> tasks) {
    return tasks.stream().mapToLong(SerializableChangelogTask::getLength).sum();
  }

  static long getLength(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).length();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).length();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).length();
    }
    throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
  }

  static StructLike getPartition(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).partition();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).partition();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).partition();
    }
    throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
  }

  static PartitionSpec getSpec(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).spec();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).spec();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).spec();
    }
    throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
  }

  static DataFile getDataFile(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).file();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).file();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).file();
    }
    throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
  }

  static List<DeleteFile> getAddedDeleteFiles(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).deletes();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).addedDeletes();
    } else if (task instanceof DeletedDataFileScanTask) {
      return Collections.emptyList();
    }
    throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
  }

  private static List<SerializableDeleteFile> toSerializableDeletes(
      List<DeleteFile> dfs, Map<Integer, PartitionSpec> specs, boolean includeMetrics) {
    return dfs.stream()
        .map(
            df ->
                SerializableDeleteFile.from(
                    df,
                    checkStateNotNull(specs.get(df.specId())).partitionToPath(df.partition()),
                    includeMetrics))
        .collect(Collectors.toList());
  }
}
