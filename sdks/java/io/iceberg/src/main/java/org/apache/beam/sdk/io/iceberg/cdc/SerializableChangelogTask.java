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

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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

  @SchemaFieldNumber("11")
  public abstract long getTimestampMillis();

  @SchemaIgnore
  public Expression getExpression(Schema schema) {
    return ExpressionParser.fromJson(getJsonExpression(), schema);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setType(Type type);

    abstract Builder setDataFile(SerializableDataFile dataFile);

    @SchemaIgnore
    public Builder setDataFile(DataFile df, String partitionPath) {
      return setDataFile(SerializableDataFile.from(df, partitionPath));
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

    abstract Builder setTimestampMillis(long timestampMillis);

    abstract SerializableChangelogTask build();
  }

  @SuppressWarnings("nullness")
  public static SerializableChangelogTask from(ChangelogScanTask task, long timestampMillis) {
    checkState(
        task instanceof ContentScanTask, "Expected ChangelogScanTask to also be a ContentScanTask");
    ContentScanTask<DataFile> contentScanTask = (ContentScanTask<DataFile>) task;
    PartitionSpec spec = contentScanTask.spec();
    SerializableChangelogTask.Builder builder =
        SerializableChangelogTask.builder()
            .setOperation(task.operation())
            .setOrdinal(task.changeOrdinal())
            .setCommitSnapshotId(task.commitSnapshotId())
            .setDataFile(contentScanTask.file(), spec.partitionToPath(contentScanTask.partition()))
            .setSpecId(spec.specId())
            .setStart(contentScanTask.start())
            .setLength(contentScanTask.length())
            .setJsonExpression(ExpressionParser.toJson(contentScanTask.residual()))
            .setTimestampMillis(timestampMillis);

    if (task instanceof AddedRowsScanTask) {
      AddedRowsScanTask addedRowsTask = (AddedRowsScanTask) task;
      builder =
          builder
              .setType(Type.ADDED_ROWS)
              .setAddedDeletes(toSerializableDeletes(addedRowsTask.deletes(), spec));
    } else if (task instanceof DeletedRowsScanTask) {
      DeletedRowsScanTask deletedRowsTask = (DeletedRowsScanTask) task;
      builder =
          builder
              .setType(Type.DELETED_ROWS)
              .setAddedDeletes(toSerializableDeletes(deletedRowsTask.addedDeletes(), spec))
              .setExistingDeletes(toSerializableDeletes(deletedRowsTask.existingDeletes(), spec));
    } else if (task instanceof DeletedDataFileScanTask) {
      DeletedDataFileScanTask deletedFileTask = (DeletedDataFileScanTask) task;
      builder =
          builder
              .setType(Type.DELETED_FILE)
              .setExistingDeletes(toSerializableDeletes(deletedFileTask.existingDeletes(), spec));
    } else {
      throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
    }
    return builder.build();
  }

  private static List<SerializableDeleteFile> toSerializableDeletes(
      List<DeleteFile> dfs, PartitionSpec spec) {
    return dfs.stream()
        .map(df -> SerializableDeleteFile.from(df, spec.partitionToPath(df.partition())))
        .collect(Collectors.toList());
  }

  public static Comparator<SerializableChangelogTask> comparator() {
    return (task1, task2) -> {
      int ordinalCompare = Integer.compare(task1.getOrdinal(), task2.getOrdinal());
      if (ordinalCompare != 0) {
        return ordinalCompare;
      }

      int op1Weight = getOperationWeight(task1.getOperation());
      int op2Weight = getOperationWeight(task2.getOperation());

      return Integer.compare(op1Weight, op2Weight);
    };
  }

  private static int getOperationWeight(ChangelogOperation op) {
    switch (op) {
      case DELETE:
      case UPDATE_BEFORE:
        return 0;
      case INSERT:
      case UPDATE_AFTER:
        return 1;
      default:
        throw new UnsupportedOperationException("Unknown ChangelogOperation: " + op);
    }
  }
}
