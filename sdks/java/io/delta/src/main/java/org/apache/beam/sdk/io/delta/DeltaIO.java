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
package org.apache.beam.sdk.io.delta;

import com.google.auto.value.AutoValue;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A connector that reads from <a href="https://delta.io/">Delta Lake</a> tables.
 *
 * <h2>Configuration Options</h2>
 *
 * Please check the <a href="https://beam.apache.org/documentation/io/managed-io/">Managed IO
 * configuration page</a> for more details.
 *
 * <p>This is work in progress and is subject to change. For more details and to track progress, see
 * <a href="https://github.com/apache/beam/issues/21100">Issue 21100</a>.
 */
@Internal
public class DeltaIO {

  public static final String CHANGE_TYPE_COLUMN = "_change_type";
  public static final String COMMIT_VERSION_COLUMN = "_commit_version";
  public static final String COMMIT_TIMESTAMP_COLUMN = "_commit_timestamp";

  /**
   * Reads rows from a Delta Lake table.
   *
   * <p>Normally, it is recommended to use {@link org.apache.beam.sdk.managed.Managed#read(String)}
   * with {@code Managed.DELTA_LAKE} instead of directly using this transform.
   */
  public static ReadRows readRows() {
    return new AutoValue_DeltaIO_ReadRows.Builder().build();
  }

  /** Reads change data feed (CDC) from a Delta Lake table. */
  public static ReadChanges readChanges() {
    return new AutoValue_DeltaIO_ReadChanges.Builder().build();
  }

  @AutoValue
  public abstract static class ReadRows extends PTransform<PBegin, PCollection<Row>> {

    public abstract @Nullable String getTablePath();

    public abstract @Nullable Long getVersion();

    public abstract @Nullable String getTimestamp();

    public abstract @Nullable Map<String, String> getHadoopConfig();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTablePath(String tablePath);

      abstract Builder setVersion(@Nullable Long version);

      abstract Builder setTimestamp(@Nullable String timestamp);

      abstract Builder setHadoopConfig(@Nullable Map<String, String> hadoopConfig);

      abstract ReadRows build();
    }

    public ReadRows from(String tablePath) {
      return toBuilder().setTablePath(tablePath).build();
    }

    public ReadRows withVersion(@Nullable Long version) {
      return toBuilder().setVersion(version).build();
    }

    public ReadRows withTimestamp(@Nullable String timestamp) {
      return toBuilder().setTimestamp(timestamp).build();
    }

    public ReadRows withConfig(Map<String, String> config) {
      return toBuilder().setHadoopConfig(config).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      String path = getTablePath();
      if (path == null) {
        throw new IllegalArgumentException("Table path must be set.");
      }
      if (getVersion() != null && getTimestamp() != null) {
        throw new IllegalArgumentException("Cannot set both version and timestamp.");
      }

      Configuration conf = new Configuration();
      Map<String, String> hadoopConfig = getHadoopConfig();
      if (hadoopConfig != null) {
        for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
      }
      Engine engine = DefaultEngine.create(conf);
      Table table = Table.forPath(engine, path);
      Snapshot snapshot;
      Long versionVal = getVersion();
      String timestampVal = getTimestamp();
      if (versionVal != null) {
        snapshot = table.getSnapshotAsOfVersion(engine, versionVal);
      } else if (timestampVal != null) {
        long timestampMillis = java.time.Instant.parse(timestampVal).toEpochMilli();
        snapshot = table.getSnapshotAsOfTimestamp(engine, timestampMillis);
      } else {
        snapshot = table.getLatestSnapshot(engine);
      }
      StructType deltaSchema = snapshot.getSchema();
      if (deltaSchema == null) {
        throw new IllegalStateException("Table schema is null.");
      }
      Schema beamSchema = convertToBeamSchema(deltaSchema);

      return input
          .apply("Create Path", Create.of(path))
          .apply(
              "Plan Files",
              ParDo.of(new CreateReadTasksDoFn(hadoopConfig, getVersion(), getTimestamp())))
          .apply("Read Logical Data", ParDo.of(new DeltaSourceDoFn(hadoopConfig)))
          .setRowSchema(beamSchema);
    }

    static Schema convertToBeamSchema(StructType deltaSchema) {
      Schema.Builder builder = Schema.builder();
      for (StructField field : deltaSchema.fields()) {
        builder.addField(
            Schema.Field.of(field.getName(), convertToBeamFieldType(field.getDataType()))
                .withNullable(field.isNullable()));
      }
      return builder.build();
    }

    static Schema.FieldType convertToBeamFieldType(DataType deltaType) {
      if (deltaType instanceof StringType) {
        return Schema.FieldType.STRING;
      } else if (deltaType instanceof IntegerType) {
        return Schema.FieldType.INT32;
      } else if (deltaType instanceof LongType) {
        return Schema.FieldType.INT64;
      } else if (deltaType instanceof FloatType) {
        return Schema.FieldType.FLOAT;
      } else if (deltaType instanceof DoubleType) {
        return Schema.FieldType.DOUBLE;
      } else if (deltaType instanceof BooleanType) {
        return Schema.FieldType.BOOLEAN;
      } else if (deltaType instanceof BinaryType) {
        return Schema.FieldType.BYTES;
      } else if (deltaType instanceof TimestampType) {
        return Schema.FieldType.logicalType(Timestamp.MICROS);
      } else if (deltaType instanceof TimestampNTZType) {
        return Schema.FieldType.logicalType(SqlTypes.DATETIME);
      } else if (deltaType instanceof DateType) {
        return Schema.FieldType.logicalType(SqlTypes.DATE);
      } else if (deltaType instanceof ArrayType) {
        DataType elementType = ((ArrayType) deltaType).getElementType();
        return Schema.FieldType.iterable(convertToBeamFieldType(elementType));
      } else if (deltaType instanceof MapType) {
        DataType keyType = ((MapType) deltaType).getKeyType();
        DataType valueType = ((MapType) deltaType).getValueType();
        return Schema.FieldType.map(
            convertToBeamFieldType(keyType), convertToBeamFieldType(valueType));
      } else if (deltaType instanceof StructType) {
        return Schema.FieldType.row(convertToBeamSchema((StructType) deltaType));
      } else {
        throw new UnsupportedOperationException("Unsupported Delta type: " + deltaType.getClass());
      }
    }
  }

  static Schema buildPublicBeamSchema(Schema baseSchema, @Nullable List<String> metadataColumns) {
    if (metadataColumns == null || metadataColumns.isEmpty()) {
      return baseSchema;
    }
    Schema.Builder builder = Schema.builder();
    for (Schema.Field field : baseSchema.getFields()) {
      builder.addField(field);
    }
    for (String col : metadataColumns) {
      if (col.equals(CHANGE_TYPE_COLUMN)) {
        builder.addField(CHANGE_TYPE_COLUMN, Schema.FieldType.STRING);
      } else if (col.equals(COMMIT_VERSION_COLUMN)) {
        builder.addField(COMMIT_VERSION_COLUMN, Schema.FieldType.INT64);
      } else if (col.equals(COMMIT_TIMESTAMP_COLUMN)) {
        builder.addField(COMMIT_TIMESTAMP_COLUMN, Schema.FieldType.logicalType(Timestamp.MICROS));
      }
    }
    return builder.build();
  }

  /**
   * Returns whether two Delta schemas (or, recursively, data types) describe the same data.
   *
   * <p>{@link ReadChanges} uses this to verify that the table schema did not change across the
   * range of table versions that is read, so that all emitted change records conform to a single
   * schema.
   *
   * <p>Two schemas describe the same data if their fields have the same names (in the same order),
   * data types, nullability, and column mapping (see {@code hasSameColumnMapping()}), compared
   * recursively for nested types. These properties determine both the Beam schema of the emitted
   * rows (see {@code ReadRows.convertToBeamSchema()}) and how the rows are read from data files.
   *
   * <p>All other field metadata is ignored, since changing it does not change the data. For
   * example, column comments, column default values, or identity column high-water marks (which
   * writers update whenever they insert rows into the table) may differ between the two schemas.
   */
  static boolean hasSameDataSchema(DataType left, DataType right) {
    if (left instanceof StructType && right instanceof StructType) {
      List<StructField> leftFields = ((StructType) left).fields();
      List<StructField> rightFields = ((StructType) right).fields();
      if (leftFields.size() != rightFields.size()) {
        return false;
      }
      // Fields are compared by position since the field order is part of the schema.
      for (int i = 0; i < leftFields.size(); i++) {
        StructField leftField = leftFields.get(i);
        StructField rightField = rightFields.get(i);
        if (!leftField.getName().equals(rightField.getName())
            || leftField.isNullable() != rightField.isNullable()
            || !hasSameColumnMapping(leftField, rightField)
            || !hasSameDataSchema(leftField.getDataType(), rightField.getDataType())) {
          return false;
        }
      }
      return true;
    } else if (left instanceof ArrayType && right instanceof ArrayType) {
      // The nullability of array elements is part of the array type, not of the field metadata.
      ArrayType leftArray = (ArrayType) left;
      ArrayType rightArray = (ArrayType) right;
      return leftArray.containsNull() == rightArray.containsNull()
          && hasSameDataSchema(leftArray.getElementType(), rightArray.getElementType());
    } else if (left instanceof MapType && right instanceof MapType) {
      // The nullability of map values is part of the map type, not of the field metadata.
      MapType leftMap = (MapType) left;
      MapType rightMap = (MapType) right;
      return leftMap.isValueContainsNull() == rightMap.isValueContainsNull()
          && hasSameDataSchema(leftMap.getKeyType(), rightMap.getKeyType())
          && hasSameDataSchema(leftMap.getValueType(), rightMap.getValueType());
    } else {
      // Primitive types don't contain fields, and hence don't carry field metadata, so they can be
      // compared directly (for example, this compares the precision and scale of decimal types).
      // This also returns false if the two types are of different kinds (e.g. a struct and an
      // array).
      return left.equals(right);
    }
  }

  /**
   * Returns whether two fields are read from the same column of the data files.
   *
   * <p>If column mapping is enabled for a table, data files identify columns by the physical names
   * (and, in the 'id' column mapping mode, by the IDs) stored in the column mapping metadata of the
   * fields, instead of by their logical names. Although this is field metadata, it has to be
   * compared since {@link ReadChanges} reads all data files in the requested range of versions
   * using the schema of the end version. For example, if a column was dropped and then re-added
   * with the same name and type, the logical schemas would match. However, data files written
   * before the drop don't contain the re-added column, so change records read from those files
   * would silently get null values for it.
   */
  private static boolean hasSameColumnMapping(StructField left, StructField right) {
    // getPhysicalName() returns the logical name of a field that doesn't have a physical name,
    // which is the case if column mapping is not enabled. Hence, enabling column mapping on an
    // existing table, which sets the physical names of the existing columns to their logical
    // names, is not considered to be a change.
    if (!ColumnMapping.getPhysicalName(left).equals(ColumnMapping.getPhysicalName(right))) {
      return false;
    }
    // Column IDs are never reused, so fields with different IDs belong to different columns (for
    // example, a column that was dropped and re-added) even if their physical names match. IDs are
    // only compared if both fields have one, since enabling column mapping on an existing table
    // assigns IDs to the existing columns.
    String idKey = ColumnMapping.COLUMN_MAPPING_ID_KEY;
    if (left.getMetadata().contains(idKey) && right.getMetadata().contains(idKey)) {
      return ColumnMapping.getColumnId(left) == ColumnMapping.getColumnId(right);
    }
    return true;
  }

  @AutoValue
  public abstract static class ReadChanges extends PTransform<PBegin, PCollection<Row>> {
    public abstract @Nullable String getTablePath();

    public abstract @Nullable Long getStartVersion();

    public abstract @Nullable String getStartTimestamp();

    public abstract @Nullable Long getEndVersion();

    public abstract @Nullable String getEndTimestamp();

    public abstract @Nullable List<String> getMetadataColumns();

    public abstract @Nullable Map<String, String> getHadoopConfig();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTablePath(String tablePath);

      abstract Builder setStartVersion(@Nullable Long startVersion);

      abstract Builder setStartTimestamp(@Nullable String startTimestamp);

      abstract Builder setEndVersion(@Nullable Long endVersion);

      abstract Builder setEndTimestamp(@Nullable String endTimestamp);

      abstract Builder setMetadataColumns(@Nullable List<String> metadataColumns);

      abstract Builder setHadoopConfig(@Nullable Map<String, String> hadoopConfig);

      abstract ReadChanges build();
    }

    public ReadChanges from(String tablePath) {
      return toBuilder().setTablePath(tablePath).build();
    }

    public ReadChanges withStartVersion(long startVersion) {
      return toBuilder().setStartVersion(startVersion).build();
    }

    public ReadChanges withStartTimestamp(String startTimestamp) {
      return toBuilder().setStartTimestamp(startTimestamp).build();
    }

    public ReadChanges withEndVersion(long endVersion) {
      return toBuilder().setEndVersion(endVersion).build();
    }

    public ReadChanges withEndTimestamp(String endTimestamp) {
      return toBuilder().setEndTimestamp(endTimestamp).build();
    }

    public ReadChanges withMetadataColumns(String... metadataColumns) {
      for (String col : metadataColumns) {
        if (!col.equals(CHANGE_TYPE_COLUMN)
            && !col.equals(COMMIT_VERSION_COLUMN)
            && !col.equals(COMMIT_TIMESTAMP_COLUMN)) {
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported metadata column %s. Supported columns are: %s, %s, and %s.",
                  col, CHANGE_TYPE_COLUMN, COMMIT_VERSION_COLUMN, COMMIT_TIMESTAMP_COLUMN));
        }
      }
      return toBuilder().setMetadataColumns(Arrays.asList(metadataColumns)).build();
    }

    public ReadChanges withConfig(Map<String, String> config) {
      return toBuilder().setHadoopConfig(config).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      String path = getTablePath();
      if (path == null) {
        throw new IllegalArgumentException("Table path must be set.");
      }
      if (getStartVersion() == null && getStartTimestamp() == null) {
        // TODO: for unbounded reads, support using current HEAD or the latest snapshot
        // as the default starting point.
        throw new IllegalArgumentException("Either startVersion or startTimestamp must be set.");
      }
      if (getStartVersion() != null && getStartTimestamp() != null) {
        throw new IllegalArgumentException("Cannot set both startVersion and startTimestamp.");
      }
      if (getEndVersion() != null && getEndTimestamp() != null) {
        throw new IllegalArgumentException("Cannot set both endVersion and endTimestamp.");
      }

      Configuration conf = new Configuration();
      Map<String, String> hadoopConfig = getHadoopConfig();
      if (hadoopConfig != null) {
        for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
      }
      Engine engine = DefaultEngine.create(conf);
      Table table = Table.forPath(engine, path);

      TableImpl tableImpl = (TableImpl) table;

      long resolvedStartVersion;
      Long startVersionVal = getStartVersion();
      String startTimestampVal = getStartTimestamp();
      if (startVersionVal != null) {
        resolvedStartVersion = startVersionVal;
      } else if (startTimestampVal != null) {
        long startMillis = java.time.Instant.parse(startTimestampVal).toEpochMilli();
        resolvedStartVersion = tableImpl.getVersionAtOrAfterTimestamp(engine, startMillis);
      } else {
        throw new IllegalArgumentException("Either startVersion or startTimestamp must be set.");
      }

      long resolvedEndVersion;
      Long endVersionVal = getEndVersion();
      String endTimestampVal = getEndTimestamp();
      if (endVersionVal != null) {
        resolvedEndVersion = endVersionVal;
      } else if (endTimestampVal != null) {
        long endMillis = java.time.Instant.parse(endTimestampVal).toEpochMilli();
        resolvedEndVersion = tableImpl.getVersionBeforeOrAtTimestamp(engine, endMillis);
      } else {
        resolvedEndVersion = table.getLatestSnapshot(engine).getVersion();
      }

      if (resolvedStartVersion > resolvedEndVersion) {
        throw new IllegalArgumentException(
            String.format(
                "Resolved start version %d is greater than resolved end version %d",
                resolvedStartVersion, resolvedEndVersion));
      }

      Snapshot startSnapshot = table.getSnapshotAsOfVersion(engine, resolvedStartVersion);
      Snapshot endSnapshot =
          resolvedStartVersion == resolvedEndVersion
              ? startSnapshot
              : table.getSnapshotAsOfVersion(engine, resolvedEndVersion);
      StructType startDeltaSchema = startSnapshot.getSchema();
      StructType deltaSchema = endSnapshot.getSchema();
      if (startDeltaSchema == null || deltaSchema == null) {
        throw new IllegalStateException("Table schema is null.");
      }
      // Fail if the data schema of the table changed across the range of versions that is read, so
      // that all change records conform to a single schema. Metadata-only schema changes (for
      // example, changes to column comments) are allowed. See hasSameDataSchema() for details.
      if (!hasSameDataSchema(startDeltaSchema, deltaSchema)) {
        throw new IllegalArgumentException(
            String.format(
                "Table schema changed between start version %d and end version %d. "
                    + "Schema at start version: %s, schema at end version: %s.",
                resolvedStartVersion, resolvedEndVersion, startDeltaSchema, deltaSchema));
      }
      Schema baseSchema = ReadRows.convertToBeamSchema(deltaSchema);
      Schema publicBeamSchema = buildPublicBeamSchema(baseSchema, getMetadataColumns());

      return input
          .apply("Create Path", Create.of(path))
          .apply(
              "Plan CDF Files",
              ParDo.of(
                  new CreateCDCReadTasksDoFn(
                      hadoopConfig,
                      getStartVersion(),
                      getStartTimestamp(),
                      getEndVersion(),
                      getEndTimestamp())))
          .apply(
              "Read CDF Data", ParDo.of(new DeltaCDCSourceDoFn(hadoopConfig, getMetadataColumns())))
          .setRowSchema(publicBeamSchema);
    }
  }
}
