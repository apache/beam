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

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

/**
 * Utilities for validating the schema changes of a Delta table within the range of table versions
 * that is read by {@link DeltaIO.ReadChanges}.
 *
 * <p>All changes within the range are read using the table schema at the end version of the range.
 * Similar to change data feed reads in Delta Lake, this is only supported if the range only spans
 * additive schema changes, i.e. if data written with the table schema at any version within the
 * range can be read using the table schema at the end version (see {@link #isReadCompatible}).
 */
final class DeltaSchemaUtils {

  private DeltaSchemaUtils() {}

  /**
   * Throws an {@link IllegalArgumentException} if data written with {@code schema}, the table
   * schema at version {@code schemaVersion}, can't be read using {@code endSchema}, the table
   * schema at the end version of the range of versions from {@code startVersion} to {@code
   * endVersion} that is read.
   */
  static void checkReadCompatible(
      StructType schema,
      long schemaVersion,
      StructType endSchema,
      long startVersion,
      long endVersion) {
    if (!isReadCompatible(schema, endSchema)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot read changes between versions %d and %d since the table schema at version %d "
                  + "is not compatible with the table schema at the end version %d, which is used "
                  + "to read all changes. Only additive schema changes, such as adding nullable "
                  + "columns, are supported within the range of versions that is read. "
                  + "Schema at version %d: %s, schema at version %d: %s.",
              startVersion,
              endVersion,
              schemaVersion,
              endVersion,
              schemaVersion,
              schema,
              endVersion,
              endSchema));
    }
  }

  /**
   * Returns whether data written with {@code dataSchema} can be read using {@code readSchema}, i.e.
   * whether {@code readSchema} only differs from {@code dataSchema} by additive changes.
   *
   * <p>This mirrors the schema compatibility check of change data feed reads in Delta Lake. The
   * following changes are allowed:
   *
   * <ul>
   *   <li>Adding nullable fields, including nested fields. Records read from data written with
   *       {@code dataSchema} get null values for these fields.
   *   <li>Relaxing the nullability of a field from non-nullable to nullable.
   *   <li>Reordering fields, since fields are matched by name.
   *   <li>Changing field metadata other than column mapping metadata (see {@code
   *       hasSameColumnMapping()}). For example, column comments, column default values, or
   *       identity column high-water marks (which writers update whenever they insert rows into the
   *       table) may change. Such metadata does not affect the data or the Beam schema of the
   *       emitted rows.
   * </ul>
   *
   * <p>All other changes are not allowed. In particular, dropping or renaming fields (including
   * dropping and re-adding a field with the same name), changing data types, tightening the
   * nullability of fields, and adding non-nullable fields.
   */
  static boolean isReadCompatible(StructType dataSchema, StructType readSchema) {
    // Every field of the data schema must still exist in the read schema. Fields are matched by
    // their (case-sensitive) names since the names are part of the Beam schema of the emitted rows.
    // The positions of fields don't matter since data is read from the columns of data files by
    // name.
    for (StructField dataField : dataSchema.fields()) {
      int readFieldIndex = readSchema.indexOf(dataField.getName());
      if (readFieldIndex < 0) {
        // The field was dropped or renamed.
        return false;
      }
      StructField readField = readSchema.at(readFieldIndex);
      if (!hasSameColumnMapping(dataField, readField)
          || !isNullabilityCompatible(dataField.isNullable(), readField.isNullable())
          || !isDataTypeReadCompatible(dataField.getDataType(), readField.getDataType())) {
        return false;
      }
    }
    // Fields that only exist in the read schema were added. Data written with the data schema
    // doesn't contain values for them, so they are read as nulls and hence must be nullable.
    for (StructField readField : readSchema.fields()) {
      if (dataSchema.indexOf(readField.getName()) < 0 && !readField.isNullable()) {
        return false;
      }
    }
    return true;
  }

  /** Returns whether data of type {@code dataType} can be read as type {@code readType}. */
  private static boolean isDataTypeReadCompatible(DataType dataType, DataType readType) {
    if (dataType instanceof StructType && readType instanceof StructType) {
      return isReadCompatible((StructType) dataType, (StructType) readType);
    } else if (dataType instanceof ArrayType && readType instanceof ArrayType) {
      // The nullability of array elements is part of the array type, not of field metadata.
      ArrayType dataArray = (ArrayType) dataType;
      ArrayType readArray = (ArrayType) readType;
      return isNullabilityCompatible(dataArray.containsNull(), readArray.containsNull())
          && isDataTypeReadCompatible(dataArray.getElementType(), readArray.getElementType());
    } else if (dataType instanceof MapType && readType instanceof MapType) {
      // The nullability of map values is part of the map type, not of field metadata.
      MapType dataMap = (MapType) dataType;
      MapType readMap = (MapType) readType;
      return isNullabilityCompatible(dataMap.isValueContainsNull(), readMap.isValueContainsNull())
          && isDataTypeReadCompatible(dataMap.getKeyType(), readMap.getKeyType())
          && isDataTypeReadCompatible(dataMap.getValueType(), readMap.getValueType());
    } else {
      // Primitive types must be the same since type changes (such as widening an integer to a
      // long) are not supported. Primitive types don't contain fields, and hence don't carry field
      // metadata, so they can be compared directly (for example, this compares the precision and
      // scale of decimal types). This also returns false if the two types are of different kinds
      // (e.g. a struct and an array).
      return dataType.equals(readType);
    }
  }

  /**
   * Returns whether data with nullability {@code dataNullable} can be read with nullability {@code
   * readNullable}. Nullability may be relaxed but not tightened, since data written with a nullable
   * type may contain null values.
   */
  private static boolean isNullabilityCompatible(boolean dataNullable, boolean readNullable) {
    return readNullable || !dataNullable;
  }

  /**
   * Returns whether two fields are read from the same column of the data files.
   *
   * <p>If column mapping is enabled for a table, data files identify columns by the physical names
   * (and, in the 'id' column mapping mode, by the IDs) stored in the column mapping metadata of the
   * fields, instead of by their logical names. Although this is field metadata, it has to be
   * compared since all data files in the range of versions that is read are read using the schema
   * of the end version. For example, if a column was dropped and then re-added with the same name
   * and type, the logical schemas would match. However, data files written before the drop don't
   * contain the re-added column, so change records read from those files would silently get null
   * values for it.
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
}
