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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Helpers for CDC schemas and output row construction.
 *
 * <p>CDC metadata is handled in two phases. Row metadata, such as {@code _row_id} and {@code
 * _last_updated_sequence_number}, is added to intermediate read schemas so Iceberg readers can
 * populate those values. Commit metadata, such as {@code _commit_snapshot_id} and {@code
 * _commit_snapshot_sequence_number}, is carried separately in CDC descriptors. The {@code
 * _change_type} metadata column comes from the resolved Beam output {@link ValueKind}.
 *
 * <p>The public output shape is assembled only when final Beam {@link Row}s are emitted. This keeps
 * the read path table-shaped while still exposing all requested metadata as top-level output
 * fields.
 */
final class CdcOutputUtils {
  /**
   * Returns the public CDC output schema: projected data fields followed by requested metadata
   * columns in user-configured order.
   */
  static Schema outputSchema(IcebergScanConfig scanConfig, Schema dataSchema) {
    if (scanConfig.getMetadataColumns().isEmpty()) {
      return dataSchema;
    }

    Schema.Builder builder = Schema.builder().addFields(dataSchema.getFields());
    for (String metadataColumn : scanConfig.getMetadataColumns()) {
      builder.addField(IcebergCdcMetadataColumns.beamField(metadataColumn));
    }
    return builder.build();
  }

  /**
   * Returns an Iceberg read schema that includes row metadata columns.
   *
   * <p>Commit metadata columns are not added here because Iceberg readers cannot populate them;
   * those values are taken from {@link ChangelogDescriptor} or {@link CdcRowDescriptor} when output
   * rows are built.
   */
  static org.apache.iceberg.Schema readSchemaWithRowMetadata(
      List<String> metadataColumns, org.apache.iceberg.Schema dataSchema) {
    List<Types.NestedField> fields = new ArrayList<>(dataSchema.columns());
    for (String metadataColumn : metadataColumns) {
      Types.NestedField rowMetadataField =
          IcebergCdcMetadataColumns.icebergRowMetadataField(metadataColumn);
      if (rowMetadataField != null && dataSchema.findField(rowMetadataField.fieldId()) == null) {
        fields.add(rowMetadataField);
      }
    }
    return new org.apache.iceberg.Schema(fields, dataSchema.identifierFieldIds());
  }

  /**
   * Beam-schema equivalent of {@link #readSchemaWithRowMetadata(List, org.apache.iceberg.Schema)}.
   */
  static Schema readBeamSchemaWithRowMetadata(List<String> metadataColumns, Schema dataSchema) {
    if (metadataColumns.stream().noneMatch(IcebergCdcMetadataColumns::isRowMetadataColumn)) {
      return dataSchema;
    }

    Schema.Builder builder = Schema.builder().addFields(dataSchema.getFields());
    for (String metadataColumn : metadataColumns) {
      if (IcebergCdcMetadataColumns.isRowMetadataColumn(metadataColumn)
          && !dataSchema.hasField(metadataColumn)) {
        builder.addField(IcebergCdcMetadataColumns.beamField(metadataColumn));
      }
    }
    return builder.build();
  }

  static Row outputRow(
      List<String> metadataColumns,
      Schema outputSchema,
      ChangelogDescriptor descriptor,
      ValueKind valueKind,
      Row dataAndRowMetadata) {
    return outputRow(
        metadataColumns,
        outputSchema,
        descriptor.getCommitSnapshotId(),
        descriptor.getSnapshotSequenceNumber(),
        valueKind,
        dataAndRowMetadata);
  }

  /**
   * Builds the final public Beam row.
   *
   * <p>{@code dataAndRowMetadata} may already include row metadata read from Iceberg. This method
   * copies only data fields first, then appends every requested metadata column at the top level.
   * That preserves configured column order and avoids exposing row metadata twice.
   */
  static Row outputRow(
      List<String> metadataColumns,
      Schema outputSchema,
      long commitSnapshotId,
      long snapshotSequentNumber,
      ValueKind valueKind,
      Row dataAndRowMetadata) {
    if (metadataColumns.isEmpty()
        || metadataColumns.stream().allMatch(IcebergCdcMetadataColumns::isRowMetadataColumn)) {
      return dataAndRowMetadata;
    }

    List<@Nullable Object> values = new ArrayList<>(outputSchema.getFieldCount());
    for (Schema.Field field : dataAndRowMetadata.getSchema().getFields()) {
      if (!metadataColumns.contains(field.getName())) {
        values.add(dataAndRowMetadata.getValue(field.getName()));
      }
    }

    for (String metadataColumn : metadataColumns) {
      values.add(
          metadataValue(
              metadataColumn,
              commitSnapshotId,
              snapshotSequentNumber,
              valueKind,
              dataAndRowMetadata));
    }
    return Row.withSchema(outputSchema).addValues(values).build();
  }

  static Schema readBeamSchemaWithRowMetadata(
      List<String> metadataColumns, org.apache.iceberg.Schema dataSchema) {
    return IcebergUtils.icebergSchemaToBeamSchema(
        readSchemaWithRowMetadata(metadataColumns, dataSchema));
  }

  private static @Nullable Object metadataValue(
      String metadataColumn,
      long commitSnapshotId,
      long commitSnapshotSequenceNumber,
      ValueKind valueKind,
      Row dataAndRowMetadata) {
    if (IcebergCdcMetadataColumns.CHANGE_TYPE.equals(metadataColumn)) {
      return changelogOperation(valueKind).name();
    }
    if (IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID.equals(metadataColumn)) {
      return commitSnapshotId;
    }
    if (IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER.equals(metadataColumn)) {
      return commitSnapshotSequenceNumber;
    }
    if (dataAndRowMetadata.getSchema().hasField(metadataColumn)) {
      return dataAndRowMetadata.getValue(metadataColumn);
    }
    return null;
  }

  private static ChangelogOperation changelogOperation(ValueKind valueKind) {
    switch (valueKind) {
      case INSERT:
        return ChangelogOperation.INSERT;
      case DELETE:
        return ChangelogOperation.DELETE;
      case UPDATE_BEFORE:
        return ChangelogOperation.UPDATE_BEFORE;
      case UPDATE_AFTER:
        return ChangelogOperation.UPDATE_AFTER;
      default:
        throw new IllegalArgumentException("Unsupported CDC ValueKind: " + valueKind);
    }
  }
}
