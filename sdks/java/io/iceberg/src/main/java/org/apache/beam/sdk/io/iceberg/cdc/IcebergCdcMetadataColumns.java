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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Supported top-level metadata columns for Beam Iceberg CDC reads.
 *
 * <p>The supported columns come from two sources:
 *
 * <ul>
 *   <li>Iceberg row metadata: {@code _row_id} and {@code _last_updated_sequence_number}. These are
 *       requested from the physical Iceberg reader and are only available for row-lineage tables
 *       (v3+).
 *   <li>Changelog context metadata: {@code _change_type}, {@code _commit_snapshot_id}, and {@code
 *       _commit_snapshot_sequence_number}. These are known from the changelog snapshot/task context
 *       and are appended when Beam output rows are built.
 * </ul>
 */
@Internal
public final class IcebergCdcMetadataColumns {
  public static final String CHANGE_TYPE = MetadataColumns.CHANGE_TYPE.name();
  public static final String COMMIT_SNAPSHOT_SEQUENCE_NUMBER = "_commit_snapshot_sequence_number";
  public static final String COMMIT_SNAPSHOT_ID = MetadataColumns.COMMIT_SNAPSHOT_ID.name();
  public static final String ROW_ID = MetadataColumns.ROW_ID.name();
  public static final String LAST_UPDATED_SEQUENCE_NUMBER =
      MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name();

  public static final ImmutableList<String> SUPPORTED_COLUMNS =
      ImmutableList.of(
          CHANGE_TYPE,
          COMMIT_SNAPSHOT_ID,
          COMMIT_SNAPSHOT_SEQUENCE_NUMBER,
          ROW_ID,
          LAST_UPDATED_SEQUENCE_NUMBER);

  private static final ImmutableSet<String> ROW_METADATA_COLUMNS =
      ImmutableSet.of(ROW_ID, LAST_UPDATED_SEQUENCE_NUMBER);

  public static boolean isSupportedColumn(String name) {
    return SUPPORTED_COLUMNS.contains(name);
  }

  public static boolean isRowMetadataColumn(String name) {
    return ROW_METADATA_COLUMNS.contains(name);
  }

  public static Schema.Field beamField(String name) {
    if (CHANGE_TYPE.equals(name)) {
      return Schema.Field.of(name, Schema.FieldType.STRING);
    }
    if (COMMIT_SNAPSHOT_ID.equals(name) || COMMIT_SNAPSHOT_SEQUENCE_NUMBER.equals(name)) {
      return Schema.Field.of(name, Schema.FieldType.INT64);
    }
    if (ROW_ID.equals(name) || LAST_UPDATED_SEQUENCE_NUMBER.equals(name)) {
      return Schema.Field.nullable(name, Schema.FieldType.INT64);
    }
    throw new IllegalArgumentException("Unsupported CDC metadata column: " + name);
  }

  /** Returns the Iceberg reader field for row-sourced metadata, or null for commit metadata. */
  public static Types.@Nullable NestedField icebergRowMetadataField(String name) {
    if (ROW_ID.equals(name)) {
      return MetadataColumns.ROW_ID;
    }
    if (LAST_UPDATED_SEQUENCE_NUMBER.equals(name)) {
      return MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER;
    }
    return null;
  }
}
