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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Configuration for a partition metadata table. It encapsulates the name of the metadata table and
 * indexes.
 */
public class PartitionMetadataTableNames implements Serializable {

  private static final long serialVersionUID = 8848098877671834584L;

  /** PostgreSQL max table and index length is 63 bytes. */
  @VisibleForTesting static final int MAX_NAME_LENGTH = 63;

  private static final String PARTITION_METADATA_TABLE_NAME_FORMAT = "Metadata_%s_%s";
  private static final String WATERMARK_INDEX_NAME_FORMAT = "WatermarkIdx_%s_%s";
  private static final String CREATED_AT_START_TIMESTAMP_INDEX_NAME_FORMAT = "CreatedAtIdx_%s_%s";

  /**
   * Generates a unique name for the partition metadata table and its indexes. The table name will
   * be in the form of {@code "Metadata_<databaseId>_<uuid>"}. The watermark index will be in the
   * form of {@code "WatermarkIdx_<databaseId>_<uuid>}. The createdAt / start timestamp index will
   * be in the form of {@code "CreatedAtIdx_<databaseId>_<uuid>}.
   *
   * @param databaseId The database id where the table will be created
   * @return the unique generated names of the partition metadata ddl
   */
  public static PartitionMetadataTableNames from(String databaseId) {
    UUID uuid = UUID.randomUUID();
    String table =
        String.format(PARTITION_METADATA_TABLE_NAME_FORMAT, databaseId, uuid)
            .replaceAll("-", "_")
            .substring(0, MAX_NAME_LENGTH);
    String watermarkIndex =
        String.format(WATERMARK_INDEX_NAME_FORMAT, databaseId, uuid)
            .replaceAll("-", "_")
            .substring(0, MAX_NAME_LENGTH);
    String createdAtIndex =
        String.format(CREATED_AT_START_TIMESTAMP_INDEX_NAME_FORMAT, databaseId, uuid)
            .replaceAll("-", "_")
            .substring(0, MAX_NAME_LENGTH);

    return new PartitionMetadataTableNames(table, watermarkIndex, createdAtIndex);
  }

  /**
   * Generates a unique name for the partition metadata indexes. Uses the given table name. The
   * watermark index will be in the form of {@code "WatermarkIdx_<databaseId>_<uuid>}. The createdAt
   * / start timestamp index will be in the form of {@code "CreatedAtIdx_<databaseId>_<uuid>}.
   *
   * @param databaseId The database id where the table will be created
   * @param table The table name to be used
   * @return the unique generated names of the partition metadata ddl
   */
  public static PartitionMetadataTableNames from(String databaseId, String table) {
    UUID uuid = UUID.randomUUID();
    String watermarkIndex =
        String.format(WATERMARK_INDEX_NAME_FORMAT, databaseId, uuid)
            .replaceAll("-", "_")
            .substring(0, MAX_NAME_LENGTH);
    String createdAtIndex =
        String.format(CREATED_AT_START_TIMESTAMP_INDEX_NAME_FORMAT, databaseId, uuid)
            .replaceAll("-", "_")
            .substring(0, MAX_NAME_LENGTH);

    return new PartitionMetadataTableNames(table, watermarkIndex, createdAtIndex);
  }

  private final String tableName;
  private final String watermarkIndexName;
  private final String createdAtIndexName;

  public PartitionMetadataTableNames(
      String tableName, String watermarkIndexName, String createdAtIndexName) {
    this.tableName = tableName;
    this.watermarkIndexName = watermarkIndexName;
    this.createdAtIndexName = createdAtIndexName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getWatermarkIndexName() {
    return watermarkIndexName;
  }

  public String getCreatedAtIndexName() {
    return createdAtIndexName;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionMetadataTableNames)) {
      return false;
    }
    PartitionMetadataTableNames that = (PartitionMetadataTableNames) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(watermarkIndexName, that.watermarkIndexName)
        && Objects.equals(createdAtIndexName, that.createdAtIndexName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, watermarkIndexName, createdAtIndexName);
  }

  @Override
  public String toString() {
    return "PartitionMetadataTableNames{"
        + "tableName='"
        + tableName
        + '\''
        + ", watermarkIndexName='"
        + watermarkIndexName
        + '\''
        + ", createdAtIndexName='"
        + createdAtIndexName
        + '\''
        + '}';
  }
}
