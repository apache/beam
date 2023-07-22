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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao;

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.AppProfile.SingleClusterRoutingPolicy;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Data access object for creating and dropping the metadata table.
 *
 * <p>The metadata table will be used to keep the state of the entire Beam pipeline as well as
 * splitting and merging partitions.
 *
 * <p>Each Beam pipeline will create its own metadata table.
 */
@Internal
public class MetadataTableAdminDao {
  public static final String DEFAULT_METADATA_TABLE_NAME = "__change_stream_md_table";
  public static final String CF_INITIAL_TOKEN = "initial_continuation_token";
  public static final String CF_PARENT_PARTITIONS = "parent_partitions";
  public static final String CF_PARENT_LOW_WATERMARKS = "parent_low_watermarks";
  public static final String CF_WATERMARK = "watermark";
  public static final String CF_CONTINUATION_TOKEN = "continuation_token";
  public static final String CF_LOCK = "lock";
  public static final String CF_MISSING_PARTITIONS = "missing_partitions";
  public static final String CF_VERSION = "version";
  public static final String CF_SHOULD_DELETE = "should_delete";
  public static final String QUALIFIER_DEFAULT = "latest";
  public static final ImmutableList<String> COLUMN_FAMILIES =
      ImmutableList.of(
          CF_INITIAL_TOKEN,
          CF_PARENT_PARTITIONS,
          CF_PARENT_LOW_WATERMARKS,
          CF_WATERMARK,
          CF_CONTINUATION_TOKEN,
          CF_LOCK,
          CF_MISSING_PARTITIONS,
          CF_VERSION,
          CF_SHOULD_DELETE);
  public static final ByteString NEW_PARTITION_PREFIX = ByteString.copyFromUtf8("NewPartition#");
  public static final ByteString STREAM_PARTITION_PREFIX =
      ByteString.copyFromUtf8("StreamPartition#");
  public static final ByteString DETECT_NEW_PARTITION_SUFFIX =
      ByteString.copyFromUtf8("DetectNewPartition");

  // If change metadata table schema or how the table is used, this version should be bumped up.
  // Different versions are incompatible and needs to fixed before the pipeline can continue.
  // Otherwise, the pipeline may fail or even cause corruption in the metadata table.
  public static final int CURRENT_METADATA_TABLE_VERSION = 1;

  private final BigtableTableAdminClient tableAdminClient;
  private final BigtableInstanceAdminClient instanceAdminClient;
  private final String tableId;
  private final ByteString changeStreamNamePrefix;

  public MetadataTableAdminDao(
      BigtableTableAdminClient tableAdminClient,
      BigtableInstanceAdminClient instanceAdminClient,
      String changeStreamName,
      String tableId) {
    this.tableAdminClient = tableAdminClient;
    this.instanceAdminClient = instanceAdminClient;
    this.tableId = tableId;
    this.changeStreamNamePrefix = ByteString.copyFromUtf8(changeStreamName + "#");
  }

  /**
   * Return the prefix used to identify the rows belonging to this job.
   *
   * @return the prefix used to identify the rows belonging to this job
   */
  public ByteString getChangeStreamNamePrefix() {
    return changeStreamNamePrefix;
  }

  /**
   * Return the metadata table name.
   *
   * @return the metadata table name
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * Verify the app profile is for single cluster routing with allow single-row transactions
   * enabled. For metadata data operations, the app profile needs to be single cluster routing
   * because it requires read-after-write consistency. Also, the operations depend on single row
   * transactions operations like CheckAndMutateRow.
   *
   * @return true if the app profile is single-cluster and allows single-row transactions, otherwise
   *     false
   */
  public boolean isAppProfileSingleClusterAndTransactional(String appProfileId) {
    AppProfile appProfile =
        instanceAdminClient.getAppProfile(tableAdminClient.getInstanceId(), appProfileId);
    if (appProfile.getPolicy() instanceof SingleClusterRoutingPolicy) {
      SingleClusterRoutingPolicy routingPolicy =
          (SingleClusterRoutingPolicy) appProfile.getPolicy();
      return routingPolicy.getAllowTransactionalWrites();
    }
    return false;
  }

  /** @return true if metadata table exists, otherwise false. */
  public boolean doesMetadataTableExist() {
    return tableAdminClient.exists(tableId);
  }

  /**
   * Create the metadata table if it does not exist yet. If the table does exist, verify all the
   * column families exists, if not add those column families. This table only need to be created
   * once per instance. All change streams jobs will use this table. This table is created in the
   * same instance as the table being streamed. While we don't restrict access to the table,
   * manually editing the table can lead to inconsistent beam jobs.
   *
   * @return true if the table was successfully created, otherwise false.
   */
  public boolean createMetadataTable() {
    GCRules.GCRule gcRules = GCRules.GCRULES.maxVersions(1);

    if (tableAdminClient.exists(tableId)) {
      Table table = tableAdminClient.getTable(tableId);
      List<ColumnFamily> currentCFs = table.getColumnFamilies();
      ModifyColumnFamiliesRequest request = ModifyColumnFamiliesRequest.of(tableId);
      boolean needsNewColumnFamily = false;
      for (String targetCF : COLUMN_FAMILIES) {
        boolean exists = false;
        for (ColumnFamily currentCF : currentCFs) {
          if (targetCF.equals(currentCF.getId())) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          needsNewColumnFamily = true;
          request.addFamily(targetCF, gcRules);
        }
      }
      if (needsNewColumnFamily) {
        tableAdminClient.modifyFamilies(request);
      }
      return false;
    }

    CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);
    for (String cf : COLUMN_FAMILIES) {
      createTableRequest.addFamily(cf, gcRules);
    }

    tableAdminClient.createTable(createTableRequest);
    return true;
  }

  /**
   * Delete all the metadata rows starting with the change stream name prefix, except for detect new
   * partition row because it signals the existence of a pipeline with the change stream name.
   */
  public void cleanUpPrefix() {
    // Clean up all rows except for DNP row.
    tableAdminClient.dropRowRange(tableId, getFullNewPartitionPrefix());
    tableAdminClient.dropRowRange(tableId, getFullStreamPartitionPrefix());
  }

  /**
   * Return new partition row key prefix concatenated with change stream name.
   *
   * @return new partition row key prefix concatenated with change stream name.
   */
  private ByteString getFullNewPartitionPrefix() {
    return changeStreamNamePrefix.concat(NEW_PARTITION_PREFIX);
  }

  /**
   * Return stream partition row key prefix concatenated with change stream name.
   *
   * @return stream partition row key prefix concatenated with change stream name.
   */
  private ByteString getFullStreamPartitionPrefix() {
    return changeStreamNamePrefix.concat(STREAM_PARTITION_PREFIX);
  }
}
