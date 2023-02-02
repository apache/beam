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

import com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Data access object for creating and dropping the metadata table.
 *
 * <p>The metadata table will be used to keep the state of the entire Dataflow job as well as
 * splitting and merging partitions.
 *
 * <p>Each Dataflow job will create its own metadata table.
 */
@SuppressWarnings({"UnusedVariable", "UnusedMethod"})
public class MetadataTableAdminDao {
  public static final String DEFAULT_METADATA_TABLE_NAME = "__change_stream_md_table";
  public static final String CF_INITIAL_TOKEN = "initial_continuation_token";
  public static final String CF_PARENT_PARTITIONS = "parent_partitions";
  public static final String CF_PARENT_LOW_WATERMARKS = "parent_low_watermarks";
  public static final String CF_WATERMARK = "watermark";
  public static final String CF_CONTINUATION_TOKEN = "continuation_token";
  public static final String CF_LOCK = "lock";
  public static final String CF_MISSING_PARTITIONS = "missing_partitions";
  public static final String QUALIFIER_DEFAULT = "latest";
  public static final ImmutableList<String> COLUMN_FAMILIES =
      ImmutableList.of(
          CF_INITIAL_TOKEN,
          CF_PARENT_PARTITIONS,
          CF_PARENT_LOW_WATERMARKS,
          CF_WATERMARK,
          CF_CONTINUATION_TOKEN,
          CF_LOCK,
          CF_MISSING_PARTITIONS);
  public static final ByteString NEW_PARTITION_PREFIX = ByteString.copyFromUtf8("NewPartition#");
  public static final ByteString STREAM_PARTITION_PREFIX =
      ByteString.copyFromUtf8("StreamPartition#");
  public static final ByteString DETECT_NEW_PARTITION_SUFFIX =
      ByteString.copyFromUtf8("DetectNewPartition");

  private final String tableId;
  private final ByteString changeStreamNamePrefix;

  public MetadataTableAdminDao(String changeStreamName, String tableId) {
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
}
