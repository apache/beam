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

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.NEW_PARTITION_PREFIX;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.STREAM_PARTITION_PREFIX;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data access object for managing the state of the metadata Bigtable table.
 *
 * <p>Metadata table is shared across many beam jobs. Each beam job uses a specific prefix to
 * identify itself which is used as the row prefix.
 */
@SuppressWarnings({"UnusedVariable", "UnusedMethod"})
public class MetadataTableDao {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTableDao.class);

  private final String tableId;
  private final ByteString changeStreamNamePrefix;

  public MetadataTableDao(String tableId, ByteString changeStreamNamePrefix) {
    this.tableId = tableId;
    this.changeStreamNamePrefix = changeStreamNamePrefix;
  }

  /** @return the prefix that is prepended to every row belonging to this beam job. */
  public ByteString getChangeStreamNamePrefix() {
    return changeStreamNamePrefix;
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

  /**
   * Return detect new partition row key concatenated with change stream name.
   *
   * @return detect new partition row key concatenated with change stream name.
   */
  private ByteString getFullDetectNewPartition() {
    return changeStreamNamePrefix.concat(DETECT_NEW_PARTITION_SUFFIX);
  }
}
