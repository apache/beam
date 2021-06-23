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
package org.apache.beam.sdk.io.gcp.spanner.cdc.dao;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

// TODO: Add java docs
public class DaoFactory implements Serializable {

  private static final long serialVersionUID = 7929063669009832487L;
  private static PartitionMetadataDao PARTITION_METADATA_DAO_INSTANCE;
  private static ChangeStreamDao CHANGE_STREAM_DAO_INSTANCE;

  private final String changeStreamName;
  private final String partitionMetadataTableName;

  public DaoFactory(String changeStreamName, String partitionMetadataTableName) {
    this.changeStreamName = changeStreamName;
    this.partitionMetadataTableName = partitionMetadataTableName;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized PartitionMetadataDao partitionMetadataDaoFrom(SpannerConfig spannerConfig) {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    if (PARTITION_METADATA_DAO_INSTANCE == null) {
      PARTITION_METADATA_DAO_INSTANCE =
          new PartitionMetadataDao(
              this.partitionMetadataTableName, spannerAccessor.getDatabaseClient());
    }
    return PARTITION_METADATA_DAO_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized ChangeStreamDao changeStreamDaoFrom(SpannerConfig spannerConfig) {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    if (CHANGE_STREAM_DAO_INSTANCE == null) {
      CHANGE_STREAM_DAO_INSTANCE =
          new ChangeStreamDao(this.changeStreamName, spannerAccessor.getDatabaseClient());
    }
    return CHANGE_STREAM_DAO_INSTANCE;
  }
}
