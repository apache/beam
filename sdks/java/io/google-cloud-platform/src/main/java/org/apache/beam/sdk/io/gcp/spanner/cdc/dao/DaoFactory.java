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

import com.google.cloud.spanner.DatabaseAdminClient;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

// TODO: Add java docs
public class DaoFactory implements Serializable {

  private static final long serialVersionUID = 7929063669009832487L;
  private static PartitionMetadataAdminDao partitionMetadataAdminDao;
  private static PartitionMetadataDao partitionMetadataDaoInstance;
  private static ChangeStreamDao changeStreamDaoInstance;

  private final SpannerConfig changeStreamSpannerConfig;
  private final SpannerConfig partitionMetadataSpannerConfig;

  private final String changeStreamName;
  private final String partitionMetadataTableName;

  public DaoFactory(
      SpannerConfig changeStreamSpannerConfig,
      String changeStreamName,
      SpannerConfig partitionMetadataSpannerConfig,
      String partitionMetadataTableName) {
    this.changeStreamSpannerConfig = changeStreamSpannerConfig;
    this.changeStreamName = changeStreamName;
    this.partitionMetadataSpannerConfig = partitionMetadataSpannerConfig;
    this.partitionMetadataTableName = partitionMetadataTableName;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized PartitionMetadataAdminDao getPartitionMetadataAdminDao() {
    if (partitionMetadataAdminDao == null) {
      DatabaseAdminClient databaseAdminClient =
          SpannerAccessor.getOrCreate(partitionMetadataSpannerConfig).getDatabaseAdminClient();
      partitionMetadataAdminDao =
          new PartitionMetadataAdminDao(
              databaseAdminClient,
              partitionMetadataSpannerConfig.getInstanceId().get(),
              partitionMetadataSpannerConfig.getDatabaseId().get(),
              partitionMetadataTableName);
    }
    return partitionMetadataAdminDao;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized PartitionMetadataDao getPartitionMetadataDao() {
    final SpannerAccessor spannerAccessor =
        SpannerAccessor.getOrCreate(partitionMetadataSpannerConfig);
    if (partitionMetadataDaoInstance == null) {
      partitionMetadataDaoInstance =
          new PartitionMetadataDao(
              this.partitionMetadataTableName, spannerAccessor.getDatabaseClient());
    }
    return partitionMetadataDaoInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized ChangeStreamDao getChangeStreamDao() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(changeStreamSpannerConfig);
    if (changeStreamDaoInstance == null) {
      changeStreamDaoInstance =
          new ChangeStreamDao(this.changeStreamName, spannerAccessor.getDatabaseClient());
    }
    return changeStreamDaoInstance;
  }
}
