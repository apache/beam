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

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Options.RpcPriority;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;

// TODO: Add java docs
public class DaoFactory implements Serializable {

  private static final long serialVersionUID = 7929063669009832487L;
  private static PartitionMetadataAdminDao partitionMetadataAdminDao;
  private static PartitionMetricsAdminDao partitionMetricsAdminDao;
  private static PartitionMetadataDao partitionMetadataDaoInstance;
  private static ChangeStreamDao changeStreamDaoInstance;

  private final SpannerConfig changeStreamSpannerConfig;
  private final SpannerConfig metadataSpannerConfig;

  private final String changeStreamName;
  private final String partitionMetadataTableName;
  private final String partitionMetricsTableName;
  private final MapperFactory mapperFactory;
  private final RpcPriority rpcPriority;
  private final String jobName;

  public DaoFactory(
      SpannerConfig changeStreamSpannerConfig,
      String changeStreamName,
      SpannerConfig metadataSpannerConfig,
      String partitionMetadataTableName,
      String partitionMetricsTableName,
      MapperFactory mapperFactory,
      RpcPriority rpcPriority,
      String jobName) {
    this.changeStreamSpannerConfig = changeStreamSpannerConfig;
    this.changeStreamName = changeStreamName;
    this.metadataSpannerConfig = metadataSpannerConfig;
    this.partitionMetadataTableName = partitionMetadataTableName;
    this.partitionMetricsTableName = partitionMetricsTableName;
    this.mapperFactory = mapperFactory;
    this.rpcPriority = rpcPriority;
    this.jobName = jobName;
  }

  public synchronized PartitionMetadataAdminDao getPartitionMetadataAdminDao() {
    if (partitionMetadataAdminDao == null) {
      DatabaseAdminClient databaseAdminClient =
          SpannerAccessor.getOrCreate(metadataSpannerConfig).getDatabaseAdminClient();
      partitionMetadataAdminDao =
          new PartitionMetadataAdminDao(
              databaseAdminClient,
              metadataSpannerConfig.getInstanceId().get(),
              metadataSpannerConfig.getDatabaseId().get(),
              partitionMetadataTableName);
    }
    return partitionMetadataAdminDao;
  }

  public synchronized PartitionMetricsAdminDao getPartitionMetricsAdminDao() {
    if (partitionMetricsAdminDao == null) {
      DatabaseAdminClient databaseAdminClient =
          SpannerAccessor.getOrCreate(metadataSpannerConfig).getDatabaseAdminClient();
      partitionMetricsAdminDao =
          new PartitionMetricsAdminDao(
              databaseAdminClient,
              metadataSpannerConfig.getInstanceId().get(),
              metadataSpannerConfig.getDatabaseId().get(),
              partitionMetricsTableName);
    }
    return partitionMetricsAdminDao;
  }

  public synchronized PartitionMetadataDao getPartitionMetadataDao() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(metadataSpannerConfig);
    if (partitionMetadataDaoInstance == null) {
      partitionMetadataDaoInstance =
          new PartitionMetadataDao(
              this.partitionMetadataTableName,
              this.partitionMetricsTableName,
              spannerAccessor.getDatabaseClient(),
              mapperFactory.partitionMetadataMapper());
    }
    return partitionMetadataDaoInstance;
  }

  public synchronized ChangeStreamDao getChangeStreamDao() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(changeStreamSpannerConfig);
    if (changeStreamDaoInstance == null) {
      changeStreamDaoInstance =
          new ChangeStreamDao(
              this.changeStreamName, spannerAccessor.getDatabaseClient(), rpcPriority, jobName);
    }
    return changeStreamDaoInstance;
  }
}
