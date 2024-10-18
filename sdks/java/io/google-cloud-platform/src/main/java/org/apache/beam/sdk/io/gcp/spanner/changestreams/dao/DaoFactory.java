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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options.RpcPriority;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/**
 * Factory class to create data access objects to perform change stream queries and access the
 * metadata tables. The instances created are all singletons.
 */
// transient fields are un-initialized, because we start them during the first fetch call (with the
// singleton pattern)
// nullness checks for metadata instance and database are handled in the constructor
@SuppressWarnings({"initialization.fields.uninitialized", "nullness"})
public class DaoFactory implements Serializable {

  private static final long serialVersionUID = 7929063669009832487L;

  private transient PartitionMetadataAdminDao partitionMetadataAdminDao;
  private transient PartitionMetadataDao partitionMetadataDaoInstance;
  private transient ChangeStreamDao changeStreamDaoInstance;

  private final SpannerConfig changeStreamSpannerConfig;
  private final SpannerConfig metadataSpannerConfig;

  private final String changeStreamName;
  private final PartitionMetadataTableNames partitionMetadataTableNames;
  private final RpcPriority rpcPriority;
  private final String jobName;
  private final Dialect spannerChangeStreamDatabaseDialect;
  private final Dialect metadataDatabaseDialect;

  /**
   * Constructs a {@link DaoFactory} with the configuration to be used for the underlying instances.
   *
   * @param changeStreamSpannerConfig the configuration for the change streams DAO
   * @param changeStreamName the name of the change stream for the change streams DAO
   * @param metadataSpannerConfig the metadata tables configuration
   * @param partitionMetadataTableNames the names of the partition metadata ddl objects
   * @param rpcPriority the priority of the requests made by the DAO queries
   * @param jobName the name of the running job
   */
  public DaoFactory(
      SpannerConfig changeStreamSpannerConfig,
      String changeStreamName,
      SpannerConfig metadataSpannerConfig,
      PartitionMetadataTableNames partitionMetadataTableNames,
      RpcPriority rpcPriority,
      String jobName,
      Dialect spannerChangeStreamDatabaseDialect,
      Dialect metadataDatabaseDialect) {
    if (metadataSpannerConfig.getInstanceId() == null) {
      throw new IllegalArgumentException("Metadata instance can not be null");
    }
    if (metadataSpannerConfig.getDatabaseId() == null) {
      throw new IllegalArgumentException("Metadata database can not be null");
    }
    this.changeStreamSpannerConfig = changeStreamSpannerConfig;
    this.changeStreamName = changeStreamName;
    this.metadataSpannerConfig = metadataSpannerConfig;
    this.partitionMetadataTableNames = partitionMetadataTableNames;
    this.rpcPriority = rpcPriority;
    this.jobName = jobName;
    this.spannerChangeStreamDatabaseDialect = spannerChangeStreamDatabaseDialect;
    this.metadataDatabaseDialect = metadataDatabaseDialect;
  }

  /**
   * Creates and returns a singleton DAO instance for admin operations over the partition metadata
   * table.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link PartitionMetadataDao}
   */
  public synchronized PartitionMetadataAdminDao getPartitionMetadataAdminDao() {
    if (partitionMetadataAdminDao == null) {
      DatabaseAdminClient databaseAdminClient =
          SpannerAccessor.getOrCreate(metadataSpannerConfig).getDatabaseAdminClient();
      partitionMetadataAdminDao =
          new PartitionMetadataAdminDao(
              databaseAdminClient,
              metadataSpannerConfig.getInstanceId().get(),
              metadataSpannerConfig.getDatabaseId().get(),
              partitionMetadataTableNames,
              this.metadataDatabaseDialect);
    }
    return partitionMetadataAdminDao;
  }

  /**
   * Creates and returns a singleton DAO instance for accessing the partition metadata table.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link PartitionMetadataDao}
   */
  public synchronized PartitionMetadataDao getPartitionMetadataDao() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(metadataSpannerConfig);
    if (partitionMetadataDaoInstance == null) {
      partitionMetadataDaoInstance =
          new PartitionMetadataDao(
              this.partitionMetadataTableNames.getTableName(),
              spannerAccessor.getDatabaseClient(),
              this.metadataDatabaseDialect);
    }
    return partitionMetadataDaoInstance;
  }

  /**
   * Creates and returns a singleton DAO instance for querying a partition change stream.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link ChangeStreamDao}
   */
  public synchronized ChangeStreamDao getChangeStreamDao() {
    final SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(changeStreamSpannerConfig);
    if (changeStreamDaoInstance == null) {
      changeStreamDaoInstance =
          new ChangeStreamDao(
              this.changeStreamName,
              spannerAccessor.getDatabaseClient(),
              rpcPriority,
              jobName,
              this.spannerChangeStreamDatabaseDialect);
    }
    return changeStreamDaoInstance;
  }
}
