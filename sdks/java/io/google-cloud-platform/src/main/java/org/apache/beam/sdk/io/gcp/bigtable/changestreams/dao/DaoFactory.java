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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableConfig;
import org.joda.time.Duration;

// Allows transient fields to be intialized later
@SuppressWarnings("initialization.fields.uninitialized")
@Internal
public class DaoFactory implements Serializable, AutoCloseable {
  private static final long serialVersionUID = -3423959768580600281L;

  private transient ChangeStreamDao changeStreamDao;
  private transient MetadataTableAdminDao metadataTableAdminDao;
  private transient MetadataTableDao metadataTableDao;

  private final BigtableConfig changeStreamConfig;
  private final BigtableConfig metadataTableConfig;

  private final String tableId;

  private final String metadataTableId;
  private final String changeStreamName;

  private final Duration readChangeStreamTimeout;

  public DaoFactory(
      BigtableConfig changeStreamConfig,
      BigtableConfig metadataTableConfig,
      String tableId,
      String metadataTableId,
      String changeStreamName,
      Duration readChangeStreamTimeout) {
    this.changeStreamConfig = changeStreamConfig;
    this.metadataTableConfig = metadataTableConfig;
    this.changeStreamName = changeStreamName;
    this.tableId = tableId;
    this.metadataTableId = metadataTableId;
    this.readChangeStreamTimeout = readChangeStreamTimeout;
  }

  @Override
  public void close() {
    try {
      if (metadataTableAdminDao != null || metadataTableDao != null) {
        BigtableChangeStreamAccessor.getOrCreate(metadataTableConfig).close();
      }
      if (changeStreamDao != null) {
        BigtableChangeStreamAccessor.getOrCreate(changeStreamConfig).close();
      }
    } catch (Exception ignored) {
    }
  }

  public String getChangeStreamName() {
    return changeStreamName;
  }

  public String getStreamTableDebugString() {
    return String.format(
        "Stream Table:\n"
            + "Project ID: %s\n"
            + "Instance ID: %s\n"
            + "Table Id: %s\n"
            + "App Profile Id: %s",
        this.changeStreamConfig.getProjectId(),
        this.changeStreamConfig.getInstanceId(),
        this.tableId,
        this.changeStreamConfig.getAppProfileId());
  }

  public String getMetadataTableDebugString() {
    return String.format(
        "Metadata Table:\n"
            + "Project ID: %s\n"
            + "Instance ID: %s\n"
            + "Table Id: %s\n"
            + "App Profile Id: %s",
        this.metadataTableConfig.getProjectId(),
        this.metadataTableConfig.getInstanceId(),
        this.metadataTableId,
        this.metadataTableConfig.getAppProfileId());
  }

  public synchronized ChangeStreamDao getChangeStreamDao() throws IOException {
    if (changeStreamDao == null) {
      checkArgumentNotNull(changeStreamConfig.getProjectId());
      checkArgumentNotNull(changeStreamConfig.getInstanceId());
      checkArgumentNotNull(changeStreamConfig.getAppProfileId());
      BigtableChangeStreamAccessor.setReadChangeStreamTimeout(
          org.threeten.bp.Duration.ofMillis(readChangeStreamTimeout.getMillis()));
      BigtableDataClient dataClient =
          BigtableChangeStreamAccessor.getOrCreate(changeStreamConfig).getDataClient();
      changeStreamDao = new ChangeStreamDao(dataClient, this.tableId);
    }
    return changeStreamDao;
  }

  public synchronized MetadataTableDao getMetadataTableDao() throws IOException {
    if (metadataTableDao == null) {
      checkArgumentNotNull(metadataTableConfig.getProjectId());
      checkArgumentNotNull(metadataTableConfig.getInstanceId());
      checkArgumentNotNull(this.metadataTableId);
      checkArgumentNotNull(metadataTableConfig.getAppProfileId());
      BigtableDataClient dataClient =
          BigtableChangeStreamAccessor.getOrCreate(metadataTableConfig).getDataClient();
      metadataTableDao =
          new MetadataTableDao(
              dataClient,
              getMetadataTableAdminDao().getTableId(),
              getMetadataTableAdminDao().getChangeStreamNamePrefix());
    }
    return metadataTableDao;
  }

  public synchronized MetadataTableAdminDao getMetadataTableAdminDao() throws IOException {
    if (metadataTableAdminDao == null) {
      checkArgumentNotNull(metadataTableConfig.getProjectId());
      checkArgumentNotNull(metadataTableConfig.getInstanceId());
      checkArgumentNotNull(this.metadataTableId);
      checkArgumentNotNull(metadataTableConfig.getAppProfileId());
      BigtableTableAdminClient tableAdminClient =
          BigtableChangeStreamAccessor.getOrCreate(metadataTableConfig).getTableAdminClient();
      BigtableInstanceAdminClient instanceAdminClient =
          BigtableChangeStreamAccessor.getOrCreate(metadataTableConfig).getInstanceAdminClient();
      metadataTableAdminDao =
          new MetadataTableAdminDao(
              tableAdminClient, instanceAdminClient, changeStreamName, this.metadataTableId);
    }
    return metadataTableAdminDao;
  }
}
