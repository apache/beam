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

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableConfig;

// Allows transient fields to be intialized later
@SuppressWarnings("initialization.fields.uninitialized")
public class DaoFactory implements Serializable {
  private static final long serialVersionUID = 3732208768248394205L;

  private transient ChangeStreamDao changeStreamDao;
  private transient MetadataTableAdminDao metadataTableAdminDao;
  private transient MetadataTableDao metadataTableDao;

  private final BigtableConfig changeStreamConfig;
  private final BigtableConfig metadataTableConfig;
  private final String changeStreamName;

  public DaoFactory(
      BigtableConfig changeStreamConfig,
      BigtableConfig metadataTableConfig,
      String changeStreamName) {
    this.changeStreamConfig = changeStreamConfig;
    this.metadataTableConfig = metadataTableConfig;
    this.changeStreamName = changeStreamName;
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
        this.changeStreamConfig.getTableId(),
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
        this.metadataTableConfig.getTableId(),
        this.metadataTableConfig.getAppProfileId());
  }

  public synchronized ChangeStreamDao getChangeStreamDao() throws IOException {
    if (changeStreamDao == null) {
      checkArgumentNotNull(changeStreamConfig.getProjectId());
      checkArgumentNotNull(changeStreamConfig.getInstanceId());
      String tableId = checkArgumentNotNull(changeStreamConfig.getTableId()).get();
      checkArgumentNotNull(changeStreamConfig.getAppProfileId());
      changeStreamDao = new ChangeStreamDao(tableId);
    }
    return changeStreamDao;
  }

  public synchronized MetadataTableDao getMetadataTableDao() throws IOException {
    if (metadataTableDao == null) {
      checkArgumentNotNull(metadataTableConfig.getProjectId());
      checkArgumentNotNull(metadataTableConfig.getInstanceId());
      checkArgumentNotNull(metadataTableConfig.getTableId());
      checkArgumentNotNull(metadataTableConfig.getAppProfileId());
      metadataTableDao =
          new MetadataTableDao(
              getMetadataTableAdminDao().getTableId(),
              getMetadataTableAdminDao().getChangeStreamNamePrefix());
    }
    return metadataTableDao;
  }

  public synchronized MetadataTableAdminDao getMetadataTableAdminDao() throws IOException {
    if (metadataTableAdminDao == null) {
      checkArgumentNotNull(metadataTableConfig.getProjectId());
      checkArgumentNotNull(metadataTableConfig.getInstanceId());
      String tableId = checkArgumentNotNull(metadataTableConfig.getTableId()).get();
      checkArgumentNotNull(metadataTableConfig.getAppProfileId());
      metadataTableAdminDao = new MetadataTableAdminDao(changeStreamName, tableId);
    }
    return metadataTableAdminDao;
  }
}
