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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DoFn responsible to initialize the metadata table and prepare it for managing the state of the
 * pipeline.
 */
@Internal
public class InitializeDoFn extends DoFn<byte[], Instant> implements Serializable {
  private static final long serialVersionUID = 1868189906451252363L;

  private static final Logger LOG = LoggerFactory.getLogger(InitializeDoFn.class);
  private final DaoFactory daoFactory;
  private final String metadataTableAppProfileId;
  private Instant startTime;

  public InitializeDoFn(
      DaoFactory daoFactory, String metadataTableAppProfileId, Instant startTime) {
    this.daoFactory = daoFactory;
    this.metadataTableAppProfileId = metadataTableAppProfileId;
    this.startTime = startTime;
  }

  @ProcessElement
  public void processElement(OutputReceiver<Instant> receiver) throws IOException {
    LOG.info(daoFactory.getStreamTableDebugString());
    LOG.info(daoFactory.getMetadataTableDebugString());
    LOG.info("ChangeStreamName: " + daoFactory.getChangeStreamName());
    if (!daoFactory
        .getMetadataTableAdminDao()
        .isAppProfileSingleClusterAndTransactional(this.metadataTableAppProfileId)) {
      LOG.error(
          "App profile id '"
              + metadataTableAppProfileId
              + "' provided to access metadata table needs to use single-cluster routing policy"
              + " and allow single-row transactions.");
      // Terminate this pipeline now.
      return;
    }
    if (daoFactory.getMetadataTableAdminDao().createMetadataTable()) {
      LOG.info("Created metadata table: " + daoFactory.getMetadataTableAdminDao().getTableId());
    } else {
      LOG.info(
          "Reusing existing metadata table: " + daoFactory.getMetadataTableAdminDao().getTableId());
    }

    daoFactory.getMetadataTableDao().writeDetectNewPartitionVersion();

    receiver.output(startTime);
  }
}
