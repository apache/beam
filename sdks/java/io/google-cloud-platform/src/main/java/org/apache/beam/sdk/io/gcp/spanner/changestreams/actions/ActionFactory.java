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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.actions;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;

// TODO: Add java docs
public class ActionFactory implements Serializable {

  private static final long serialVersionUID = -4060958761369602619L;
  private static DataChangeRecordAction dataChangeRecordActionInstance;
  private static HeartbeatRecordAction heartbeatRecordActionInstance;
  private static ChildPartitionsRecordAction childPartitionsRecordActionInstance;
  private static QueryChangeStreamAction queryChangeStreamActionInstance;

  public synchronized DataChangeRecordAction dataChangeRecordAction() {
    if (dataChangeRecordActionInstance == null) {
      dataChangeRecordActionInstance = new DataChangeRecordAction();
    }
    return dataChangeRecordActionInstance;
  }

  public synchronized HeartbeatRecordAction heartbeatRecordAction(ChangeStreamMetrics metrics) {
    if (heartbeatRecordActionInstance == null) {
      heartbeatRecordActionInstance = new HeartbeatRecordAction(metrics);
    }
    return heartbeatRecordActionInstance;
  }

  public synchronized ChildPartitionsRecordAction childPartitionsRecordAction(
      PartitionMetadataDao partitionMetadataDao, ChangeStreamMetrics metrics) {
    if (childPartitionsRecordActionInstance == null) {
      childPartitionsRecordActionInstance =
          new ChildPartitionsRecordAction(partitionMetadataDao, metrics);
    }
    return childPartitionsRecordActionInstance;
  }

  public synchronized QueryChangeStreamAction queryChangeStreamAction(
      ChangeStreamDao changeStreamDao,
      PartitionMetadataDao partitionMetadataDao,
      ChangeStreamRecordMapper changeStreamRecordMapper,
      DataChangeRecordAction dataChangeRecordAction,
      HeartbeatRecordAction heartbeatRecordAction,
      ChildPartitionsRecordAction childPartitionsRecordAction) {
    if (queryChangeStreamActionInstance == null) {
      queryChangeStreamActionInstance =
          new QueryChangeStreamAction(
              changeStreamDao,
              partitionMetadataDao,
              changeStreamRecordMapper,
              dataChangeRecordAction,
              heartbeatRecordAction,
              childPartitionsRecordAction);
    }
    return queryChangeStreamActionInstance;
  }
}
