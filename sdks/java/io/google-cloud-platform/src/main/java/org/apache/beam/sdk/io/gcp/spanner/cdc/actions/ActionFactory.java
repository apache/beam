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
package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.joda.time.Duration;

// TODO: Add java docs
public class ActionFactory implements Serializable {

  private static final long serialVersionUID = -4060958761369602619L;
  private static DataChangeRecordAction dataChangeRecordActionInstance;
  private static HeartbeatRecordAction heartbeatRecordActionInstance;
  private static ChildPartitionsRecordAction childPartitionsRecordActionInstance;
  private static FinishPartitionAction finishPartitionActionInstance;
  private static QueryChangeStreamAction queryChangeStreamActionInstance;
  private static WaitForChildPartitionsAction waitForChildPartitionsActionInstance;
  private static WaitForParentPartitionsAction waitForParentPartitionsActionInstance;
  private static DeletePartitionAction deletePartitionActionInstance;
  private static DonePartitionAction donePartitionActionInstance;

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized DataChangeRecordAction dataChangeRecordAction() {
    if (dataChangeRecordActionInstance == null) {
      dataChangeRecordActionInstance = new DataChangeRecordAction();
    }
    return dataChangeRecordActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized HeartbeatRecordAction heartbeatRecordAction() {
    if (heartbeatRecordActionInstance == null) {
      heartbeatRecordActionInstance = new HeartbeatRecordAction();
    }
    return heartbeatRecordActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized ChildPartitionsRecordAction childPartitionsRecordAction(
      PartitionMetadataDao partitionMetadataDao) {
    if (childPartitionsRecordActionInstance == null) {
      childPartitionsRecordActionInstance = new ChildPartitionsRecordAction(partitionMetadataDao);
    }
    return childPartitionsRecordActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized FinishPartitionAction finishPartitionAction(
      PartitionMetadataDao partitionMetadataDao) {
    if (finishPartitionActionInstance == null) {
      finishPartitionActionInstance = new FinishPartitionAction(partitionMetadataDao);
    }
    return finishPartitionActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized WaitForChildPartitionsAction waitForChildPartitionsAction(
      PartitionMetadataDao partitionMetadataDao, Duration resumeDuration) {
    if (waitForChildPartitionsActionInstance == null) {
      waitForChildPartitionsActionInstance =
          new WaitForChildPartitionsAction(partitionMetadataDao, resumeDuration);
    }
    return waitForChildPartitionsActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized QueryChangeStreamAction queryChangeStreamAction(
      ChangeStreamDao changeStreamDao,
      ChangeStreamRecordMapper changeStreamRecordMapper,
      DataChangeRecordAction dataChangeRecordAction,
      HeartbeatRecordAction heartbeatRecordAction,
      ChildPartitionsRecordAction childPartitionsRecordAction) {
    if (queryChangeStreamActionInstance == null) {
      queryChangeStreamActionInstance =
          new QueryChangeStreamAction(
              changeStreamDao,
              changeStreamRecordMapper,
              dataChangeRecordAction,
              heartbeatRecordAction,
              childPartitionsRecordAction);
    }
    return queryChangeStreamActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized WaitForParentPartitionsAction waitForParentPartitionsAction(
      PartitionMetadataDao partitionMetadataDao, Duration resumeDuration) {
    if (waitForParentPartitionsActionInstance == null) {
      waitForParentPartitionsActionInstance =
          new WaitForParentPartitionsAction(partitionMetadataDao, resumeDuration);
    }
    return waitForParentPartitionsActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized DeletePartitionAction deletePartitionAction(
      PartitionMetadataDao partitionMetadataDao) {
    if (deletePartitionActionInstance == null) {
      deletePartitionActionInstance = new DeletePartitionAction(partitionMetadataDao);
    }
    return deletePartitionActionInstance;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized DonePartitionAction donePartitionAction() {
    if (donePartitionActionInstance == null) {
      donePartitionActionInstance = new DonePartitionAction();
    }
    return donePartitionActionInstance;
  }
}
