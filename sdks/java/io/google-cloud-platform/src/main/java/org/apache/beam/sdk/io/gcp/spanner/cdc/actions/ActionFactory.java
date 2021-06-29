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
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.joda.time.Duration;

// TODO: Add java docs
public class ActionFactory implements Serializable {

  private static final long serialVersionUID = -4060958761369602619L;
  private static DataChangeRecordAction DATA_CHANGE_RECORD_ACTION_INSTANCE;
  private static HeartbeatRecordAction HEARTBEAT_RECORD_ACTION_INSTANCE;
  private static ChildPartitionsRecordAction CHILD_PARTITIONS_RECORD_ACTION_INSTANCE;
  private static FinishPartitionAction FINISH_PARTITION_ACTION_INSTANCE;
  private static WaitForChildPartitionsAction WAIT_FOR_CHILD_PARTITIONS_ACTION_INSTANCE;
  private static WaitForParentPartitionsAction WAIT_FOR_PARENT_PARTITIONS_ACTION_INSTANCE;
  private static DeletePartitionAction DELETE_PARTITION_ACTION_INSTANCE;
  private static DonePartitionAction DONE_PARTITION_ACTION_INSTANCE;

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized DataChangeRecordAction dataChangeRecordAction() {
    if (DATA_CHANGE_RECORD_ACTION_INSTANCE == null) {
      DATA_CHANGE_RECORD_ACTION_INSTANCE = new DataChangeRecordAction();
    }
    return DATA_CHANGE_RECORD_ACTION_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized HeartbeatRecordAction heartbeatRecordAction() {
    if (HEARTBEAT_RECORD_ACTION_INSTANCE == null) {
      HEARTBEAT_RECORD_ACTION_INSTANCE = new HeartbeatRecordAction();
    }
    return HEARTBEAT_RECORD_ACTION_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized ChildPartitionsRecordAction childPartitionsRecordAction(
      PartitionMetadataDao partitionMetadataDao,
      WaitForChildPartitionsAction waitForChildPartitionsAction) {
    if (CHILD_PARTITIONS_RECORD_ACTION_INSTANCE == null) {
      CHILD_PARTITIONS_RECORD_ACTION_INSTANCE =
          new ChildPartitionsRecordAction(partitionMetadataDao, waitForChildPartitionsAction);
    }
    return CHILD_PARTITIONS_RECORD_ACTION_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized FinishPartitionAction finishPartitionAction(
      PartitionMetadataDao partitionMetadataDao) {
    if (FINISH_PARTITION_ACTION_INSTANCE == null) {
      FINISH_PARTITION_ACTION_INSTANCE = new FinishPartitionAction(partitionMetadataDao);
    }
    return FINISH_PARTITION_ACTION_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized WaitForChildPartitionsAction waitForChildPartitionsAction(
      PartitionMetadataDao partitionMetadataDao, Duration resumeDuration) {
    if (WAIT_FOR_CHILD_PARTITIONS_ACTION_INSTANCE == null) {
      WAIT_FOR_CHILD_PARTITIONS_ACTION_INSTANCE =
          new WaitForChildPartitionsAction(partitionMetadataDao, resumeDuration);
    }
    return WAIT_FOR_CHILD_PARTITIONS_ACTION_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized WaitForParentPartitionsAction waitForParentPartitionsAction(
      PartitionMetadataDao partitionMetadataDao, Duration resumeDuration) {
    if (WAIT_FOR_PARENT_PARTITIONS_ACTION_INSTANCE == null) {
      WAIT_FOR_PARENT_PARTITIONS_ACTION_INSTANCE =
          new WaitForParentPartitionsAction(partitionMetadataDao, resumeDuration);
    }
    return WAIT_FOR_PARENT_PARTITIONS_ACTION_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized DeletePartitionAction deletePartitionAction(
      PartitionMetadataDao partitionMetadataDao) {
    if (DELETE_PARTITION_ACTION_INSTANCE == null) {
      DELETE_PARTITION_ACTION_INSTANCE = new DeletePartitionAction(partitionMetadataDao);
    }
    return DELETE_PARTITION_ACTION_INSTANCE;
  }

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized DonePartitionAction donePartitionAction() {
    if (DONE_PARTITION_ACTION_INSTANCE == null) {
      DONE_PARTITION_ACTION_INSTANCE = new DonePartitionAction();
    }
    return DONE_PARTITION_ACTION_INSTANCE;
  }
}
