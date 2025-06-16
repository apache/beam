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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.MoveInEvent;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.MoveOutEvent;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionEndRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionEventRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionStartRecord;

public class TestProtoMapper {

  public static com.google.spanner.v1.ChangeStreamRecord recordToProto(ChangeStreamRecord record) {
    if (record instanceof PartitionStartRecord) {
      return convertPartitionStartRecordToProto((PartitionStartRecord) record);
    } else if (record instanceof PartitionEndRecord) {
      return convertPartitionEndRecordToProto((PartitionEndRecord) record);
    } else if (record instanceof PartitionEventRecord) {
      return convertPartitionEventRecordToProto((PartitionEventRecord) record);
    } else if (record instanceof HeartbeatRecord) {
      return convertHeartbeatRecordToProto((HeartbeatRecord) record);
    } else {
      throw new UnsupportedOperationException("Unimplemented mapping for " + record.getClass());
    }
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertPartitionStartRecordToProto(
      PartitionStartRecord partitionStartRecord) {
    com.google.spanner.v1.ChangeStreamRecord.PartitionStartRecord partitionStartRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.PartitionStartRecord.newBuilder()
            .setStartTimestamp(partitionStartRecord.getStartTimestamp().toProto())
            .setRecordSequence(partitionStartRecord.getRecordSequence())
            .addAllPartitionTokens(partitionStartRecord.getPartitionTokens())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setPartitionStartRecord(partitionStartRecordProto)
            .build();
    return changeStreamRecordProto;
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertPartitionEndRecordToProto(
      PartitionEndRecord partitionEndRecord) {
    com.google.spanner.v1.ChangeStreamRecord.PartitionEndRecord partitionEndRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.PartitionEndRecord.newBuilder()
            .setEndTimestamp(partitionEndRecord.getEndTimestamp().toProto())
            .setRecordSequence(partitionEndRecord.getRecordSequence())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setPartitionEndRecord(partitionEndRecordProto)
            .build();
    return changeStreamRecordProto;
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertPartitionEventRecordToProto(
      PartitionEventRecord partitionEventRecord) {
    List<com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveInEvent>
        moveInEventsProto = new ArrayList<>();
    for (MoveInEvent moveInEvent : partitionEventRecord.getMoveInEvents()) {
      com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveInEvent moveInEventProto =
          com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveInEvent.newBuilder()
              .setSourcePartitionToken(moveInEvent.getSourcePartitionToken())
              .build();
      moveInEventsProto.add(moveInEventProto);
    }
    List<com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveOutEvent>
        moveOutEventsProto = new ArrayList<>();
    for (MoveOutEvent moveOutEvent : partitionEventRecord.getMoveOutEvents()) {
      com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveOutEvent moveOutEventProto =
          com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveOutEvent.newBuilder()
              .setDestinationPartitionToken(moveOutEvent.getDestinationPartitionToken())
              .build();
      moveOutEventsProto.add(moveOutEventProto);
    }
    com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord partitionEventRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.newBuilder()
            .setCommitTimestamp(partitionEventRecord.getCommitTimestamp().toProto())
            .setRecordSequence(partitionEventRecord.getRecordSequence())
            .addAllMoveInEvents(moveInEventsProto)
            .addAllMoveOutEvents(moveOutEventsProto)
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setPartitionEventRecord(partitionEventRecordProto)
            .build();
    return changeStreamRecordProto;
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertHeartbeatRecordToProto(
      HeartbeatRecord heartbeatRecord) {
    com.google.spanner.v1.ChangeStreamRecord.HeartbeatRecord heartbeatRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.HeartbeatRecord.newBuilder()
            .setTimestamp(heartbeatRecord.getTimestamp().toProto())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setHeartbeatRecord(heartbeatRecordProto)
            .build();
    return changeStreamRecordProto;
  }
}
