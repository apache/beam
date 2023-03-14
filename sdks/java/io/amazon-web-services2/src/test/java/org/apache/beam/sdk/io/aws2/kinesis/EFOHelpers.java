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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.sdk.io.aws2.kinesis.EFORecordsGenerators.eventWithOutRecords;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.common.InitialPositionInStream;

class EFOHelpers {
  static KinesisIO.Read createReadSpec() {
    return KinesisIO.read()
        .withStreamName("stream-01")
        .withInitialPositionInStream(InitialPositionInStream.LATEST);
  }

  static KinesisIOOptions createIOOptions(String... args) {
    return PipelineOptionsFactory.fromArgs(args).as(KinesisIOOptions.class);
  }

  static SubscribeToShardRequest subscribeLatest(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.LATEST).build())
        .build();
  }

  static SubscribeToShardRequest subscribeAfterSeqNumber(String shardId, String seqNumber) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(
            StartingPosition.builder()
                .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(seqNumber)
                .build())
        .build();
  }

  static SubscribeToShardRequest subscribeTrimHorizon(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.TRIM_HORIZON).build())
        .build();
  }

  static SubscribeToShardEvent[] eventsWithoutRecords(int numEvent, int startingSeqNum) {
    List<SubscribeToShardEvent> events = new ArrayList<>();
    for (int i = 0; i < numEvent; i++) {
      events.add(eventWithOutRecords(startingSeqNum));
      startingSeqNum++;
    }
    return events.toArray(new SubscribeToShardEvent[numEvent]);
  }
}
