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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Config;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.ShardSubscribersPoolImpl;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.common.InitialPositionInStream;

public class Helpers {
  public static KinesisIO.Read createReadSpec() {
    return KinesisIO.read()
        .withStreamName("stream-01")
        .withConsumerArn("consumer-01")
        .withInitialPositionInStream(InitialPositionInStream.LATEST);
  }

  public static Config createConfig() {
    return new Config(
        "stream-01",
        "consumer-01",
        new StartingPoint(InitialPositionInStream.LATEST),
        Optional.absent(),
        1_000L,
        0L,
        0L);
  }

  public static List<KinesisRecord> waitForRecords(ShardSubscribersPoolImpl pool, int expectedCnt)
      throws IOException {
    List<KinesisRecord> records = new ArrayList<>();
    int maxAttempts = expectedCnt * 4;
    int i = 0;
    while (i < maxAttempts) {
      CustomOptional<KinesisRecord> r = pool.nextRecord();
      if (r.isPresent()) {
        records.add(r.get());
      }
      i++;
    }
    return records;
  }

  public static SubscribeToShardRequest subscribeLatest(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.LATEST).build())
        .build();
  }

  public static SubscribeToShardRequest subscribeSeqNumber(String shardId, String seqNumber) {
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

  public static SubscribeToShardRequest subscribeTrimHorizon(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN("consumer-01")
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.TRIM_HORIZON).build())
        .build();
  }
}
