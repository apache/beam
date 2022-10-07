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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class RecordsShardEvent extends ShardEventAbs {
  private final List<ExtendedKinesisRecord> records;

  private RecordsShardEvent(String streamName, String shardId, SubscribeToShardEvent event) {
    super(shardId, ShardEventType.RECORDS);

    if (event.hasRecords() && !event.records().isEmpty()) {
      AggregatorUtil au = new AggregatorUtil();
      List<KinesisClientRecord> deAggregated =
          au.deaggregate(
              event.records().stream()
                  .map(KinesisClientRecord::fromRecord)
                  .collect(Collectors.toList()));
      this.records =
          deAggregated.stream()
              .map(r -> ExtendedKinesisRecord.fromRecord(new KinesisRecord(r, streamName, shardId)))
              .collect(Collectors.toList());
    } else {
      ExtendedKinesisRecord dummyRecord =
          ExtendedKinesisRecord.fromEmpty(shardId, event.continuationSequenceNumber());

      this.records = Collections.singletonList(dummyRecord);
    }
  }

  public static RecordsShardEvent fromNext(
      String streamName, String shardId, SubscribeToShardEvent event) {
    return new RecordsShardEvent(streamName, shardId, event);
  }

  public List<ExtendedKinesisRecord> getRecords() {
    return records;
  }
}
