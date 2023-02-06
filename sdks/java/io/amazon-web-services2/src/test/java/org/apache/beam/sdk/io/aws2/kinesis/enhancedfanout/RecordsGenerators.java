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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

class RecordsGenerators {
  static Record createRecord(Integer sequenceNumber) {
    return Record.builder()
        .partitionKey("foo")
        .approximateArrivalTimestamp(Instant.now())
        .sequenceNumber(String.valueOf(sequenceNumber))
        .data(SdkBytes.fromByteArray(sequenceNumber.toString().getBytes(UTF_8)))
        .build();
  }

  // TODO: ugly
  static SubscribeToShardEvent eventWithRecords(List<Record> recordList) {
    String lastSeqNum = "0";
    for (Record r : recordList) {
      lastSeqNum = r.sequenceNumber();
    }
    return SubscribeToShardEvent.builder()
        .millisBehindLatest(0L)
        .records(recordList)
        .continuationSequenceNumber(lastSeqNum)
        .build();
  }

  static SubscribeToShardEvent eventWithOutRecords(int sequenceNumber) {
    return SubscribeToShardEvent.builder()
        .millisBehindLatest(0L)
        .continuationSequenceNumber(String.valueOf(sequenceNumber))
        .build();
  }

  static SubscribeToShardEvent eventWithRecords(int numRecords) {
    return eventWithRecords(0, numRecords);
  }

  static SubscribeToShardEvent eventWithRecords(int startSeqNumber, int numRecords) {
    List<Record> records = new ArrayList<>();
    for (int i = startSeqNumber; i < startSeqNumber + numRecords; i++) {
      records.add(createRecord(i));
    }
    return eventWithRecords(records);
  }
}
