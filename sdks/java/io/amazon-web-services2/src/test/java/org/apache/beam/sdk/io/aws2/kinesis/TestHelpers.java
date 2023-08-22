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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.joda.time.Duration.standardSeconds;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.common.InitialPositionInStream;

class TestHelpers {
  static final int SHARD_EVENTS = 100;
  private static final String STREAM = "stream-01";
  private static final String CONSUMER = "consumer-01";

  static void mockShards(KinesisClient client, int count) {
    IntFunction<Shard> shard = i -> Shard.builder().shardId(Integer.toString(i)).build();
    List<Shard> shards = range(0, count).mapToObj(shard).collect(toList());
    when(client.listShards(any(ListShardsRequest.class)))
        .thenReturn(ListShardsResponse.builder().shards(shards).build());
  }

  static void mockShardIterators(KinesisClient client, List<List<Record>> data) {
    for (int id = 0; id < data.size(); id++) {
      when(client.getShardIterator(argThat(hasShardId(id))))
          .thenReturn(GetShardIteratorResponse.builder().shardIterator(id + ":0").build());
    }
  }

  static void mockRecords(KinesisClient client, List<List<Record>> data, int limit) {
    BiFunction<List<Record>, String, GetRecordsResponse.Builder> resp =
        (recs, it) ->
            GetRecordsResponse.builder().millisBehindLatest(0L).records(recs).nextShardIterator(it);

    for (int shard = 0; shard < data.size(); shard++) {
      List<Record> records = data.get(shard);
      for (int i = 0; i < records.size(); i += limit) {
        int to = Math.max(i + limit, records.size());
        String nextIt = (to == records.size()) ? "done" : shard + ":" + to;
        when(client.getRecords(argThat(hasShardIterator(shard + ":" + i))))
            .thenReturn(resp.apply(records.subList(i, to), nextIt).build());
      }
    }
    when(client.getRecords(argThat(hasShardIterator("done"))))
        .thenReturn(resp.apply(ImmutableList.of(), "done").build());
  }

  static List<List<Record>> createRecords(int shards, int events) {
    final Instant now = DateTime.now().toInstant();
    Function<Integer, List<Record>> dataStream =
        shard -> range(0, events).mapToObj(off -> record(now, shard, off)).collect(toList());
    return range(0, shards).boxed().map(dataStream).collect(toList());
  }

  static List<List<Record>> createAggregatedRecords(int shards, int events) {
    final Instant now = DateTime.now().toInstant();
    Function<Integer, List<Record>> dataStream =
        shard -> {
          RecordsAggregator aggregator = new RecordsAggregator(1024, new org.joda.time.Instant());
          List<Record> records =
              range(0, events).mapToObj(off -> record(now, shard, off)).collect(toList());
          for (Record record : records) {
            aggregator.addRecord(record.partitionKey(), null, record.data().asByteArray());
          }
          return ImmutableList.of(recordWithCustomPayload(now, shard, 0, aggregator.toBytes()));
        };
    return range(0, shards).boxed().map(dataStream).collect(toList());
  }

  static Record record(Instant arrival, byte[] data, String seqNum) {
    return Record.builder()
        .approximateArrivalTimestamp(TimeUtil.toJava(arrival))
        .data(SdkBytes.fromByteArray(data))
        .sequenceNumber(seqNum)
        .partitionKey("foo-part-key")
        .build();
  }

  static KinesisIO.Read createReadSpec() {
    return KinesisIO.read()
        .withStreamName(STREAM)
        .withInitialPositionInStream(InitialPositionInStream.LATEST);
  }

  static KinesisIOOptions createIOOptions(String... args) {
    return PipelineOptionsFactory.fromArgs(args).as(KinesisIOOptions.class);
  }

  static SubscribeToShardRequest subscribeLatest(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN(CONSUMER)
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.LATEST).build())
        .build();
  }

  static SubscribeToShardRequest subscribeAfterSeqNumber(String shardId, String seqNumber) {
    return SubscribeToShardRequest.builder()
        .consumerARN(CONSUMER)
        .shardId(shardId)
        .startingPosition(
            StartingPosition.builder()
                .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(seqNumber)
                .build())
        .build();
  }

  static SubscribeToShardRequest subscribeAtSeqNumber(String shardId, String seqNumber) {
    return SubscribeToShardRequest.builder()
        .consumerARN(CONSUMER)
        .shardId(shardId)
        .startingPosition(
            StartingPosition.builder()
                .type(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .sequenceNumber(seqNumber)
                .build())
        .build();
  }

  static SubscribeToShardRequest subscribeTrimHorizon(String shardId) {
    return SubscribeToShardRequest.builder()
        .consumerARN(CONSUMER)
        .shardId(shardId)
        .startingPosition(StartingPosition.builder().type(ShardIteratorType.TRIM_HORIZON).build())
        .build();
  }

  static SubscribeToShardRequest subscribeAtTs(String shardId, Instant ts) {
    return SubscribeToShardRequest.builder()
        .consumerARN(CONSUMER)
        .shardId(shardId)
        .startingPosition(
            StartingPosition.builder()
                .timestamp(TimeUtil.toJava(ts))
                .type(ShardIteratorType.AT_TIMESTAMP)
                .build())
        .build();
  }

  static SubscribeToShardEvent[] eventsWithoutRecords(int startingSeqNum, int numEvent) {
    List<SubscribeToShardEvent> events = new ArrayList<>();
    for (int i = 0; i < numEvent; i++) {
      events.add(eventWithOutRecords(startingSeqNum));
      startingSeqNum++;
    }
    return events.toArray(new SubscribeToShardEvent[numEvent]);
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

  static SubscribeToShardEvent reShardEvent(
      List<String> parentShardsIds, List<String> childShardsIds) {
    List<ChildShard> childShards =
        childShardsIds.stream()
            .map(s -> ChildShard.builder().shardId(s).parentShards(parentShardsIds).build())
            .collect(Collectors.toList());

    return SubscribeToShardEvent.builder()
        .continuationSequenceNumber(null)
        .childShards(childShards)
        .build();
  }

  static SubscribeToShardEvent reShardEventWithRecords(
      int startSeqNumber,
      int numRecords,
      List<String> parentShardsIds,
      List<String> childShardsIds) {
    List<Record> records = new ArrayList<>();
    for (int i = startSeqNumber; i < startSeqNumber + numRecords; i++) {
      records.add(record(String.valueOf(i)));
    }

    List<ChildShard> childShards =
        childShardsIds.stream()
            .map(s -> ChildShard.builder().shardId(s).parentShards(parentShardsIds).build())
            .collect(Collectors.toList());

    return SubscribeToShardEvent.builder()
        .records(records)
        .continuationSequenceNumber(null)
        .childShards(childShards)
        .build();
  }

  static SubscribeToShardEvent[] eventsWithRecords(int startSeqNumber, int numRecords) {
    List<SubscribeToShardEvent> events = new ArrayList<>();
    for (int i = startSeqNumber; i < startSeqNumber + numRecords; i++) {
      events.add(eventWithRecords(ImmutableList.of(record(String.valueOf(i)))));
    }
    return events.toArray(new SubscribeToShardEvent[0]);
  }

  static SubscribeToShardEvent eventWithRecords(int startSeqNumber, int numRecords) {
    List<Record> records = new ArrayList<>();
    for (int i = startSeqNumber; i < startSeqNumber + numRecords; i++) {
      records.add(record(String.valueOf(i)));
    }
    return eventWithRecords(records);
  }

  static SubscribeToShardEvent eventWithAggRecords(int startSeqNumber, int numRecords) {
    RecordsAggregator aggregator = new RecordsAggregator(1024, new org.joda.time.Instant());
    for (int i = startSeqNumber; i < startSeqNumber + numRecords; i++) {
      aggregator.addRecord("foo", null, String.valueOf(i).getBytes(UTF_8));
    }
    Record record = record(Instant.now(), aggregator.toBytes(), String.valueOf(startSeqNumber));
    return SubscribeToShardEvent.builder()
        .continuationSequenceNumber(String.valueOf(startSeqNumber))
        .records(record)
        .build();
  }

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

  static List<Record> recordWithMinutesAgo(int minutesAgo) {
    return ImmutableList.of(
        record(Instant.now().minus(Duration.standardMinutes(minutesAgo)), new byte[] {}, "0"));
  }

  private static Record record(String seqNum) {
    return record(Instant.now(), seqNum.getBytes(UTF_8), seqNum);
  }

  private static Record record(Instant now, int shard, int offset) {
    String seqNum = Integer.toString(shard * SHARD_EVENTS + offset);
    return record(now.plus(standardSeconds(offset)), seqNum.getBytes(UTF_8), seqNum);
  }

  private static Record recordWithCustomPayload(
      Instant now, int shard, int offset, byte[] payload) {
    String seqNum = Integer.toString(shard * SHARD_EVENTS + offset);
    return record(now.plus(standardSeconds(offset)), payload, seqNum);
  }

  private static ArgumentMatcher<GetShardIteratorRequest> hasShardId(int id) {
    return req -> req != null && req.shardId().equals("" + id);
  }

  private static ArgumentMatcher<GetRecordsRequest> hasShardIterator(String id) {
    return req -> req != null && req.shardIterator().equals(id);
  }
}
