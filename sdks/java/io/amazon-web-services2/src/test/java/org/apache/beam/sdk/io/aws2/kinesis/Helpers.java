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

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

class Helpers {
  static final int SHARD_EVENTS = 100;

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

  static List<List<Record>> testRecords(int shards, int events) {
    final Instant now = DateTime.now().toInstant();
    Function<Integer, List<Record>> dataStream =
        shard -> range(0, events).mapToObj(off -> record(now, shard, off)).collect(toList());
    return range(0, shards).boxed().map(dataStream).collect(toList());
  }

  static List<List<Record>> testAggregatedRecords(int shards, int events) {
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
