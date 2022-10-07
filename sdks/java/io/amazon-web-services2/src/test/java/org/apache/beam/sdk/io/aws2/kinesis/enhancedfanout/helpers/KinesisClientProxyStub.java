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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.RecordsGenerators.createRecord;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.AsyncClientProxy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public class KinesisClientProxyStub implements AsyncClientProxy {
  private final KinesisClientStubConfig config;
  private final BlockingQueue<SubscribeToShardRequest> subscribeRequestsCollector;
  private final AtomicInteger seqNumber;
  private final AtomicInteger subscriptionsCountdown;
  private final Function<KinesisClientStubShardState, Void> eventsSubmitter;

  KinesisClientProxyStub(
      KinesisClientStubConfig config,
      AtomicInteger subscriptionsCountdown,
      AtomicInteger seqNumber,
      BlockingQueue<SubscribeToShardRequest> subscribeRequestsCollector,
      Function<KinesisClientStubShardState, Void> eventsSubmitter) {
    this.config = config;
    this.seqNumber = seqNumber;
    this.subscriptionsCountdown = subscriptionsCountdown;
    this.subscribeRequestsCollector = subscribeRequestsCollector;
    this.eventsSubmitter = eventsSubmitter;
  }

  @Override
  public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest) {
    if (listShardsRequest.shardFilter().shardId() == null) {
      return CompletableFuture.completedFuture(
          ListShardsResponse.builder()
              .shards(buildShards(0, config.getInitialShardsCnt()))
              .build());
    } else {
      return CompletableFuture.completedFuture(
          ListShardsResponse.builder()
              .shards(buildShards(config.getInitialShardsCnt(), config.getFinalShardId() + 1))
              .build());
    }
  }

  @Override
  public CompletableFuture<Void> subscribeToShard(
      SubscribeToShardRequest request, SubscribeToShardResponseHandler responseHandler) {
    subscribeRequestsCollector.add(request);

    return CompletableFuture.supplyAsync(
        () ->
            buildEventsSupplier(
                request.shardId(),
                config.getRecordsPerSubscriptionPerShardToSend(),
                seqNumber,
                subscriptionsCountdown,
                responseHandler,
                eventsSubmitter));
  }

  @Override
  public void close() throws Exception {}

  public List<SubscribeToShardRequest> subscribeRequestsSeen() {
    return new ArrayList<>(subscribeRequestsCollector);
  }

  private static List<Shard> buildShards(int startId, int endId) {
    return IntStream.range(startId, endId)
        .mapToObj(
            i ->
                Shard.builder()
                    .shardId(String.format("shard-%03d", i))
                    .sequenceNumberRange(
                        SequenceNumberRange.builder()
                            .startingSequenceNumber(String.format("%03d", i))
                            .build())
                    .build())
        .collect(Collectors.toList());
  }

  private static Void buildEventsSupplier(
      final String shardId,
      final Integer recordsPerShardToSend,
      final AtomicInteger globalSeqNumber,
      final AtomicInteger subscriptionsCountdown,
      final SubscribeToShardResponseHandler responseHandler,
      final Function<KinesisClientStubShardState, Void> eventsSubmitter) {
    responseHandler.responseReceived(SubscribeToShardResponse.builder().build());
    responseHandler.onEventStream(
        subscriber ->
            attachSubscriber(
                shardId,
                globalSeqNumber,
                recordsPerShardToSend,
                subscriptionsCountdown,
                subscriber,
                eventsSubmitter));

    return null;
  }

  private static void attachSubscriber(
      String shardId,
      AtomicInteger seqNumber,
      Integer recordsPerShardToSend,
      AtomicInteger subscriptionsCountdown,
      Subscriber<? super SubscribeToShardEventStream> subscriber,
      Function<KinesisClientStubShardState, Void> eventsSubmitter) {
    List<SubscribeToShardEvent> eventsToSend;
    if (subscriptionsCountdown.getAndDecrement() > 0) {
      eventsToSend = generateEvents(recordsPerShardToSend, seqNumber);
    } else {
      eventsToSend =
          ImmutableList.of(
              SubscribeToShardEvent.builder()
                  .millisBehindLatest(0L)
                  .continuationSequenceNumber(String.valueOf(seqNumber.incrementAndGet()))
                  .build());
    }

    Subscription subscription = mock(Subscription.class);
    Iterator<SubscribeToShardEvent> iterator = eventsToSend.iterator();
    KinesisClientStubShardState state =
        new KinesisClientStubShardState(shardId, subscriber, iterator);
    doAnswer(a -> eventsSubmitter.apply(state)).when(subscription).request(anyLong());
    subscriber.onSubscribe(subscription);
  }

  private static List<SubscribeToShardEvent> generateEvents(
      final Integer numberOfEvents, AtomicInteger sequenceNumber) {
    Stream<SubscribeToShardEvent> recordsWithData =
        IntStream.range(0, numberOfEvents)
            .mapToObj(
                i -> {
                  Integer sn = sequenceNumber.incrementAndGet();
                  return SubscribeToShardEvent.builder()
                      .records(createRecord(sn))
                      .continuationSequenceNumber(sn.toString())
                      .build();
                });

    Stream<SubscribeToShardEvent> recordsWithOutData =
        IntStream.range(0, numberOfEvents)
            .mapToObj(
                i ->
                    SubscribeToShardEvent.builder()
                        .records(ImmutableList.of())
                        .continuationSequenceNumber(
                            String.valueOf(sequenceNumber.incrementAndGet()))
                        .build());

    return Stream.concat(recordsWithData, recordsWithOutData).collect(Collectors.toList());
  }
}
