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

import io.netty.handler.timeout.ReadTimeoutException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.reactivestreams.Subscriber;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;

public class KinesisClientStubShardState {
  private static final Map<String, List<String>> OLD_SHARD_ID_TO_NEW_SHARD_UP = oldToNewShardUp();
  private static final Map<List<String>, String> OLD_SHARD_ID_TO_NEW_SHARD_DOWN =
      oldToNewShardDown();
  private static final Map<String, SubscribeToShardEvent> SHARD_ID_TO_SHARD_UP_EVENT =
      shardIdToShardUpEventMap();
  private static final Map<String, SubscribeToShardEvent> SHARD_ID_TO_SHARD_DOWN_EVENT =
      shardIdToShardDownEventMap();

  private final String shardId;
  private final Subscriber<? super SubscribeToShardEventStream> subscriber;
  private final Iterator<SubscribeToShardEvent> recordsIterator;

  public KinesisClientStubShardState(
      String shardId,
      Subscriber<? super SubscribeToShardEventStream> subscriber,
      Iterator<SubscribeToShardEvent> recordsIterator) {
    this.shardId = shardId;
    this.subscriber = subscriber;
    this.recordsIterator = recordsIterator;
  }

  public static Void submitEventsAndThenComplete(KinesisClientStubShardState state) {
    if (state.recordsIterator.hasNext()) {
      state.subscriber.onNext(state.recordsIterator.next());
    } else {
      state.subscriber.onComplete();
    }
    return null;
  }

  public static Void submitEventsAndThenShardUp(KinesisClientStubShardState state) {
    if (state.recordsIterator.hasNext()) {
      state.subscriber.onNext(state.recordsIterator.next());
    } else {
      SubscribeToShardEvent reShardEvent = SHARD_ID_TO_SHARD_UP_EVENT.get(state.shardId);
      if (reShardEvent != null) {
        state.subscriber.onNext(reShardEvent);
      } else {
        state.subscriber.onComplete();
      }
    }
    return null;
  }

  public static Void submitEventsAndThenShardDown(KinesisClientStubShardState state) {
    if (state.recordsIterator.hasNext()) {
      state.subscriber.onNext(state.recordsIterator.next());
    } else {
      SubscribeToShardEvent reShardEvent = SHARD_ID_TO_SHARD_DOWN_EVENT.get(state.shardId);
      if (reShardEvent != null) {
        state.subscriber.onNext(reShardEvent);
      } else {
        state.subscriber.onComplete();
      }
    }
    return null;
  }

  public static Void submitEventsAndThenSendError(KinesisClientStubShardState state) {
    if (state.recordsIterator.hasNext()) {
      state.subscriber.onNext(state.recordsIterator.next());
    } else {
      if (state.shardId.equals("shard-000")) {
        state.subscriber.onComplete();
      } else {
        Throwable throwable = new ExecutionException(new RuntimeException("Oh.."));
        state.subscriber.onError(throwable);
      }
    }
    return null;
  }

  public static Void submitEventsAndThenSendRecoverableError(KinesisClientStubShardState state) {
    if (state.recordsIterator.hasNext()) {
      state.subscriber.onNext(state.recordsIterator.next());
    } else {
      if (state.shardId.equals("shard-000")) {
        state.subscriber.onComplete();
      } else {
        Throwable throwable = new ReadTimeoutException();
        state.subscriber.onError(throwable);
      }
    }
    return null;
  }

  private static Map<String, List<String>> oldToNewShardUp() {
    Map<String, List<String>> m = new HashMap<>();
    m.put("shard-000", ImmutableList.of("shard-002", "shard-003"));
    m.put("shard-001", ImmutableList.of("shard-004", "shard-005"));
    return m;
  }

  private static Map<List<String>, String> oldToNewShardDown() {
    Map<List<String>, String> m = new HashMap<>();
    m.put(ImmutableList.of("shard-000", "shard-001"), "shard-004");
    m.put(ImmutableList.of("shard-002"), "shard-005");
    m.put(ImmutableList.of("shard-003"), "shard-006");
    return m;
  }

  private static Map<String, SubscribeToShardEvent> shardIdToShardUpEventMap() {
    Map<String, SubscribeToShardEvent> m = new HashMap<>();
    OLD_SHARD_ID_TO_NEW_SHARD_UP.forEach(
        (k, v) -> {
          List<ChildShard> cs =
              v.stream()
                  .map(s -> ChildShard.builder().shardId(s).parentShards(k).build())
                  .collect(Collectors.toList());
          m.put(
              k,
              SubscribeToShardEvent.builder()
                  .continuationSequenceNumber(null)
                  .childShards(cs)
                  .build());
        });
    return m;
  }

  private static Map<String, SubscribeToShardEvent> shardIdToShardDownEventMap() {
    Map<String, SubscribeToShardEvent> m = new HashMap<>();
    OLD_SHARD_ID_TO_NEW_SHARD_DOWN.forEach(
        (k, v) -> {
          List<ChildShard> cs =
              k.stream()
                  .map(s -> ChildShard.builder().shardId(v).parentShards(k).build())
                  .collect(Collectors.toList());

          k.forEach(
              i ->
                  m.put(
                      i,
                      SubscribeToShardEvent.builder()
                          .continuationSequenceNumber(null)
                          .childShards(cs)
                          .build()));
        });
    return m;
  }
}
