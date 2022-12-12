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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.getCallContext;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.CursorClientSettings;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.internal.wire.CommitterSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.cloud.pubsublite.v1.CursorServiceSettings;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;

class SubscriberAssembler {
  private static final Framework FRAMEWORK = Framework.of("BEAM");
  private static final ConcurrentHashMap<SubscriptionPath, TopicPath> KNOWN_PATHS =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<SubscriptionPath, SubscriberServiceClient> SUB_CLIENTS =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<SubscriptionPath, CursorServiceClient> CURSOR_CLIENTS =
      new ConcurrentHashMap<>();

  private final SubscriberOptions options;
  private final Partition partition;

  private static TopicPath lookupTopicPath(SubscriptionPath subscriptionPath) {
    try (AdminClient adminClient =
        AdminClient.create(
            AdminClientSettings.newBuilder()
                .setRegion(subscriptionPath.location().extractRegion())
                .build())) {
      return TopicPath.parse(
          adminClient.getSubscription(subscriptionPath).get(1, MINUTES).getTopic());
    } catch (Throwable t) {
      throw ExtractStatus.toCanonical(t).underlying;
    }
  }

  private TopicPath getTopicPath() {
    return KNOWN_PATHS.computeIfAbsent(
        options.subscriptionPath(), SubscriberAssembler::lookupTopicPath);
  }

  private SubscriberServiceClient newSubscriberServiceClient() throws ApiException {
    try {
      SubscriberServiceSettings.Builder settingsBuilder = SubscriberServiceSettings.newBuilder();
      return SubscriberServiceClient.create(
          addDefaultSettings(
              options.subscriptionPath().location().extractRegion(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private SubscriberServiceClient getSubscriberServiceClient() {
    return SUB_CLIENTS.computeIfAbsent(
        options.subscriptionPath(), path -> newSubscriberServiceClient());
  }

  private CursorServiceClient newCursorClient() throws ApiException {
    try {
      CursorServiceSettings.Builder settingsBuilder = CursorServiceSettings.newBuilder();
      return CursorServiceClient.create(
          addDefaultSettings(
              options.subscriptionPath().location().extractRegion(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private CursorServiceClient getCursorClient() {
    return CURSOR_CLIENTS.computeIfAbsent(options.subscriptionPath(), path -> newCursorClient());
  }

  SubscriberAssembler(SubscriberOptions options, Partition partition) {
    this.options = options;
    this.partition = partition;
  }

  SubscriberFactory getSubscriberFactory(Offset initialOffset) {
    SubscriberServiceClient client = getSubscriberServiceClient();
    return consumer ->
        SubscriberBuilder.newBuilder()
            .setMessageConsumer(consumer)
            .setSubscriptionPath(options.subscriptionPath())
            .setPartition(partition)
            .setRetryStreamRaces(false)
            .setStreamFactory(
                responseStream -> {
                  ApiCallContext context =
                      getCallContext(
                          PubsubContext.of(FRAMEWORK),
                          RoutingMetadata.of(options.subscriptionPath(), partition));
                  return client.subscribeCallable().splitCall(responseStream, context);
                })
            .setInitialLocation(
                SeekRequest.newBuilder()
                    .setCursor(Cursor.newBuilder().setOffset(initialOffset.value()))
                    .build())
            .build();
  }

  BlockingCommitter newCommitter() {
    CursorServiceClient client = getCursorClient();
    Committer committer =
        CommitterSettings.newBuilder()
            .setPartition(partition)
            .setSubscriptionPath(options.subscriptionPath())
            .setStreamFactory(
                responseStream -> {
                  ApiCallContext context =
                      getCallContext(
                          PubsubContext.of(FRAMEWORK),
                          RoutingMetadata.of(options.subscriptionPath(), partition));
                  return client.streamingCommitCursorCallable().splitCall(responseStream, context);
                })
            .build()
            .instantiate();
    committer.startAsync().awaitRunning();
    return new BlockingCommitterImpl(committer);
  }

  TopicBacklogReader getBacklogReader() {
    return TopicBacklogReaderSettings.newBuilder()
        .setTopicPath(getTopicPath())
        .setPartition(partition)
        .build()
        .instantiate();
  }

  InitialOffsetReader getInitialOffsetReader() {
    return new InitialOffsetReaderImpl(
        CursorClient.create(
            CursorClientSettings.newBuilder()
                .setServiceClient(getCursorClient())
                .setRegion(options.subscriptionPath().location().extractRegion())
                .build()),
        options.subscriptionPath(),
        partition);
  }
}
