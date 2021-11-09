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
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultMetadata;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.CursorClientSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.CommitCursorRequest;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.cloud.pubsublite.v1.CursorServiceSettings;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;

class SubscriberAssembler {
  private static final Framework FRAMEWORK = Framework.of("BEAM");
  private final SubscriberOptions options;
  private final Partition partition;

  SubscriberAssembler(SubscriberOptions options, Partition partition) {
    this.options = options;
    this.partition = partition;
  }

  private SubscriberServiceClient newSubscriberServiceClient() throws ApiException {
    try {
      SubscriberServiceSettings.Builder settingsBuilder = SubscriberServiceSettings.newBuilder();
      settingsBuilder =
          addDefaultMetadata(
              PubsubContext.of(FRAMEWORK),
              RoutingMetadata.of(options.subscriptionPath(), partition),
              settingsBuilder);
      return SubscriberServiceClient.create(
          addDefaultSettings(
              options.subscriptionPath().location().extractRegion(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  SubscriberFactory getSubscriberFactory(Offset initialOffset) {
    return consumer ->
        SubscriberBuilder.newBuilder()
            .setMessageConsumer(consumer)
            .setSubscriptionPath(options.subscriptionPath())
            .setPartition(partition)
            .setServiceClient(newSubscriberServiceClient())
            .setInitialLocation(
                SeekRequest.newBuilder()
                    .setCursor(Cursor.newBuilder().setOffset(initialOffset.value()))
                    .build())
            .build();
  }

  private CursorServiceClient newCursorServiceClient() throws ApiException {
    try {
      return CursorServiceClient.create(
          addDefaultSettings(
              options.subscriptionPath().location().extractRegion(),
              CursorServiceSettings.newBuilder()));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  BlockingCommitter getCommitter() {
    return offset ->
        newCursorServiceClient()
            .commitCursor(
                CommitCursorRequest.newBuilder()
                    .setSubscription(options.subscriptionPath().toString())
                    .setPartition(partition.value())
                    .setCursor(Cursor.newBuilder().setOffset(offset.value()))
                    .build());
  }

  TopicBacklogReader getBacklogReader() {
    return TopicBacklogReaderSettings.newBuilder()
        .setTopicPathFromSubscriptionPath(options.subscriptionPath())
        .setPartition(partition)
        .build()
        .instantiate();
  }

  InitialOffsetReader getInitialOffsetReader() {
    return new InitialOffsetReaderImpl(
        CursorClient.create(
            CursorClientSettings.newBuilder()
                .setRegion(options.subscriptionPath().location().extractRegion())
                .build()),
        options.subscriptionPath(),
        partition);
  }
}
