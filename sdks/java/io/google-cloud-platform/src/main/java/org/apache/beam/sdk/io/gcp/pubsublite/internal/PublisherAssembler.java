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

import static com.google.cloud.pubsublite.cloudpubsub.PublisherSettings.DEFAULT_BATCHING_SETTINGS;
import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.getCallContext;

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PartitionPublisherFactory;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.PublisherServiceClient;
import com.google.cloud.pubsublite.v1.PublisherServiceSettings;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;

class PublisherAssembler {
  private static final Framework FRAMEWORK = Framework.of("BEAM");

  private final PublisherOptions options;

  PublisherAssembler(PublisherOptions options) {
    this.options = options;
  }

  private AdminClient newAdminClient() throws ApiException {
    try {
      return AdminClient.create(
          AdminClientSettings.newBuilder()
              .setServiceClient(
                  AdminServiceClient.create(
                      addDefaultSettings(
                          options.topicPath().location().extractRegion(),
                          AdminServiceSettings.newBuilder())))
              .setRegion(options.topicPath().location().extractRegion())
              .build());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private PublisherServiceClient newServiceClient() {
    try {
      return PublisherServiceClient.create(
          addDefaultSettings(
              options.topicPath().location().extractRegion(),
              PublisherServiceSettings.newBuilder()));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private PartitionPublisherFactory getPartitionPublisherFactory() throws ApiException {
    PublisherServiceClient client = newServiceClient();
    return new PartitionPublisherFactory() {
      @Override
      public com.google.cloud.pubsublite.internal.Publisher<MessageMetadata> newPublisher(
          Partition partition) throws ApiException {
        SinglePartitionPublisherBuilder.Builder singlePartitionBuilder =
            SinglePartitionPublisherBuilder.newBuilder()
                .setTopic(options.topicPath())
                .setPartition(partition)
                .setBatchingSettings(DEFAULT_BATCHING_SETTINGS)
                .setStreamFactory(
                    responseStream -> {
                      ApiCallContext context =
                          getCallContext(
                              PubsubContext.of(FRAMEWORK),
                              RoutingMetadata.of(options.topicPath(), partition));
                      return client.publishCallable().splitCall(responseStream, context);
                    });
        return singlePartitionBuilder.build();
      }

      @Override
      public void close() {
        client.close();
      }
    };
  }

  Publisher<MessageMetadata> newPublisher() throws ApiException {
    return PartitionCountWatchingPublisherSettings.newBuilder()
        .setTopic(options.topicPath())
        .setPublisherFactory(getPartitionPublisherFactory())
        .setAdminClient(newAdminClient())
        .build()
        .instantiate();
  }
}
