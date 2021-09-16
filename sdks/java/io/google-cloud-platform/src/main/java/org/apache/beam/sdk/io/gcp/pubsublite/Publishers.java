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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static com.google.cloud.pubsublite.internal.UncheckedApiPreconditions.checkArgument;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultMetadata;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.PublisherServiceClient;
import com.google.cloud.pubsublite.v1.PublisherServiceSettings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.TypeToken;

class Publishers {
  private static final Framework FRAMEWORK = Framework.of("BEAM");

  private Publishers() {}

  private static AdminClient newAdminClient(PublisherOptions options) throws ApiException {
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

  private static PublisherServiceClient newServiceClient(
      PublisherOptions options, Partition partition) {
    PublisherServiceSettings.Builder settingsBuilder = PublisherServiceSettings.newBuilder();
    settingsBuilder =
        addDefaultMetadata(
            PubsubContext.of(FRAMEWORK),
            RoutingMetadata.of(options.topicPath(), partition),
            settingsBuilder);
    try {
      return PublisherServiceClient.create(
          addDefaultSettings(options.topicPath().location().extractRegion(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  @SuppressWarnings("unchecked")
  static Publisher<MessageMetadata> newPublisher(PublisherOptions options) throws ApiException {
    SerializableSupplier<Object> supplier = options.publisherSupplier();
    if (supplier != null) {
      Object supplied = supplier.get();
      TypeToken<Publisher<MessageMetadata>> token = new TypeToken<Publisher<MessageMetadata>>() {};
      checkArgument(token.isSupertypeOf(supplied.getClass()));
      return (Publisher<MessageMetadata>) supplied;
    }
    return PartitionCountWatchingPublisherSettings.newBuilder()
        .setTopic(options.topicPath())
        .setPublisherFactory(
            partition ->
                SinglePartitionPublisherBuilder.newBuilder()
                    .setTopic(options.topicPath())
                    .setPartition(partition)
                    .setServiceClient(newServiceClient(options, partition))
                    .setBatchingSettings(PublisherSettings.DEFAULT_BATCHING_SETTINGS)
                    .build())
        .setAdminClient(newAdminClient(options))
        .build()
        .instantiate();
  }
}
