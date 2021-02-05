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

import static com.google.cloud.pubsublite.internal.UncheckedApiPreconditions.checkArgument;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.PublishMetadata;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.reflect.TypeToken;

class Publishers {
  private static final Framework FRAMEWORK = Framework.of("BEAM");

  private Publishers() {}

  @SuppressWarnings("unchecked")
  static Publisher<PublishMetadata> newPublisher(PublisherOptions options) throws ApiException {
    SerializableSupplier<Object> supplier = options.publisherSupplier();
    if (supplier != null) {
      Object supplied = supplier.get();
      TypeToken<Publisher<PublishMetadata>> token = new TypeToken<Publisher<PublishMetadata>>() {};
      checkArgument(token.isSupertypeOf(supplied.getClass()));
      return (Publisher<PublishMetadata>) supplied;
    }
    return new PartitionCountWatchingPublisher(
        PartitionCountWatchingPublisherSettings.newBuilder()
            .setTopic(options.topicPath())
            .setPublisherFactory(
                partition ->
                    SinglePartitionPublisherBuilder.newBuilder()
                        .setTopic(options.topicPath())
                        .setPartition(partition)
                        .setContext(PubsubContext.of(FRAMEWORK))
                        .build())
            .build());
  }
}
