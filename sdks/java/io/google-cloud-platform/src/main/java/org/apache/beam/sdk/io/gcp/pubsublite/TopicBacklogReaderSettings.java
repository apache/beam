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

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.internal.TopicStatsClientSettings;
import com.google.cloud.pubsublite.proto.TopicStatsServiceGrpc.TopicStatsServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@AutoValue
public abstract class TopicBacklogReaderSettings {

  /**
   * The topic path for this backlog reader. Either topicPath or subscriptionPath must be set. If
   * both are set, subscriptionPath will be ignored.
   */
  abstract Optional<TopicPath> topicPath();

  /**
   * The subscription path for this backlog reader. Either topicPath or subscriptionPath must be
   * set. If both are set, subscriptionPath will be ignored.
   */
  abstract Optional<SubscriptionPath> subscriptionPath();

  // Optional parameters
  abstract Optional<TopicStatsServiceBlockingStub> stub();

  public static Builder newBuilder() {
    return new AutoValue_TopicBacklogReaderSettings.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    // Required parameters.
    public abstract Builder setTopicPath(TopicPath topicPath);

    public abstract Builder setSubscriptionPath(SubscriptionPath topicPath);

    public abstract Builder setStub(TopicStatsServiceBlockingStub stub);

    public abstract TopicBacklogReaderSettings build();
  }

  TopicBacklogReader instantiate() throws StatusException {
    TopicStatsClientSettings.Builder builder = TopicStatsClientSettings.newBuilder();
    if (stub().isPresent()) {
      builder.setStub(stub().get());
    }
    TopicPath path;
    if (topicPath().isPresent()) {
      path = topicPath().get();
    } else if (subscriptionPath().isPresent()) {
      try (AdminClient adminClient =
          AdminClient.create(
              AdminClientSettings.newBuilder()
                  .setRegion(subscriptionPath().get().location().region())
                  .build())) {
        path =
            TopicPath.parse(adminClient.getSubscription(subscriptionPath().get()).get().getTopic());
      } catch (ExecutionException e) {
        throw ExtractStatus.toCanonical(e.getCause());
      } catch (Throwable t) {
        throw ExtractStatus.toCanonical(t);
      }
    } else {
      throw new StatusException(
          Status.INVALID_ARGUMENT.withDescription(
              "Either subscriptionPath or topicPath must be set"));
    }
    builder.setRegion(path.location().region());
    return new TopicBacklogReaderImpl(TopicStatsClient.create(builder.build()), path);
  }
}
