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
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.PartitionLookupUtils;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.internal.wire.CommitterBuilder;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.CursorServiceGrpc.CursorServiceStub;
import com.google.cloud.pubsublite.proto.SubscriberServiceGrpc.SubscriberServiceStub;
import io.grpc.StatusException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class SubscriberOptions implements Serializable {
  private static final long serialVersionUID = 269598118L;

  private static final Framework FRAMEWORK = Framework.of("BEAM");

  // Required parameters.
  public abstract SubscriptionPath subscriptionPath();

  public abstract FlowControlSettings flowControlSettings();

  // Optional parameters.
  /** A set of partitions. If empty, retrieve the set of partitions using an admin client. */
  public abstract Set<Partition> partitions();

  /** The class used to read backlog for the subscription described by subscriptionPath(). */
  public abstract TopicBacklogReader topicBacklogReader();

  /** A supplier for the subscriber stub to be used. */
  public abstract @Nullable SerializableSupplier<SubscriberServiceStub> subscriberStubSupplier();

  /** A supplier for the cursor service stub to be used. */
  public abstract @Nullable SerializableSupplier<CursorServiceStub> committerStubSupplier();

  /**
   * A factory to override subscriber creation entirely and delegate to another method. Primarily
   * useful for testing.
   */
  abstract @Nullable SubscriberFactory subscriberFactory();

  /**
   * A supplier to override committer creation entirely and delegate to another method. Primarily
   * useful for testing.
   */
  abstract @Nullable SerializableSupplier<Committer> committerSupplier();

  public static Builder newBuilder() {
    Builder builder = new AutoValue_SubscriberOptions.Builder();
    return builder.setPartitions(ImmutableSet.of());
  }

  public abstract Builder toBuilder();

  @SuppressWarnings("CheckReturnValue")
  Map<Partition, SubscriberFactory> getSubscriberFactories() {
    ImmutableMap.Builder<Partition, SubscriberFactory> factories = ImmutableMap.builder();
    for (Partition partition : partitions()) {
      SubscriberFactory factory = subscriberFactory();
      if (factory == null) {
        factory =
            consumer -> {
              SubscriberBuilder.Builder builder = SubscriberBuilder.newBuilder();
              builder.setMessageConsumer(consumer);
              builder.setSubscriptionPath(subscriptionPath());
              builder.setPartition(partition);
              builder.setContext(PubsubContext.of(FRAMEWORK));
              if (subscriberStubSupplier() != null) {
                builder.setSubscriberServiceStub(subscriberStubSupplier().get());
              }
              return builder.build();
            };
      }
      factories.put(partition, factory);
    }
    return factories.build();
  }

  @SuppressWarnings("CheckReturnValue")
  Map<Partition, Committer> getCommitters() throws StatusException {
    ImmutableMap.Builder<Partition, Committer> committers = ImmutableMap.builder();
    for (Partition partition : partitions()) {
      if (committerSupplier() != null) {
        committers.put(partition, committerSupplier().get());
      } else {
        CommitterBuilder.Builder builder = CommitterBuilder.newBuilder();
        builder.setSubscriptionPath(subscriptionPath());
        builder.setPartition(partition);
        if (committerStubSupplier() != null) {
          builder.setCursorStub(committerStubSupplier().get());
        }
        committers.put(partition, builder.build());
      }
    }
    return committers.build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    // Required parameters.
    public abstract Builder setSubscriptionPath(SubscriptionPath path);

    public abstract Builder setPartitions(Set<Partition> partitions);

    public abstract Builder setFlowControlSettings(FlowControlSettings flowControlSettings);

    // Optional parameters.
    public abstract Builder setSubscriberStubSupplier(
        SerializableSupplier<SubscriberServiceStub> stubSupplier);

    public abstract Builder setCommitterStubSupplier(
        SerializableSupplier<CursorServiceStub> stubSupplier);

    public abstract Builder setTopicBacklogReader(TopicBacklogReader topicBacklogReader);

    // Used in unit tests
    abstract Builder setSubscriberFactory(SubscriberFactory subscriberFactory);

    abstract Builder setCommitterSupplier(SerializableSupplier<Committer> committerSupplier);

    // Used for implementing build();
    abstract SubscriptionPath subscriptionPath();

    abstract Set<Partition> partitions();

    abstract Optional<TopicBacklogReader> topicBacklogReader();

    abstract SubscriberOptions autoBuild();

    public SubscriberOptions build() throws StatusException {
      if (!partitions().isEmpty() && topicBacklogReader().isPresent()) {
        return autoBuild();
      }

      if (partitions().isEmpty()) {
        int partitionCount = PartitionLookupUtils.numPartitions(subscriptionPath());
        ImmutableSet.Builder<Partition> partitions = ImmutableSet.builder();
        for (int i = 0; i < partitionCount; i++) {
          partitions.add(Partition.of(i));
        }
        setPartitions(partitions.build());
      }
      if (!topicBacklogReader().isPresent()) {
        setTopicBacklogReader(
            TopicBacklogReader.create(
                TopicBacklogReaderSettings.newBuilder()
                    .setSubscriptionPath(subscriptionPath())
                    .build()));
      }
      return autoBuild();
    }
  }
}
