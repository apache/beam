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
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultMetadata;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;

import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.CursorClientSettings;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.internal.wire.CommitterSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.cloud.pubsublite.v1.CursorServiceSettings;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import java.io.Serializable;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@AutoValue
public abstract class SubscriberOptions implements Serializable {
  private static final long serialVersionUID = 269598118L;

  private static final Framework FRAMEWORK = Framework.of("BEAM");

  private static final long MEBIBYTE = 1L << 20;

  private static final Duration MIN_BUNDLE_TIMEOUT = Duration.standardMinutes(1);

  public static final FlowControlSettings DEFAULT_FLOW_CONTROL =
      FlowControlSettings.builder()
          .setMessagesOutstanding(Long.MAX_VALUE)
          .setBytesOutstanding(100 * MEBIBYTE)
          .build();

  // Required parameters.
  public abstract SubscriptionPath subscriptionPath();

  // Optional parameters.
  /** Per-partition flow control parameters for this subscription. */
  public abstract FlowControlSettings flowControlSettings();

  /**
   * A set of partitions. If empty, continuously poll the set of partitions using an admin client.
   */
  public abstract Set<Partition> partitions();

  /**
   * The minimum wall time to pass before allowing bundle closure.
   *
   * <p>Setting this to too small of a value will result in increased compute costs and lower
   * throughput per byte. Immediate timeouts (Duration.ZERO) may be useful for testing.
   */
  public abstract Duration minBundleTimeout();

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

  /**
   * A supplier to override topic backlog reader creation entirely and delegate to another method.
   * Primarily useful for testing.
   */
  abstract @Nullable SerializableSupplier<TopicBacklogReader> backlogReaderSupplier();

  /**
   * A supplier to override offset reader creation entirely and delegate to another method.
   * Primarily useful for testing.
   */
  abstract @Nullable SerializableSupplier<InitialOffsetReader> offsetReaderSupplier();

  public static Builder newBuilder() {
    Builder builder = new AutoValue_SubscriberOptions.Builder();
    return builder
        .setPartitions(ImmutableSet.of())
        .setFlowControlSettings(DEFAULT_FLOW_CONTROL)
        .setMinBundleTimeout(MIN_BUNDLE_TIMEOUT);
  }

  public abstract Builder toBuilder();

  private SubscriberServiceClient newSubscriberServiceClient(Partition partition)
      throws ApiException {
    try {
      SubscriberServiceSettings.Builder settingsBuilder = SubscriberServiceSettings.newBuilder();

      settingsBuilder =
          addDefaultMetadata(
              PubsubContext.of(FRAMEWORK),
              RoutingMetadata.of(subscriptionPath(), partition),
              settingsBuilder);
      return SubscriberServiceClient.create(
          addDefaultSettings(subscriptionPath().location().region(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  SubscriberFactory getSubscriberFactory(Partition partition) {
    SubscriberFactory factory = subscriberFactory();
    if (factory != null) {
      return factory;
    }
    return consumer ->
        SubscriberBuilder.newBuilder()
            .setMessageConsumer(consumer)
            .setSubscriptionPath(subscriptionPath())
            .setPartition(partition)
            .setServiceClient(newSubscriberServiceClient(partition))
            .build();
  }

  private CursorServiceClient newCursorServiceClient() throws ApiException {
    try {
      return CursorServiceClient.create(
          addDefaultSettings(
              subscriptionPath().location().region(), CursorServiceSettings.newBuilder()));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  Committer getCommitter(Partition partition) {
    SerializableSupplier<Committer> supplier = committerSupplier();
    if (supplier != null) {
      return supplier.get();
    }
    return CommitterSettings.newBuilder()
        .setSubscriptionPath(subscriptionPath())
        .setPartition(partition)
        .setServiceClient(newCursorServiceClient())
        .build()
        .instantiate();
  }

  TopicBacklogReader getBacklogReader(Partition partition) {
    SerializableSupplier<TopicBacklogReader> supplier = backlogReaderSupplier();
    if (supplier != null) {
      return supplier.get();
    }
    return TopicBacklogReaderSettings.newBuilder()
        .setTopicPathFromSubscriptionPath(subscriptionPath())
        .setPartition(partition)
        .build()
        .instantiate();
  }

  InitialOffsetReader getInitialOffsetReader(Partition partition) {
    SerializableSupplier<InitialOffsetReader> supplier = offsetReaderSupplier();
    if (supplier != null) {
      return supplier.get();
    }
    return new InitialOffsetReaderImpl(
        CursorClient.create(
            CursorClientSettings.newBuilder()
                .setRegion(subscriptionPath().location().region())
                .build()),
        subscriptionPath(),
        partition);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    // Required parameters.
    public abstract Builder setSubscriptionPath(SubscriptionPath path);

    // Optional parameters
    public abstract Builder setPartitions(Set<Partition> partitions);

    public abstract Builder setFlowControlSettings(FlowControlSettings flowControlSettings);

    public abstract Builder setMinBundleTimeout(Duration minBundleTimeout);

    // Used in unit tests
    abstract Builder setSubscriberFactory(SubscriberFactory subscriberFactory);

    abstract Builder setCommitterSupplier(SerializableSupplier<Committer> committerSupplier);

    abstract Builder setBacklogReaderSupplier(
        SerializableSupplier<TopicBacklogReader> backlogReaderSupplier);

    abstract Builder setOffsetReaderSupplier(
        SerializableSupplier<InitialOffsetReader> offsetReaderSupplier);

    public abstract SubscriberOptions build();
  }
}
