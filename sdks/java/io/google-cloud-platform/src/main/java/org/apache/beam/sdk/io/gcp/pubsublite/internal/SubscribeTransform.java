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
import static com.google.cloud.pubsublite.internal.UncheckedApiPreconditions.checkArgument;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class SubscribeTransform extends PTransform<PBegin, PCollection<SequencedMessage>> {
  private static final long MEBIBYTE = 1L << 20;
  private static final long SOFT_MEMORY_LIMIT = 512 * MEBIBYTE;
  private static final long MIN_PER_PARTITION_MEMORY = 4 * MEBIBYTE;
  private static final long MAX_PER_PARTITION_MEMORY = 100 * MEBIBYTE;

  private static final MemoryLimiter LIMITER =
      new MemoryLimiterImpl(MIN_PER_PARTITION_MEMORY, MAX_PER_PARTITION_MEMORY, SOFT_MEMORY_LIMIT);

  private final SubscriberOptions options;

  public SubscribeTransform(SubscriberOptions options) {
    this.options = options;
  }

  private void checkSubscription(SubscriptionPartition subscriptionPartition) throws ApiException {
    checkArgument(subscriptionPartition.subscription().equals(options.subscriptionPath()));
  }

  private Subscriber newSubscriber(
      Partition partition, Offset initialOffset, Consumer<List<SequencedMessage>> consumer) {
    try {
      return new SubscriberAssembler(options, partition)
          .getSubscriberFactory(initialOffset)
          .newSubscriber(
              messages ->
                  consumer.accept(
                      messages.stream()
                          .map(message -> message.toProto())
                          .collect(Collectors.toList())));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private SubscriptionPartitionProcessor newPartitionProcessor(
      SubscriptionPartition subscriptionPartition,
      RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker,
      OutputReceiver<SequencedMessage> receiver) {
    Supplier<MemoryBufferedSubscriber> newSubscriber =
        () ->
            newBufferedSubscriber(
                subscriptionPartition,
                Offset.of(tracker.currentRestriction().getRange().getFrom()));
    return new SubscriptionPartitionProcessorImpl(
        subscriptionPartition,
        tracker,
        receiver,
        () -> PerServerSubscriberCache.CACHE.get(subscriptionPartition, newSubscriber));
  }

  private MemoryBufferedSubscriber newBufferedSubscriber(
      SubscriptionPartition subscriptionPartition, Offset startOffset) throws ApiException {
    checkSubscription(subscriptionPartition);
    return new MemoryBufferedSubscriberImpl(
        subscriptionPartition.partition(),
        startOffset,
        LIMITER,
        consumer -> newSubscriber(subscriptionPartition.partition(), startOffset, consumer));
  }

  private TopicBacklogReader newBacklogReader(SubscriptionPartition subscriptionPartition) {
    checkSubscription(subscriptionPartition);
    return new SubscriberAssembler(options, subscriptionPartition.partition()).getBacklogReader();
  }

  private TrackerWithProgress newRestrictionTracker(
      TopicBacklogReader backlogReader, OffsetByteRange initial) {
    return new OffsetByteRangeTracker(initial, backlogReader);
  }

  private InitialOffsetReader newInitialOffsetReader(SubscriptionPartition subscriptionPartition) {
    checkSubscription(subscriptionPartition);
    return new SubscriberAssembler(options, subscriptionPartition.partition())
        .getInitialOffsetReader();
  }

  private BlockingCommitter newCommitter(SubscriptionPartition subscriptionPartition) {
    checkSubscription(subscriptionPartition);
    return new SubscriberAssembler(options, subscriptionPartition.partition()).getCommitter();
  }

  private TopicPath getTopicPath() {
    try (AdminClient admin =
        AdminClient.create(
            AdminClientSettings.newBuilder()
                .setRegion(options.subscriptionPath().location().extractRegion())
                .build())) {
      return TopicPath.parse(admin.getSubscription(options.subscriptionPath()).get().getTopic());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  @Override
  public PCollection<SequencedMessage> expand(PBegin input) {
    PCollection<SubscriptionPartition> subscriptionPartitions;
    subscriptionPartitions =
        input.apply(new SubscriptionPartitionLoader(getTopicPath(), options.subscriptionPath()));

    return subscriptionPartitions.apply(
        ParDo.of(
            new PerSubscriptionPartitionSdf(
                new ManagedBacklogReaderFactoryImpl(this::newBacklogReader),
                this::newInitialOffsetReader,
                this::newRestrictionTracker,
                this::newPartitionProcessor,
                this::newCommitter)));
  }
}
