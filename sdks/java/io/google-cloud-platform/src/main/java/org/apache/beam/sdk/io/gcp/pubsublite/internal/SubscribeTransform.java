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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.construction.PTransformMatchers;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscribeTransform extends PTransform<PBegin, PCollection<SequencedMessage>> {

  private static final Logger LOG = LoggerFactory.getLogger(SubscribeTransform.class);

  private static final long MEBIBYTE = 1L << 20;
  private static final long SOFT_MEMORY_LIMIT = 512 * MEBIBYTE;
  private static final long MIN_PER_PARTITION_MEMORY = 10 * MEBIBYTE;
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
              messages -> consumer.accept(messages.stream().collect(Collectors.toList())));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
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

  private MemoryBufferedSubscriber getCachedSubscriber(
      SubscriptionPartition subscriptionPartition, Offset startOffset) {
    Supplier<MemoryBufferedSubscriber> getOrCreate =
        () ->
            PerServerSubscriberCache.CACHE.get(
                subscriptionPartition,
                () -> newBufferedSubscriber(subscriptionPartition, startOffset));
    while (true) {
      MemoryBufferedSubscriber subscriber = getOrCreate.get();
      Offset fetchOffset = subscriber.fetchOffset();
      if (startOffset.equals(fetchOffset)) {
        return subscriber;
      }
      LOG.info(
          "Discarding subscriber due to mismatch, this should be rare. {}, start: {} fetch: {}",
          subscriptionPartition,
          startOffset,
          fetchOffset);
      try {
        subscriber.stopAsync().awaitTerminated();
      } catch (Exception ignored) {
      }
    }
  }

  private SubscriptionPartitionProcessor newPartitionProcessor(
      SubscriptionPartition subscriptionPartition,
      RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker,
      OutputReceiver<SequencedMessage> receiver) {
    return new SubscriptionPartitionProcessorImpl(
        tracker,
        receiver,
        getCachedSubscriber(
            subscriptionPartition, Offset.of(tracker.currentRestriction().getRange().getFrom())));
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
    return new SubscriberAssembler(options, subscriptionPartition.partition()).newCommitter();
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

  @SuppressWarnings("unused")
  private PCollection<SequencedMessage> expandSdf(PBegin input) {
    PCollection<SubscriptionPartition> subscriptionPartitions =
        input.apply(new SubscriptionPartitionLoader(getTopicPath(), options.subscriptionPath()));
    return subscriptionPartitions.apply(
        ParDo.of(
            new PerSubscriptionPartitionSdf(
                new ManagedFactoryImpl<>(this::newBacklogReader),
                new ManagedFactoryImpl<>(this::newCommitter),
                this::newInitialOffsetReader,
                this::newRestrictionTracker,
                this::newPartitionProcessor)));
  }

  private PCollection<SequencedMessage> expandSource(PBegin input) {
    return input.apply(
        Read.from(
            new UnboundedSourceImpl(options, this::newBufferedSubscriber, this::newBacklogReader)));
  }

  private static final class SourceTransform
      extends PTransform<PBegin, PCollection<SequencedMessage>> {

    private final SubscribeTransform impl;

    private SourceTransform(SubscribeTransform impl) {
      this.impl = impl;
    }

    @Override
    public PCollection<SequencedMessage> expand(PBegin input) {
      return impl.expandSource(input);
    }
  }

  public static final PTransformOverride V1_READ_OVERRIDE =
      PTransformOverride.of(
          PTransformMatchers.classEqualTo(SubscribeTransform.class), new ReadOverrideFactory());

  private static class ReadOverrideFactory
      implements PTransformOverrideFactory<
          PBegin, PCollection<SequencedMessage>, SubscribeTransform> {

    @Override
    public PTransformReplacement<PBegin, PCollection<SequencedMessage>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<SequencedMessage>, SubscribeTransform> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new SourceTransform(transform.getTransform()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<SequencedMessage> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  @Override
  public PCollection<SequencedMessage> expand(PBegin input) {
    return expandSdf(input);
  }
}
