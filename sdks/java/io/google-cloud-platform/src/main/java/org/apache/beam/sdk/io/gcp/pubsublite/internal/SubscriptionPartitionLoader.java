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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.PartitionLookupUtils;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import java.util.Collections;
import org.apache.beam.sdk.testing.SerializableMatchers.SerializableSupplier;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SubscriptionPartitionLoader extends PTransform<PBegin, PCollection<SubscriptionPartition>> {
  private final TopicPath topic;
  private final SubscriptionPath subscription;
  private final SerializableFunction<TopicPath, Integer> getPartitionCount;
  private final Duration pollDuration;
  private final SerializableSupplier<Boolean> terminate;

  private class GeneratorFn extends DoFn<Void, SubscriptionPartition> {
    @ProcessElement
    public ProcessContinuation processElement(
        OutputReceiver<SubscriptionPartition> output,
        RestrictionTracker<Integer, Integer> restrictionTracker) {
      int previousCount = restrictionTracker.currentRestriction();
      int newCount = getPartitionCount.apply(topic);
      if (newCount <= previousCount) {
        return ProcessContinuation.resume().withResumeDelay(pollDuration);
      }
      if (!restrictionTracker.tryClaim(newCount)) {
        return ProcessContinuation.stop();
      }
      Instant ts = previousCount == 0 ? Instant.EPOCH : getWatermark();
      for (int i = previousCount; i < newCount; ++i) {
        output.outputWithTimestamp(SubscriptionPartition.of(subscription, Partition.of(i)), ts);
      }
      if (terminate.get()) {
        return ProcessContinuation.stop();
      }
      return ProcessContinuation.resume().withResumeDelay(pollDuration);
    }

    @GetInitialRestriction
    public Integer getInitialRestriction() {
      return 0;
    }

    @NewTracker
    public RestrictionTracker<Integer, Integer> newTracker(@Restriction Integer input) {
      return new RestrictionTracker<Integer, Integer>() {
        private int position = input;

        @Override
        public boolean tryClaim(Integer newPosition) {
          checkArgument(newPosition > position);
          position = newPosition;
          return true;
        }

        @Override
        public Integer currentRestriction() {
          return position;
        }

        @Override
        public @Nullable SplitResult<Integer> trySplit(double fractionOfRemainder) {
          return null;
        }

        @Override
        public void checkDone() throws IllegalStateException {}

        @Override
        public IsBounded isBounded() {
          return IsBounded.UNBOUNDED;
        }
      };
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Void> newWatermarkEstimator() {
      return new WatermarkEstimator<Void>() {
        @Override
        public Instant currentWatermark() {
          return getWatermark();
        }

        @Override
        public Void getState() {
          return null;
        }
      };
    }

    private Instant getWatermark() {
      return Instant.now().minus(watermarkDelay());
    }

    private Duration watermarkDelay() {
      return pollDuration.multipliedBy(3).dividedBy(2);
    }
  }

  SubscriptionPartitionLoader(TopicPath topic, SubscriptionPath subscription) {
    this(
        topic,
        subscription,
        PartitionLookupUtils::numPartitions,
        Duration.standardMinutes(1),
        () -> false);
  }

  @VisibleForTesting
  SubscriptionPartitionLoader(
      TopicPath topic,
      SubscriptionPath subscription,
      SerializableFunction<TopicPath, Integer> getPartitionCount,
      Duration pollDuration,
      SerializableSupplier<Boolean> terminate) {
    this.topic = topic;
    this.subscription = subscription;
    this.getPartitionCount = getPartitionCount;
    this.pollDuration = pollDuration;
    this.terminate = terminate;
  }

  @Override
  public PCollection<SubscriptionPartition> expand(PBegin input) {
    return input
        .apply(Create.of(Collections.<Void>singletonList(null)))
        .apply(ParDo.of(new GeneratorFn()));
  }
}
