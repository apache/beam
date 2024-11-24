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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.PartitionLookupUtils;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SubscriptionPartitionLoader extends PTransform<PBegin, PCollection<SubscriptionPartition>> {
  private final TopicPath topic;
  private final SubscriptionPath subscription;
  private final SerializableFunction<TopicPath, Integer> getPartitionCount;
  private final Duration pollDuration;
  private final SerializableSupplier<Boolean> terminate;

  private class GeneratorFn extends DoFn<byte[], SubscriptionPartition> {
    @ProcessElement
    public ProcessContinuation processElement(
        RestrictionTracker<Integer, Integer> restrictionTracker,
        OutputReceiver<SubscriptionPartition> output,
        ManualWatermarkEstimator<Instant> estimator) {
      int previousCount = restrictionTracker.currentRestriction();
      int newCount = getPartitionCount.apply(topic);
      if (!restrictionTracker.tryClaim(newCount)) {
        return ProcessContinuation.stop();
      }
      if (newCount > previousCount) {
        for (int i = previousCount; i < newCount; ++i) {
          output.outputWithTimestamp(
              SubscriptionPartition.of(subscription, Partition.of(i)),
              estimator.currentWatermark());
        }
      }
      estimator.setWatermark(getWatermark());
      return ProcessContinuation.resume().withResumeDelay(pollDuration);
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant initial) {
      // TODO: Add back when dataflow is fixed.
      // checkArgument(initial.equals(BoundedWindow.TIMESTAMP_MIN_VALUE));
      // return initial;
      return Instant.EPOCH;
    }

    @GetInitialRestriction
    public Integer getInitialRestriction() {
      return 0;
    }

    @NewTracker
    public RestrictionTracker<Integer, Integer> newTracker(@Restriction Integer input) {
      return new RestrictionTracker<Integer, Integer>() {
        private boolean terminated = false;
        private int position = input;

        @Override
        public boolean tryClaim(Integer newPosition) {
          checkArgument(newPosition >= position);
          if (terminated) {
            return false;
          }
          if (terminate.get()) {
            terminated = true;
            return false;
          }
          position = newPosition;
          return true;
        }

        @Override
        public Integer currentRestriction() {
          return position;
        }

        @Override
        public @Nullable SplitResult<Integer> trySplit(double fractionOfRemainder) {
          if (fractionOfRemainder != 0) {
            return null;
          }
          if (terminated) {
            return null;
          }
          terminated = true;
          return SplitResult.of(position, position);
        }

        @Override
        public void checkDone() throws IllegalStateException {
          checkState(terminated);
        }

        @Override
        public IsBounded isBounded() {
          return IsBounded.UNBOUNDED;
        }
      };
    }

    @NewWatermarkEstimator
    public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
        @WatermarkEstimatorState Instant state) {
      return new WatermarkEstimators.Manual(state);
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
        .apply("Impulse", Impulse.create())
        .apply("Watch Partition Count", ParDo.of(new GeneratorFn()));
  }
}
