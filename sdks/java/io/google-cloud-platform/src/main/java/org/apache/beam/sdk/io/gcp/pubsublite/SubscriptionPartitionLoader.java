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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.PartitionLookupUtils;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.PollResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SubscriptionPartitionLoader extends PTransform<PBegin, PCollection<SubscriptionPartition>> {
  private final TopicPath topic;
  private final SubscriptionPath subscription;
  private final SerializableFunction<TopicPath, Integer> getPartitionCount;
  private final Duration pollDuration;
  private final boolean terminate;

  SubscriptionPartitionLoader(TopicPath topic, SubscriptionPath subscription) {
    this(
        topic,
        subscription,
        PartitionLookupUtils::numPartitions,
        Duration.standardMinutes(1),
        false);
  }

  @VisibleForTesting
  SubscriptionPartitionLoader(
      TopicPath topic,
      SubscriptionPath subscription,
      SerializableFunction<TopicPath, Integer> getPartitionCount,
      Duration pollDuration,
      boolean terminate) {
    this.topic = topic;
    this.subscription = subscription;
    this.getPartitionCount = getPartitionCount;
    this.pollDuration = pollDuration;
    this.terminate = terminate;
  }

  @Override
  public PCollection<SubscriptionPartition> expand(PBegin input) {
    PCollection<TopicPath> start = input.apply(Create.of(ImmutableList.of(topic)));
    PCollection<KV<TopicPath, Partition>> partitions =
        start.apply(
            Watch.growthOf(
                    new PollFn<TopicPath, Partition>() {
                      @Override
                      public PollResult<Partition> apply(TopicPath element, Context c) {
                        checkArgument(element.equals(topic));
                        int partitionCount = getPartitionCount.apply(element);
                        List<Partition> partitions =
                            IntStream.range(0, partitionCount)
                                .mapToObj(Partition::of)
                                .collect(Collectors.toList());
                        return PollResult.incomplete(Instant.now(), partitions);
                      }
                    })
                .withPollInterval(pollDuration)
                .withTerminationPerInput(
                    terminate ? Watch.Growth.afterIterations(10) : Watch.Growth.never()));
    return partitions.apply(
        MapElements.into(TypeDescriptor.of(SubscriptionPartition.class))
            .via(kv -> SubscriptionPartition.of(subscription, kv.getValue())));
  }
}
