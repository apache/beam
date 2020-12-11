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

import static java.lang.Math.min;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.BufferingPullSubscriber;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An UnboundedSource of Pub/Sub Lite SequencedMessages. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class PubsubLiteUnboundedSource extends UnboundedSource<SequencedMessage, OffsetCheckpointMark> {
  private final SubscriberOptions subscriberOptions;

  PubsubLiteUnboundedSource(SubscriberOptions options) {
    this.subscriberOptions = options;
  }

  @Override
  public List<? extends UnboundedSource<SequencedMessage, OffsetCheckpointMark>> split(
      int desiredNumSplits, PipelineOptions options) {
    ArrayList<ArrayList<Partition>> partitionPartitions =
        new ArrayList<>(min(desiredNumSplits, subscriberOptions.partitions().size()));
    for (int i = 0; i < desiredNumSplits; i++) {
      partitionPartitions.add(new ArrayList<>());
    }
    int counter = 0;
    for (Partition partition : subscriberOptions.partitions()) {
      partitionPartitions.get(counter % desiredNumSplits).add(partition);
      ++counter;
    }
    ImmutableList.Builder<PubsubLiteUnboundedSource> builder = ImmutableList.builder();
    for (List<Partition> partitionSubset : partitionPartitions) {
      if (partitionSubset.isEmpty()) {
        continue;
      }
      builder.add(
          new PubsubLiteUnboundedSource(
              subscriberOptions
                  .toBuilder()
                  .setPartitions(ImmutableSet.copyOf(partitionSubset))
                  .build()));
    }
    return builder.build();
  }

  @Override
  public UnboundedReader<SequencedMessage> createReader(
      PipelineOptions options, @Nullable OffsetCheckpointMark checkpointMark) throws IOException {
    try {
      ImmutableMap.Builder<Partition, PubsubLiteUnboundedReader.SubscriberState> statesBuilder =
          ImmutableMap.builder();
      for (Partition partition : subscriberOptions.partitions()) {
        PubsubLiteUnboundedReader.SubscriberState state =
            new PubsubLiteUnboundedReader.SubscriberState();
        state.committer = subscriberOptions.getCommitter(partition);
        if (checkpointMark != null && checkpointMark.partitionOffsetMap.containsKey(partition)) {
          Offset checkpointed = checkpointMark.partitionOffsetMap.get(partition);
          state.lastDelivered = Optional.of(checkpointed);
          state.subscriber =
              new TranslatingPullSubscriber(
                  new BufferingPullSubscriber(
                      subscriberOptions.getSubscriberFactory(partition),
                      subscriberOptions.flowControlSettings(),
                      SeekRequest.newBuilder()
                          .setCursor(Cursor.newBuilder().setOffset(checkpointed.value()))
                          .build()));
        } else {
          state.subscriber =
              new TranslatingPullSubscriber(
                  new BufferingPullSubscriber(
                      subscriberOptions.getSubscriberFactory(partition),
                      subscriberOptions.flowControlSettings()));
        }
        statesBuilder.put(partition, state);
      }
      return new PubsubLiteUnboundedReader(
          this, statesBuilder.build(), subscriberOptions.getBacklogReader());
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public Coder<OffsetCheckpointMark> getCheckpointMarkCoder() {
    return OffsetCheckpointMark.getCoder();
  }

  @Override
  public Coder<SequencedMessage> getOutputCoder() {
    return ProtoCoder.of(SequencedMessage.class);
  }
}
