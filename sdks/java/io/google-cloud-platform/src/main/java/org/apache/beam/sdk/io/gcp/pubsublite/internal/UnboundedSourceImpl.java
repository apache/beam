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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.PartitionLookupUtils;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;

public class UnboundedSourceImpl extends UnboundedSource<SequencedMessage, CheckpointMarkImpl> {

  private final SubscriberOptions subscriberOptions;
  private final SubscriberFactory subscriberFactory;
  private final BacklogReaderFactory readerFactory;
  private final Optional<Partition> partition;

  interface SubscriberFactory extends Serializable {

    MemoryBufferedSubscriber create(SubscriptionPartition subscriptionPartition, Offset offset);
  }

  interface BacklogReaderFactory extends Serializable {

    TopicBacklogReader create(SubscriptionPartition subscriptionPartition);
  }

  UnboundedSourceImpl(
      SubscriberOptions subscriberOptions,
      SubscriberFactory subscriberFactory,
      BacklogReaderFactory readerFactory) {
    this.subscriberOptions = subscriberOptions;
    this.subscriberFactory = subscriberFactory;
    this.readerFactory = readerFactory;
    this.partition = Optional.absent();
  }

  private UnboundedSourceImpl(
      SubscriberOptions subscriberOptions,
      SubscriberFactory subscriberFactory,
      BacklogReaderFactory readerFactory,
      Partition partition) {
    this.subscriberOptions = subscriberOptions;
    this.subscriberFactory = subscriberFactory;
    this.readerFactory = readerFactory;
    this.partition = Optional.of(partition);
  }

  @Override
  public List<? extends UnboundedSource<SequencedMessage, CheckpointMarkImpl>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    checkState(!partition.isPresent());
    int numPartitions = PartitionLookupUtils.numPartitions(subscriberOptions.subscriptionPath());
    return IntStream.range(0, numPartitions)
        .mapToObj(
            val ->
                new UnboundedSourceImpl(
                    subscriberOptions, subscriberFactory, readerFactory, Partition.of(val)))
        .collect(Collectors.toList());
  }

  @Override
  public UnboundedReader<SequencedMessage> createReader(
      PipelineOptions options, @Nullable CheckpointMarkImpl checkpointMark) throws IOException {
    checkState(partition.isPresent());
    SubscriberAssembler assembler = new SubscriberAssembler(subscriberOptions, partition.get());
    Offset initialOffset;
    if (checkpointMark == null) {
      initialOffset = assembler.getInitialOffsetReader().read();
    } else {
      initialOffset = checkpointMark.offset;
    }
    SubscriptionPartition subscription =
        SubscriptionPartition.of(subscriberOptions.subscriptionPath(), partition.get());
    MemoryBufferedSubscriber subscriber = subscriberFactory.create(subscription, initialOffset);
    return new UnboundedReaderImpl(
        this,
        subscriber,
        readerFactory.create(subscription),
        CloserReference.of(assembler.newCommitter()),
        initialOffset);
  }

  @Override
  public Coder<CheckpointMarkImpl> getCheckpointMarkCoder() {
    return CheckpointMarkImpl.coder();
  }

  @Override
  public Coder<SequencedMessage> getOutputCoder() {
    return ProtoCoder.of(SequencedMessage.class);
  }
}
