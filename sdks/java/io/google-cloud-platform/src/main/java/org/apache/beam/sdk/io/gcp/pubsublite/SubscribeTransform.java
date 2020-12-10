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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.common.math.LongMath;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Stopwatch;
import org.joda.time.Duration;

class SubscribeTransform extends PTransform<PBegin, PCollection<SequencedMessage>> {
  private static final Duration MAX_SLEEP_TIME = Duration.standardMinutes(1);

  private final SubscriberOptions options;

  SubscribeTransform(SubscriberOptions options) {
    this.options = options;
  }

  private Subscriber newSubscriber(Partition partition, Consumer<List<SequencedMessage>> consumer)
      throws ApiException {
    try {
      return options
          .getSubscriberFactory(partition)
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

  private PartitionProcessor newPartitionProcessor(
      Partition partition,
      RestrictionTracker<OffsetRange, OffsetByteProgress> tracker,
      OutputReceiver<SequencedMessage> receiver) {
    return new PartitionProcessorImpl(
        tracker,
        receiver,
        options.getCommitter(partition),
        consumer -> newSubscriber(partition, consumer),
        options.flowControlSettings());
  }

  private RestrictionTracker<OffsetRange, OffsetByteProgress> newRestrictionTracker(
      Partition partition, OffsetRange initial) {
    return new OffsetByteRangeTracker(
        initial,
        options.getBacklogReader(partition),
        Stopwatch.createUnstarted(),
        MAX_SLEEP_TIME.multipliedBy(3).dividedBy(4),
        LongMath.saturatedMultiply(options.flowControlSettings().bytesOutstanding(), 10));
  }

  @Override
  public PCollection<SequencedMessage> expand(PBegin input) {
    PCollection<Partition> partitions = Create.of(options.partitions()).expand(input);
    // Prevent fusion between Create and read.
    PCollection<Partition> shuffledPartitions = partitions.apply(Reshuffle.viaRandomKey());
    return shuffledPartitions.apply(
        ParDo.of(
            new PerPartitionSdf(
                MAX_SLEEP_TIME,
                options::getInitialOffsetReader,
                this::newRestrictionTracker,
                this::newPartitionProcessor)));
  }
}
