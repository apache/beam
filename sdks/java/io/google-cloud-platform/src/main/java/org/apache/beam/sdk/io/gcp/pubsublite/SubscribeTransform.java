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
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.BufferingPullSubscriber;
import com.google.cloud.pubsublite.internal.PullSubscriber;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

class SubscribeTransform extends PTransform<PBegin, PCollection<SequencedMessage>> {
  private static final Duration MAX_SLEEP_TIME = Duration.standardMinutes(1);

  private final SubscriberOptions options;

  SubscribeTransform(SubscriberOptions options) {
    this.options = options;
  }

  private PullSubscriber<SequencedMessage> newPullSubscriber(Partition partition, Offset offset)
      throws ApiException {
    try {
      return new TranslatingPullSubscriber(
          new BufferingPullSubscriber(
              options.getSubscriberFactory(partition),
              options.flowControlSettings(),
              SeekRequest.newBuilder()
                  .setCursor(Cursor.newBuilder().setOffset(offset.value()))
                  .build()));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private RestrictionTracker<OffsetRange, OffsetByteProgress> newRestrictionTracker(
      Partition partition, OffsetRange initial) {
    return new OffsetByteRangeTracker(initial, options.getBacklogReader(partition));
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
                this::newPullSubscriber,
                options::getCommitter,
                () -> Sleeper.DEFAULT,
                options::getInitialOffsetReader,
                this::newRestrictionTracker)));
  }
}
