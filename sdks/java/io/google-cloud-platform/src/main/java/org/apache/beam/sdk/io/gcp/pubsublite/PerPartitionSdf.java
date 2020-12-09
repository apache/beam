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

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;

class PerPartitionSdf extends DoFn<Partition, SequencedMessage> {
  private final Duration maxSleepTime;
  private final PartitionProcessorFactory processorFactory;
  private final SerializableFunction<Partition, InitialOffsetReader> offsetReaderFactory;
  private final SerializableBiFunction<
          Partition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
      trackerFactory;

  PerPartitionSdf(
      Duration maxSleepTime,
      SerializableFunction<Partition, InitialOffsetReader> offsetReaderFactory,
      SerializableBiFunction<
              Partition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
          trackerFactory,
      PartitionProcessorFactory processorFactory) {
    this.maxSleepTime = maxSleepTime;
    this.processorFactory = processorFactory;
    this.offsetReaderFactory = offsetReaderFactory;
    this.trackerFactory = trackerFactory;
  }

  @ProcessElement
  public ProcessContinuation processElement(
      RestrictionTracker<OffsetRange, OffsetByteProgress> tracker,
      @Element Partition partition,
      OutputReceiver<SequencedMessage> receiver)
      throws Exception {
    try (PartitionProcessor processor =
        processorFactory.newProcessor(partition, tracker, receiver)) {
      processor.start();
      return processor.waitForCompletion(maxSleepTime);
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element Partition partition) {
    try (InitialOffsetReader reader = offsetReaderFactory.apply(partition)) {
      Offset offset = reader.read();
      return new OffsetRange(offset.value(), Long.MAX_VALUE /* open interval */);
    }
  }

  @NewTracker
  public RestrictionTracker<OffsetRange, OffsetByteProgress> newTracker(
      @Element Partition partition, @Restriction OffsetRange range) {
    return trackerFactory.apply(partition, range);
  }
}
