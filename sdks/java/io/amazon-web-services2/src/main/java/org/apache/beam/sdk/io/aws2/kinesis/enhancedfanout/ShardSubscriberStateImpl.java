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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import java.util.concurrent.CompletableFuture;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicy;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicyFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class ShardSubscriberStateImpl implements ShardSubscriberState {
  private final KinesisShardEventsSubscriber subscriber;
  private final CompletableFuture<Void> subscriptionFuture;
  private ShardCheckpoint shardCheckpoint;
  private final WatermarkPolicy latestRecordTimestampPolicy =
      WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();
  private @Nullable Throwable err = null;

  public ShardSubscriberStateImpl(
      CompletableFuture<Void> subscriptionFuture,
      KinesisShardEventsSubscriber subscriber,
      ShardCheckpoint initialCheckpoint) {
    this.subscriptionFuture = subscriptionFuture;
    this.subscriber = subscriber;
    this.shardCheckpoint = initialCheckpoint;
  }

  @Override
  public void requestRecords(long n) {
    subscriber.requestRecords(n);
  }

  @Override
  public void ackRecord(ExtendedKinesisRecord record) throws IllegalStateException {
    if (err != null) {
      throw new IllegalStateException(err);
    }
    if (!shardCheckpoint.isClosed()) {
      shardCheckpoint = shardCheckpoint.moveAfter(record.getContinuationSequenceNumber());
    } else {
      shardCheckpoint = shardCheckpoint.markOrphan();
    }
    KinesisRecord kinesisRecord = record.getKinesisRecord();
    if (kinesisRecord != null) {
      latestRecordTimestampPolicy.update(kinesisRecord);
    }
  }

  @Override
  public ShardCheckpoint getCheckpoint() {
    return shardCheckpoint;
  }

  @Override
  public void cancel() {
    subscriber.cancel();
    subscriptionFuture.cancel(false);
  }

  @Override
  public void markClosed() {
    shardCheckpoint = shardCheckpoint.markClosed();
  }

  @Override
  public Instant getShardWatermark() {
    return latestRecordTimestampPolicy.getWatermark();
  }

  @Override
  public void setErr(Throwable e) {
    if (err == null) {
      err = Checkers.checkNotNull(e, "err");
    }
  }
}
