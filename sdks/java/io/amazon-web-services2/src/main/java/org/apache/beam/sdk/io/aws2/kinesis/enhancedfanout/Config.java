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

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.Checkers.checkNotNull;

import java.io.Serializable;
import java.time.Instant;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import software.amazon.kinesis.common.InitialPositionInStream;

/** This class is immutable. */
public class Config implements Serializable {
  private final String streamName;
  private final String consumerArn;
  private final StartingPoint startingPoint;
  private final Optional<Instant> startTimestamp;

  private static final long POOL_START_TIMEOUT_MS_DEFAULT = 10_000L;
  private final long poolStartTimeoutMs;
  private static final long RECONNECT_AFTER_FAILURE_DELAY_MS_DEFAULT = 1_000L;
  private final long reconnectAfterFailureDelayMs;

  // If you call SubscribeToShard again with the same ConsumerARN and ShardId
  // within 5 seconds of a successful call, you'll get a ResourceInUseException.
  private static final long STOP_COOL_DOWN_DELAY_MS_DEFAULT = 6_000L;
  private final long stopCoolDownDelayMs;

  public Config(
      String streamName,
      String consumerArn,
      StartingPoint startingPoint,
      Optional<Instant> startTimestamp) {
    if (startingPoint.getPosition().equals(InitialPositionInStream.AT_TIMESTAMP)
        && !startTimestamp.isPresent()) {
      throw new IllegalStateException("Timestamp must not be empty");
    }

    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.startingPoint = startingPoint;
    this.startTimestamp = startTimestamp;
    this.poolStartTimeoutMs = POOL_START_TIMEOUT_MS_DEFAULT;
    this.reconnectAfterFailureDelayMs = RECONNECT_AFTER_FAILURE_DELAY_MS_DEFAULT;
    this.stopCoolDownDelayMs = STOP_COOL_DOWN_DELAY_MS_DEFAULT;
  }

  public Config(String streamName, String consumerArn, StartingPoint startingPoint) {
    this(streamName, consumerArn, startingPoint, Optional.absent());
  }

  public Config(
      String streamName,
      String consumerArn,
      StartingPoint startingPoint,
      Optional<Instant> startTimestamp,
      long poolStartTimeoutMs,
      long reconnectAfterFailureDelayMs,
      long stopCoolDownDelayMs) {
    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.startingPoint = startingPoint;
    this.startTimestamp = startTimestamp;
    this.poolStartTimeoutMs = poolStartTimeoutMs;
    this.reconnectAfterFailureDelayMs = reconnectAfterFailureDelayMs;
    this.stopCoolDownDelayMs = stopCoolDownDelayMs;
  }

  public static Config fromIOSpec(KinesisIO.Read spec) {
    return new Config(
        checkNotNull(spec.getStreamName(), "streamName is null"),
        checkNotNull(spec.getConsumerArn(), "consumer ARN is null"),
        checkNotNull(spec.getInitialPosition(), "initial position is null"));
  }

  public String getStreamName() {
    return streamName;
  }

  public String getConsumerArn() {
    return consumerArn;
  }

  public StartingPoint getStartingPoint() {
    return startingPoint;
  }

  public Instant getStartTimestamp() {
    return startTimestamp.get();
  }

  public long getPoolStartTimeoutMs() {
    return poolStartTimeoutMs;
  }

  public long getReconnectAfterFailureDelayMs() {
    return reconnectAfterFailureDelayMs;
  }

  public long getStopCoolDownDelayMs() {
    return stopCoolDownDelayMs;
  }
}
