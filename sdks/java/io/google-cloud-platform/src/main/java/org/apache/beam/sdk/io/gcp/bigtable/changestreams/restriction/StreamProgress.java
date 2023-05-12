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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Position for {@link ReadChangeStreamPartitionProgressTracker}. This represents contains
 * information that allows a stream, along with the {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord} to resume from a
 * checkpoint.
 *
 * <p>It should contain either a continuation token which represents a position in the stream, or it
 * can contain a close stream message which represents an end to the stream and the DoFn needs to
 * stop.
 */
@Internal
public class StreamProgress implements Serializable {
  private static final long serialVersionUID = -5384329262726188695L;

  private @Nullable ChangeStreamContinuationToken currentToken;
  private @Nullable Instant estimatedLowWatermark;
  private @Nullable BigDecimal throughputEstimate;
  private @Nullable Instant lastRunTimestamp;
  private @Nullable CloseStream closeStream;
  private boolean failToLock;
  private boolean isHeartbeat;

  public StreamProgress() {}

  public StreamProgress(
      @Nullable ChangeStreamContinuationToken token,
      Instant estimatedLowWatermark,
      BigDecimal throughputEstimate,
      Instant lastRunTimestamp,
      boolean isHeartbeat) {
    this.currentToken = token;
    this.estimatedLowWatermark = estimatedLowWatermark;
    this.throughputEstimate = throughputEstimate;
    this.lastRunTimestamp = lastRunTimestamp;
    this.isHeartbeat = isHeartbeat;
  }

  public StreamProgress(@Nullable CloseStream closeStream) {
    this.closeStream = closeStream;
  }

  public @Nullable ChangeStreamContinuationToken getCurrentToken() {
    return currentToken;
  }

  public @Nullable Instant getEstimatedLowWatermark() {
    return estimatedLowWatermark;
  }

  public @Nullable BigDecimal getThroughputEstimate() {
    return throughputEstimate;
  }

  public @Nullable Instant getLastRunTimestamp() {
    return lastRunTimestamp;
  }

  public @Nullable CloseStream getCloseStream() {
    return closeStream;
  }

  public boolean isFailToLock() {
    return failToLock;
  }

  public void setFailToLock(boolean failToLock) {
    this.failToLock = failToLock;
  }

  public boolean isHeartbeat() {
    return this.isHeartbeat;
  }

  public boolean isEmpty() {
    return closeStream == null && currentToken == null;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamProgress)) {
      return false;
    }
    StreamProgress that = (StreamProgress) o;
    return Objects.equals(getCurrentToken(), that.getCurrentToken())
        && Objects.equals(getEstimatedLowWatermark(), that.getEstimatedLowWatermark())
        && Objects.equals(getCloseStream(), that.getCloseStream())
        && (isFailToLock() == that.isFailToLock())
        && Objects.equals(getThroughputEstimate(), that.getThroughputEstimate())
        && Objects.equals(getLastRunTimestamp(), that.getLastRunTimestamp())
        && (isHeartbeat() == that.isHeartbeat());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCurrentToken(), getCloseStream());
  }

  @Override
  public String toString() {
    return "StreamProgress{"
        + "currentToken="
        + currentToken
        + ", estimatedLowWatermark="
        + estimatedLowWatermark
        + ", closeStream="
        + closeStream
        + ", failToLock="
        + failToLock
        + ", throughputEstimate="
        + throughputEstimate
        + ", lastRunTimestamp="
        + lastRunTimestamp
        + ", isHeartbeat="
        + isHeartbeat
        + '}';
  }
}
