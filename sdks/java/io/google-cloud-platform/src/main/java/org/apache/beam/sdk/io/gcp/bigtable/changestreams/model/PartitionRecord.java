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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.model;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.formatByteStringRange;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;

/**
 * Output result of {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn} containing
 * information required to stream a partition.
 */
@Internal
public class PartitionRecord implements Serializable {
  private static final long serialVersionUID = -4524061648930484599L;

  private final ByteStringRange partition;
  @Nullable private Instant startTime;
  @Nullable private List<ChangeStreamContinuationToken> changeStreamContinuationTokens;
  @Nullable private Instant endTime;
  private String uuid;
  private final Instant parentLowWatermark;
  private final List<NewPartition> parentPartitions;

  public PartitionRecord(
      ByteStringRange partition,
      List<ChangeStreamContinuationToken> changeStreamContinuationTokens,
      Instant parentLowWatermark,
      List<NewPartition> parentPartitions) {
    this(partition, changeStreamContinuationTokens, "", parentLowWatermark, parentPartitions, null);
  }

  public PartitionRecord(
      ByteStringRange partition,
      Instant startTime,
      Instant parentLowWatermark,
      List<NewPartition> parentPartitions) {
    this(partition, startTime, "", parentLowWatermark, parentPartitions, null);
  }

  public PartitionRecord(
      ByteStringRange partition,
      Instant startTime,
      String uuid,
      Instant parentLowWatermark,
      List<NewPartition> parentPartitions,
      @Nullable Instant endTime) {
    this.partition = partition;
    this.startTime = startTime;
    this.uuid = uuid;
    this.parentLowWatermark = parentLowWatermark;
    this.endTime = endTime;
    this.parentPartitions = parentPartitions;
  }

  public PartitionRecord(
      ByteStringRange partition,
      List<ChangeStreamContinuationToken> changeStreamContinuationTokens,
      String uuid,
      Instant parentLowWatermark,
      List<NewPartition> parentPartitions,
      @Nullable Instant endTime) {
    this.partition = partition;
    this.changeStreamContinuationTokens = changeStreamContinuationTokens;
    this.uuid = uuid;
    this.parentLowWatermark = parentLowWatermark;
    this.endTime = endTime;
    this.parentPartitions = parentPartitions;
  }

  @Nullable
  public Instant getStartTime() {
    return startTime;
  }

  public String getUuid() {
    return uuid;
  }

  public Instant getParentLowWatermark() {
    return parentLowWatermark;
  }

  public ByteStringRange getPartition() {
    return partition;
  }

  @Nullable
  public List<ChangeStreamContinuationToken> getChangeStreamContinuationTokens() {
    return changeStreamContinuationTokens;
  }

  @Nullable
  public Instant getEndTime() {
    return endTime;
  }

  public List<NewPartition> getParentPartitions() {
    return parentPartitions;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public void setEndTime(@Nullable Instant endTime) {
    this.endTime = endTime;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionRecord)) {
      return false;
    }
    PartitionRecord that = (PartitionRecord) o;
    return Objects.equals(getPartition(), that.getPartition())
        && Objects.equals(getStartTime(), that.getStartTime())
        && Objects.equals(
            getChangeStreamContinuationTokens(), that.getChangeStreamContinuationTokens())
        && Objects.equals(getEndTime(), that.getEndTime())
        && Objects.equals(getUuid(), that.getUuid())
        && Objects.equals(getParentLowWatermark(), that.getParentLowWatermark())
        && Objects.equals(parentPartitions, that.parentPartitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getPartition(),
        getStartTime(),
        getChangeStreamContinuationTokens(),
        getEndTime(),
        getUuid(),
        getParentLowWatermark(),
        parentPartitions);
  }

  @Override
  public String toString() {
    return "PartitionRecord{"
        + "partition="
        + formatByteStringRange(partition)
        + ", startTime="
        + startTime
        + ", changeStreamContinuationTokens="
        + changeStreamContinuationTokens
        + ", endTime="
        + endTime
        + ", uuid='"
        + uuid
        + '\''
        + ", parentLowWatermark="
        + parentLowWatermark
        + ", parentPartitions="
        + parentPartitions
        + '}';
  }
}
