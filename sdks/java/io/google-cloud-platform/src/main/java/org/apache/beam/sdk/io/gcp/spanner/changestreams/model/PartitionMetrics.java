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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Model for the partition metadata database table used in the Connector. */
@DefaultCoder(AvroCoder.class)
public class PartitionMetrics implements Serializable {

  private static final long serialVersionUID = -4423522387253932074L;

  // Unique partition token, obtained from the Child Partition record from the Change Streams API
  // call.
  private String partitionToken;
  // When the row was inserted.
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp createdAt;
  // When the partition was scheduled
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp scheduledAt;
  // When the partition started running
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp runningAt;
  // When the partition finished
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp finishedAt;
  // When the partition was deleted
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp deletedAt;
  // When the query started
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp queryStartedAt;
  // Number of records processed
  private Long recordsProcessed;
  // When the latest record was processed
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp lastProcessedAt;
  // When the last update was
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp lastUpdatedAt;

  /** Default constructor for serialization only. */
  private PartitionMetrics() {}

  public PartitionMetrics(
      String partitionToken,
      Timestamp createdAt,
      Timestamp scheduledAt,
      Timestamp runningAt,
      Timestamp finishedAt,
      Timestamp deletedAt,
      Timestamp queryStartedAt,
      Long recordsProcessed,
      Timestamp lastProcessedAt,
      Timestamp lastUpdatedAt) {
    this.partitionToken = partitionToken;
    this.createdAt = createdAt;
    this.scheduledAt = scheduledAt;
    this.runningAt = runningAt;
    this.finishedAt = finishedAt;
    this.deletedAt = deletedAt;
    this.queryStartedAt = queryStartedAt;
    this.recordsProcessed = recordsProcessed;
    this.lastProcessedAt = lastProcessedAt;
    this.lastUpdatedAt = lastUpdatedAt;
  }

  public String getPartitionToken() {
    return partitionToken;
  }

  public Timestamp getCreatedAt() {
    return createdAt;
  }

  public Timestamp getScheduledAt() {
    return scheduledAt;
  }

  public Timestamp getRunningAt() {
    return runningAt;
  }

  public Timestamp getFinishedAt() {
    return finishedAt;
  }

  public Timestamp getDeletedAt() {
    return deletedAt;
  }

  public Timestamp getQueryStartedAt() {
    return queryStartedAt;
  }

  public Long getRecordsProcessed() {
    return recordsProcessed;
  }

  public Timestamp getLastProcessedAt() {
    return lastProcessedAt;
  }

  public Timestamp getLastUpdatedAt() {
    return lastUpdatedAt;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionMetrics)) {
      return false;
    }
    PartitionMetrics that = (PartitionMetrics) o;
    return partitionToken.equals(that.partitionToken)
        && createdAt.equals(that.createdAt)
        && Objects.equals(scheduledAt, that.scheduledAt)
        && Objects.equals(runningAt, that.runningAt)
        && Objects.equals(finishedAt, that.finishedAt)
        && Objects.equals(deletedAt, that.deletedAt)
        && Objects.equals(queryStartedAt, that.queryStartedAt)
        && Objects.equals(recordsProcessed, that.recordsProcessed)
        && Objects.equals(lastProcessedAt, that.lastProcessedAt)
        && lastUpdatedAt.equals(that.lastUpdatedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        partitionToken,
        createdAt,
        scheduledAt,
        runningAt,
        finishedAt,
        deletedAt,
        queryStartedAt,
        recordsProcessed,
        lastProcessedAt,
        lastUpdatedAt);
  }

  @Override
  public String toString() {
    return "PartitionMetrics{"
        + "partitionToken='"
        + partitionToken
        + '\''
        + ", createdAt="
        + createdAt
        + ", scheduledAt="
        + scheduledAt
        + ", runningAt="
        + runningAt
        + ", finishedAt="
        + finishedAt
        + ", deletedAt="
        + deletedAt
        + ", queryStartedAt="
        + queryStartedAt
        + ", recordsProcessed="
        + recordsProcessed
        + ", lastProcessedAt="
        + lastProcessedAt
        + ", lastUpdatedAt="
        + lastUpdatedAt
        + '}';
  }

  public static PartitionMetrics.Builder newBuilder() {
    return new PartitionMetrics.Builder();
  }

  public static class Builder {

    private String partitionToken;
    private Timestamp createdAt;
    private Timestamp scheduledAt;
    private Timestamp runningAt;
    private Timestamp finishedAt;
    private Timestamp deletedAt;
    private Timestamp queryStartedAt;
    private Long recordsProcessed;
    private Timestamp lastProcessedAt;
    private Timestamp lastUpdatedAt;

    public Builder() {}

    private Builder(PartitionMetrics partition) {
      this.partitionToken = partition.partitionToken;
      this.createdAt = partition.createdAt;
      this.scheduledAt = partition.scheduledAt;
      this.runningAt = partition.runningAt;
      this.finishedAt = partition.finishedAt;
      this.deletedAt = partition.deletedAt;
      this.queryStartedAt = partition.queryStartedAt;
      this.recordsProcessed = partition.recordsProcessed;
      this.lastProcessedAt = partition.lastProcessedAt;
      this.lastUpdatedAt = partition.lastUpdatedAt;
    }

    public Builder setPartitionToken(String partitionToken) {
      this.partitionToken = partitionToken;
      return this;
    }

    public Builder setCreatedAt(Timestamp createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public Builder setScheduledAt(Timestamp scheduledAt) {
      this.scheduledAt = scheduledAt;
      return this;
    }

    public Builder setRunningAt(Timestamp runningAt) {
      this.runningAt = runningAt;
      return this;
    }

    public Builder setFinishedAt(Timestamp finishedAt) {
      this.finishedAt = finishedAt;
      return this;
    }

    public Builder setDeletedAt(Timestamp deletedAt) {
      this.deletedAt = deletedAt;
      return this;
    }

    public Builder setQueryStartedAt(Timestamp queryStartedAt) {
      this.queryStartedAt = queryStartedAt;
      return this;
    }

    public Builder setRecordsProcessed(Long recordsProcessed) {
      this.recordsProcessed = recordsProcessed;
      return this;
    }

    public Builder setLastProcessedAt(Timestamp lastProcessedAt) {
      this.lastProcessedAt = lastProcessedAt;
      return this;
    }

    public Builder setLastUpdatedAt(Timestamp lastUpdatedAt) {
      this.lastUpdatedAt = lastUpdatedAt;
      return this;
    }

    public PartitionMetrics build() {
      Preconditions.checkState(partitionToken != null, "partitionToken");
      if (createdAt == null) {
        createdAt = Value.COMMIT_TIMESTAMP;
      }
      if (lastUpdatedAt == null) {
        lastUpdatedAt = Value.COMMIT_TIMESTAMP;
      }
      return new PartitionMetrics(
          partitionToken,
          createdAt,
          scheduledAt,
          runningAt,
          finishedAt,
          deletedAt,
          queryStartedAt,
          recordsProcessed,
          lastProcessedAt,
          lastUpdatedAt);
    }
  }
}
