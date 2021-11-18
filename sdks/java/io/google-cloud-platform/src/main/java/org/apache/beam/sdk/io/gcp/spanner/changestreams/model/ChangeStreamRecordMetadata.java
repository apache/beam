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
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** Holds internal execution metrics / metadata for the processed {@link ChangeStreamRecord}. */
public class ChangeStreamRecordMetadata implements Serializable {

  private static final long serialVersionUID = -7294067549709034080L;

  private String partitionToken;
  private Timestamp recordTimestamp;
  private Timestamp partitionStartTimestamp;
  private Timestamp partitionEndTimestamp;
  private Timestamp partitionCreatedAt;
  private Timestamp partitionScheduledAt;
  private Timestamp partitionRunningAt;
  private Timestamp queryStartedAt;
  private Timestamp recordStreamStartedAt;
  private Timestamp recordStreamEndedAt;
  private Timestamp recordReadAt;
  private long totalStreamTimeMillis;
  private long numberOfRecordsRead;

  /** Default constructor for serialization only. */
  private ChangeStreamRecordMetadata() {}

  @VisibleForTesting
  ChangeStreamRecordMetadata(
      String partitionToken,
      Timestamp recordTimestamp,
      Timestamp partitionStartTimestamp,
      Timestamp partitionEndTimestamp,
      Timestamp partitionCreatedAt,
      Timestamp partitionScheduledAt,
      Timestamp partitionRunningAt,
      Timestamp queryStartedAt,
      Timestamp recordStreamStartedAt,
      Timestamp recordStreamEndedAt,
      Timestamp recordReadAt,
      long totalStreamTimeMillis,
      long numberOfRecordsRead) {
    this.partitionToken = partitionToken;
    this.recordTimestamp = recordTimestamp;

    this.partitionStartTimestamp = partitionStartTimestamp;
    this.partitionEndTimestamp = partitionEndTimestamp;
    this.partitionCreatedAt = partitionCreatedAt;
    this.partitionScheduledAt = partitionScheduledAt;
    this.partitionRunningAt = partitionRunningAt;

    this.queryStartedAt = queryStartedAt;
    this.recordStreamStartedAt = recordStreamStartedAt;
    this.recordStreamEndedAt = recordStreamEndedAt;
    this.recordReadAt = recordReadAt;
    this.totalStreamTimeMillis = totalStreamTimeMillis;
    this.numberOfRecordsRead = numberOfRecordsRead;
  }

  /** @return the partition token that produced this change stream record */
  public String getPartitionToken() {
    return partitionToken;
  }

  /** @return the Cloud Spanner timestamp time when this record occurred */
  public Timestamp getRecordTimestamp() {
    return recordTimestamp;
  }

  /** @return the start time for the partition change stream query, which produced this record */
  public Timestamp getPartitionStartTimestamp() {
    return partitionStartTimestamp;
  }

  /** @return the end time for the partition change stream query, which produced this record */
  public Timestamp getPartitionEndTimestamp() {
    return partitionEndTimestamp;
  }

  /**
   * @return the time at which this partition was first detected and created in the metadata table
   */
  public Timestamp getPartitionCreatedAt() {
    return partitionCreatedAt;
  }

  /** @return the time at which this partition was scheduled to be queried */
  public Timestamp getPartitionScheduledAt() {
    return partitionScheduledAt;
  }

  /** @return the time at which the connector started processing this partition */
  public Timestamp getPartitionRunningAt() {
    return partitionRunningAt;
  }

  /** @return the time that the change stream query which produced this record started */
  public Timestamp getQueryStartedAt() {
    return queryStartedAt;
  }

  /** @return the time at which the record started to be streamed */
  public Timestamp getRecordStreamStartedAt() {
    return recordStreamStartedAt;
  }

  /** @return the time at which the record finished streaming */
  public Timestamp getRecordStreamEndedAt() {
    return recordStreamEndedAt;
  }

  /** @return the time at which the record was fully read */
  public Timestamp getRecordReadAt() {
    return recordReadAt;
  }

  /** @return the total streaming time (in millis) for this record */
  public long getTotalStreamTimeMillis() {
    return totalStreamTimeMillis;
  }

  /**
   * @return the number of records read in the partition change stream query before reading this
   *     record
   */
  public long getNumberOfRecordsRead() {
    return numberOfRecordsRead;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ChangeStreamRecordMetadata)) {
      return false;
    }
    ChangeStreamRecordMetadata metadata = (ChangeStreamRecordMetadata) o;
    return totalStreamTimeMillis == metadata.totalStreamTimeMillis
        && numberOfRecordsRead == metadata.numberOfRecordsRead
        && Objects.equals(partitionToken, metadata.partitionToken)
        && Objects.equals(recordTimestamp, metadata.recordTimestamp)
        && Objects.equals(partitionStartTimestamp, metadata.partitionStartTimestamp)
        && Objects.equals(partitionEndTimestamp, metadata.partitionEndTimestamp)
        && Objects.equals(partitionCreatedAt, metadata.partitionCreatedAt)
        && Objects.equals(partitionScheduledAt, metadata.partitionScheduledAt)
        && Objects.equals(partitionRunningAt, metadata.partitionRunningAt)
        && Objects.equals(queryStartedAt, metadata.queryStartedAt)
        && Objects.equals(recordStreamStartedAt, metadata.recordStreamStartedAt)
        && Objects.equals(recordStreamEndedAt, metadata.recordStreamEndedAt)
        && Objects.equals(recordReadAt, metadata.recordReadAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        partitionToken,
        recordTimestamp,
        partitionStartTimestamp,
        partitionEndTimestamp,
        partitionCreatedAt,
        partitionScheduledAt,
        partitionRunningAt,
        queryStartedAt,
        recordStreamStartedAt,
        recordStreamEndedAt,
        recordReadAt,
        totalStreamTimeMillis,
        numberOfRecordsRead);
  }

  @Override
  public String toString() {
    return "ChangeStreamRecordMetadata{"
        + "partitionToken='"
        + partitionToken
        + '\''
        + ", recordTimestamp="
        + recordTimestamp
        + ", partitionStartTimestamp="
        + partitionStartTimestamp
        + ", partitionEndTimestamp="
        + partitionEndTimestamp
        + ", partitionCreatedAt="
        + partitionCreatedAt
        + ", partitionScheduledAt="
        + partitionScheduledAt
        + ", partitionRunningAt="
        + partitionRunningAt
        + ", queryStartedAt="
        + queryStartedAt
        + ", recordStreamStartedAt="
        + recordStreamStartedAt
        + ", recordStreamEndedAt="
        + recordStreamEndedAt
        + ", recordReadAt="
        + recordReadAt
        + ", totalStreamTimeMillis="
        + totalStreamTimeMillis
        + ", numberOfRecordsRead="
        + numberOfRecordsRead
        + '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String partitionToken;
    private Timestamp recordTimestamp;
    private Timestamp partitionStartTimestamp;
    private Timestamp partitionEndTimestamp;
    private Timestamp partitionCreatedAt;
    private Timestamp partitionScheduledAt;
    private Timestamp partitionRunningAt;
    private Timestamp queryStartedAt;
    private Timestamp recordStreamStartedAt;
    private Timestamp recordStreamEndedAt;
    private Timestamp recordReadAt;
    private long totalStreamTimeMillis;
    private long numberOfRecordsRead;

    /**
     * Sets the partition token where this record originated from.
     *
     * @param partitionToken the partition token to be set
     * @return Builder
     */
    public Builder withPartitionToken(String partitionToken) {
      this.partitionToken = partitionToken;
      return this;
    }

    /**
     * Sets the timestamp of when this record occurred.
     *
     * @param recordTimestamp the timestamp to be set
     * @return Builder
     */
    public Builder withRecordTimestamp(Timestamp recordTimestamp) {
      this.recordTimestamp = recordTimestamp;
      return this;
    }

    /**
     * Sets the start time for the partition change stream query that originated this record.
     *
     * @param partitionStartTimestamp the timestamp to be set
     * @return Builder
     */
    public Builder withPartitionStartTimestamp(Timestamp partitionStartTimestamp) {
      this.partitionStartTimestamp = partitionStartTimestamp;
      return this;
    }

    /**
     * Sets the end time for the partition change stream query that originated this record.
     *
     * @param partitionEndTimestamp the timestamp to be set
     * @return Builder
     */
    public Builder withPartitionEndTimestamp(Timestamp partitionEndTimestamp) {
      this.partitionEndTimestamp = partitionEndTimestamp;
      return this;
    }

    /**
     * Sets the time at which this partition was first detected and created in the metadata table.
     *
     * @param partitionCreatedAt the timestamp to be set
     * @return Builder
     */
    public Builder withPartitionCreatedAt(Timestamp partitionCreatedAt) {
      this.partitionCreatedAt = partitionCreatedAt;
      return this;
    }

    /**
     * Sets the time at which this partition was scheduled to be queried.
     *
     * @param partitionScheduledAt the timestamp to be set
     * @return Builder
     */
    public Builder withPartitionScheduledAt(Timestamp partitionScheduledAt) {
      this.partitionScheduledAt = partitionScheduledAt;
      return this;
    }

    /**
     * Sets the time at which the connector started processing this partition.
     *
     * @param partitionRunningAt the timestamp to be set
     * @return Builder
     */
    public Builder withPartitionRunningAt(Timestamp partitionRunningAt) {
      this.partitionRunningAt = partitionRunningAt;
      return this;
    }

    /**
     * Sets the time that the change stream query which produced this record started.
     *
     * @param queryStartedAt the timestamp to be set
     * @return Builder
     */
    public Builder withQueryStartedAt(Timestamp queryStartedAt) {
      this.queryStartedAt = queryStartedAt;
      return this;
    }

    /**
     * Sets the time at which the record started to be streamed.
     *
     * @param recordStreamStartedAt the timestamp to be set
     * @return Builder
     */
    public Builder withRecordStreamStartedAt(Timestamp recordStreamStartedAt) {
      this.recordStreamStartedAt = recordStreamStartedAt;
      return this;
    }

    /**
     * Sets the time at which the record finished streaming.
     *
     * @param recordStreamEndedAt the timestamp to be set
     * @return Builder
     */
    public Builder withRecordStreamEndedAt(Timestamp recordStreamEndedAt) {
      this.recordStreamEndedAt = recordStreamEndedAt;
      return this;
    }

    /**
     * Sets the time at which the record was fully read.
     *
     * @param recordReadAt the timestamp to be set
     * @return Builder
     */
    public Builder withRecordReadAt(Timestamp recordReadAt) {
      this.recordReadAt = recordReadAt;
      return this;
    }

    /**
     * Sets the total streaming time (in millis) for this record.
     *
     * @param totalStreamTimeMillis the total time in millis
     * @return Builder
     */
    public Builder withTotalStreamTimeMillis(long totalStreamTimeMillis) {
      this.totalStreamTimeMillis = totalStreamTimeMillis;
      return this;
    }

    /**
     * Sets the number of records read in the partition change stream query before reading this
     * record.
     *
     * @param numberOfRecordsRead the number of records read
     * @return Builder
     */
    public Builder withNumberOfRecordsRead(long numberOfRecordsRead) {
      this.numberOfRecordsRead = numberOfRecordsRead;
      return this;
    }

    /**
     * Builds the {@link ChangeStreamRecordMetadata}
     *
     * @return ChangeStreamRecordMetadata
     */
    public ChangeStreamRecordMetadata build() {
      return new ChangeStreamRecordMetadata(
          partitionToken,
          recordTimestamp,
          partitionStartTimestamp,
          partitionEndTimestamp,
          partitionCreatedAt,
          partitionScheduledAt,
          partitionRunningAt,
          queryStartedAt,
          recordStreamStartedAt,
          recordStreamEndedAt,
          recordReadAt,
          totalStreamTimeMillis,
          numberOfRecordsRead);
    }
  }
}
