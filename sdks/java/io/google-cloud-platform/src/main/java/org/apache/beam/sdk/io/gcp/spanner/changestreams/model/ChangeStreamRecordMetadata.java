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
import javax.annotation.Nullable;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** Holds internal execution metrics / metadata for the processed {@link ChangeStreamRecord}. */
@SuppressWarnings({
  "initialization.field.uninitialized", // Avro requires the default constructor
  "initialization.fields.uninitialized"
})
@DefaultCoder(AvroCoder.class)
public class ChangeStreamRecordMetadata implements Serializable {

  private static final long serialVersionUID = -7294067549709034080L;

  private String partitionToken;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp recordTimestamp;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp partitionStartTimestamp;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp partitionEndTimestamp;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp partitionCreatedAt;

  @Nullable
  @org.apache.avro.reflect.Nullable
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp partitionScheduledAt;

  @Nullable
  @org.apache.avro.reflect.Nullable
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp partitionRunningAt;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp queryStartedAt;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp recordStreamStartedAt;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp recordStreamEndedAt;

  @AvroEncode(using = TimestampEncoding.class)
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
      @Nullable Timestamp partitionScheduledAt,
      @Nullable Timestamp partitionRunningAt,
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

  /** The partition token that produced this change stream record. */
  public String getPartitionToken() {
    return partitionToken;
  }

  /** The Cloud Spanner timestamp time when this record occurred. */
  public Timestamp getRecordTimestamp() {
    return recordTimestamp;
  }

  /** The start time for the partition change stream query, which produced this record. */
  public Timestamp getPartitionStartTimestamp() {
    return partitionStartTimestamp;
  }

  /** The end time for the partition change stream query, which produced this record. */
  public Timestamp getPartitionEndTimestamp() {
    return partitionEndTimestamp;
  }

  /** The time at which this partition was first detected and created in the metadata table. */
  public Timestamp getPartitionCreatedAt() {
    return partitionCreatedAt;
  }

  /** The time at which this partition was scheduled to be queried. */
  public @Nullable Timestamp getPartitionScheduledAt() {
    return partitionScheduledAt;
  }

  /** The time at which the connector started processing this partition. */
  public @Nullable Timestamp getPartitionRunningAt() {
    return partitionRunningAt;
  }

  /** The time that the change stream query which produced this record started. */
  public Timestamp getQueryStartedAt() {
    return queryStartedAt;
  }

  /** The time at which the record started to be streamed. */
  public Timestamp getRecordStreamStartedAt() {
    return recordStreamStartedAt;
  }

  /** The time at which the record finished streaming. */
  public Timestamp getRecordStreamEndedAt() {
    return recordStreamEndedAt;
  }

  /** The time at which the record was fully read. */
  public Timestamp getRecordReadAt() {
    return recordReadAt;
  }

  /** The total streaming time (in millis) for this record. */
  public long getTotalStreamTimeMillis() {
    return totalStreamTimeMillis;
  }

  /** The number of records read in the partition change stream query before reading this record. */
  public long getNumberOfRecordsRead() {
    return numberOfRecordsRead;
  }

  @Override
  public boolean equals(@Nullable Object o) {
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
    @Nullable private Timestamp partitionScheduledAt;
    @Nullable private Timestamp partitionRunningAt;
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
    public Builder withPartitionScheduledAt(@Nullable Timestamp partitionScheduledAt) {
      this.partitionScheduledAt = partitionScheduledAt;
      return this;
    }

    /**
     * Sets the time at which the connector started processing this partition.
     *
     * @param partitionRunningAt the timestamp to be set
     * @return Builder
     */
    public Builder withPartitionRunningAt(@Nullable Timestamp partitionRunningAt) {
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
     * Builds the {@link ChangeStreamRecordMetadata}.
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
