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
package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

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

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp partitionScheduledAt;

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

  public String getPartitionToken() {
    return partitionToken;
  }

  public Timestamp getRecordTimestamp() {
    return recordTimestamp;
  }

  public Timestamp getPartitionStartTimestamp() {
    return partitionStartTimestamp;
  }

  public Timestamp getPartitionEndTimestamp() {
    return partitionEndTimestamp;
  }

  public Timestamp getPartitionCreatedAt() {
    return partitionCreatedAt;
  }

  public Timestamp getPartitionScheduledAt() {
    return partitionScheduledAt;
  }

  public Timestamp getPartitionRunningAt() {
    return partitionRunningAt;
  }

  public Timestamp getQueryStartedAt() {
    return queryStartedAt;
  }

  public Timestamp getRecordStreamStartedAt() {
    return recordStreamStartedAt;
  }

  public Timestamp getRecordStreamEndedAt() {
    return recordStreamEndedAt;
  }

  public Timestamp getRecordReadAt() {
    return recordReadAt;
  }

  public long getTotalStreamTimeMillis() {
    return totalStreamTimeMillis;
  }

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

    public Builder withPartitionToken(String partitionToken) {
      this.partitionToken = partitionToken;
      return this;
    }

    public Builder withRecordTimestamp(Timestamp recordTimestamp) {
      this.recordTimestamp = recordTimestamp;
      return this;
    }

    public Builder withPartitionStartTimestamp(Timestamp partitionStartTimestamp) {
      this.partitionStartTimestamp = partitionStartTimestamp;
      return this;
    }

    public Builder withPartitionEndTimestamp(Timestamp partitionEndTimestamp) {
      this.partitionEndTimestamp = partitionEndTimestamp;
      return this;
    }

    public Builder withPartitionCreatedAt(Timestamp partitionCreatedAt) {
      this.partitionCreatedAt = partitionCreatedAt;
      return this;
    }

    public Builder withPartitionScheduledAt(Timestamp partitionScheduledAt) {
      this.partitionScheduledAt = partitionScheduledAt;
      return this;
    }

    public Builder withPartitionRunningAt(Timestamp partitionRunningAt) {
      this.partitionRunningAt = partitionRunningAt;
      return this;
    }

    public Builder withQueryStartedAt(Timestamp queryStartedAt) {
      this.queryStartedAt = queryStartedAt;
      return this;
    }

    public Builder withRecordStreamStartedAt(Timestamp recordStreamStartedAt) {
      this.recordStreamStartedAt = recordStreamStartedAt;
      return this;
    }

    public Builder withRecordStreamEndedAt(Timestamp recordStreamEndedAt) {
      this.recordStreamEndedAt = recordStreamEndedAt;
      return this;
    }

    public Builder withRecordReadAt(Timestamp recordReadAt) {
      this.recordReadAt = recordReadAt;
      return this;
    }

    public Builder withTotalStreamTimeMillis(long totalStreamTimeMillis) {
      this.totalStreamTimeMillis = totalStreamTimeMillis;
      return this;
    }

    public Builder withNumberOfRecordsRead(long numberOfRecordsRead) {
      this.numberOfRecordsRead = numberOfRecordsRead;
      return this;
    }

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
