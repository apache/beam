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
import java.util.HashSet;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** Model for the partition metadata database table used in the Connector. */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class PartitionMetadata implements Serializable {

  private static final long serialVersionUID = 995720273301116075L;

  /**
   * The state at which a partition can be in the system:
   *
   * <ul>
   *   <li>CREATED: the partition has been created, but no query has been done against it yet.
   *   <li>SCHEDULED: the partition has been scheduled to start the query.
   *   <li>RUNNING: the partition is currently being processed and a change stream query is made
   *       against it.
   *   <li>FINISHED: all the records from the partition have been consumed and the change stream
   *       query has finished.
   * </ul>
   */
  @DefaultCoder(AvroCoder.class)
  public enum State {
    // The partition has been discovered and is waiting to be started
    CREATED,
    // The partition has been scheduled to be processed
    SCHEDULED,
    // The partition has started being processed
    RUNNING,
    // The partition has finished processing
    FINISHED
  }

  private String partitionToken;
  private HashSet<String> parentTokens;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp startTimestamp;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp endTimestamp;

  private long heartbeatMillis;
  private State state;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp watermark;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp createdAt;

  @Nullable
  @org.apache.avro.reflect.Nullable
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp scheduledAt;

  @Nullable
  @org.apache.avro.reflect.Nullable
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp runningAt;

  @Nullable
  @org.apache.avro.reflect.Nullable
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp finishedAt;

  /** Default constructor for serialization only. */
  private PartitionMetadata() {}

  public PartitionMetadata(
      String partitionToken,
      HashSet<String> parentTokens,
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      long heartbeatMillis,
      State state,
      Timestamp watermark,
      Timestamp createdAt,
      @Nullable Timestamp scheduledAt,
      @Nullable Timestamp runningAt,
      @Nullable Timestamp finishedAt) {
    this.partitionToken = partitionToken;
    this.parentTokens = parentTokens;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.heartbeatMillis = heartbeatMillis;
    this.state = state;
    this.watermark = watermark;
    this.createdAt = createdAt;
    this.scheduledAt = scheduledAt;
    this.runningAt = runningAt;
    this.finishedAt = finishedAt;
  }

  /** Unique partition identifier, which can be used to perform a change stream query. */
  public String getPartitionToken() {
    return partitionToken;
  }

  /**
   * The unique partition identifiers of the parent partitions where this child partition originated
   * from.
   */
  public HashSet<String> getParentTokens() {
    return parentTokens;
  }

  /**
   * It is the start time at which the partition started existing in Cloud Spanner. This timestamp
   * can be used to perform a change stream query for the partition.
   */
  public Timestamp getStartTimestamp() {
    return startTimestamp;
  }

  /**
   * The end time for querying this given partition. It does not necessarily mean that the partition
   * exists until this time, but it will be the timestamp used on its change stream query.
   */
  public Timestamp getEndTimestamp() {
    return endTimestamp;
  }

  /**
   * The number of milliseconds after the stream is idle, which a heartbeat record will be emitted
   * in the change stream query.
   */
  public long getHeartbeatMillis() {
    return heartbeatMillis;
  }

  /** The state in which the current partition is in. */
  public State getState() {
    return state;
  }

  /** The time for which all records with a timestamp less than it have been processed. */
  public Timestamp getWatermark() {
    return watermark;
  }

  /** The time at which this partition was first detected and created in the metadata table. */
  public Timestamp getCreatedAt() {
    return createdAt;
  }

  /** The time at which this partition was scheduled to be queried. */
  public @Nullable Timestamp getScheduledAt() {
    return scheduledAt;
  }

  /** The time at which the connector started processing this partition. */
  public @Nullable Timestamp getRunningAt() {
    return runningAt;
  }

  /** The time at which the connector finished processing this partition. */
  public @Nullable Timestamp getFinishedAt() {
    return finishedAt;
  }

  /** Transforms the instance into a builder, so field values can be modified. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionMetadata)) {
      return false;
    }
    PartitionMetadata that = (PartitionMetadata) o;
    return heartbeatMillis == that.heartbeatMillis
        && Objects.equals(partitionToken, that.partitionToken)
        && Objects.equals(parentTokens, that.parentTokens)
        && Objects.equals(startTimestamp, that.startTimestamp)
        && Objects.equals(endTimestamp, that.endTimestamp)
        && state == that.state
        && Objects.equals(watermark, that.watermark)
        && Objects.equals(createdAt, that.createdAt)
        && Objects.equals(scheduledAt, that.scheduledAt)
        && Objects.equals(runningAt, that.runningAt)
        && Objects.equals(finishedAt, that.finishedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        partitionToken,
        parentTokens,
        startTimestamp,
        endTimestamp,
        heartbeatMillis,
        state,
        watermark,
        createdAt,
        scheduledAt,
        runningAt,
        finishedAt);
  }

  @Override
  public String toString() {
    return "PartitionMetadata{"
        + "partitionToken='"
        + partitionToken
        + '\''
        + ", parentTokens="
        + parentTokens
        + ", startTimestamp="
        + startTimestamp
        + ", endTimestamp="
        + endTimestamp
        + ", heartbeatMillis="
        + heartbeatMillis
        + ", state="
        + state
        + ", watermark="
        + watermark
        + ", createdAt="
        + createdAt
        + ", scheduledAt="
        + scheduledAt
        + ", runningAt="
        + runningAt
        + ", finishedAt="
        + finishedAt
        + '}';
  }

  /** Creates a builder for constructing a partition metadata instance. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** Partition metadata builder for better user experience. Defaults for all fields are nulls. */
  public static class Builder {

    private String partitionToken;
    private HashSet<String> parentTokens;
    private Timestamp startTimestamp;
    private Timestamp endTimestamp;
    private Long heartbeatMillis;
    private State state;
    private Timestamp watermark;
    private Timestamp createdAt;
    @Nullable private Timestamp scheduledAt;
    @Nullable private Timestamp runningAt;
    @Nullable private Timestamp finishedAt;

    public Builder() {}

    private Builder(PartitionMetadata partition) {
      this.partitionToken = partition.partitionToken;
      this.parentTokens = partition.parentTokens;
      this.startTimestamp = partition.startTimestamp;
      this.endTimestamp = partition.endTimestamp;
      this.heartbeatMillis = partition.heartbeatMillis;
      this.state = partition.state;
      this.watermark = partition.watermark;
      this.createdAt = partition.createdAt;
      this.scheduledAt = partition.scheduledAt;
      this.runningAt = partition.runningAt;
      this.finishedAt = partition.finishedAt;
    }

    /** Sets the unique partition identifier. */
    public Builder setPartitionToken(String partitionToken) {
      this.partitionToken = partitionToken;
      return this;
    }

    /** Sets the collection of parent partition identifiers. */
    public Builder setParentTokens(HashSet<String> parentTokens) {
      this.parentTokens = parentTokens;
      return this;
    }

    /** Sets the start time of the partition. */
    public Builder setStartTimestamp(Timestamp startTimestamp) {
      this.startTimestamp = startTimestamp;
      return this;
    }

    /** Sets the end time of the partition. */
    public Builder setEndTimestamp(Timestamp endTimestamp) {
      this.endTimestamp = endTimestamp;
      return this;
    }

    /** Sets the heartbeat interval in millis. */
    public Builder setHeartbeatMillis(long heartbeatMillis) {
      this.heartbeatMillis = heartbeatMillis;
      return this;
    }

    /** Sets the current state of the partition. */
    public Builder setState(State state) {
      this.state = state;
      return this;
    }

    /** Sets the watermark (last processed timestamp) for the partition. */
    public Builder setWatermark(Timestamp watermark) {
      this.watermark = watermark;
      return this;
    }

    /** Sets the time at which the partition was created. */
    public Builder setCreatedAt(Timestamp createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    /** Sets the time at which the partition was scheduled. */
    public Builder setScheduledAt(@Nullable Timestamp scheduledAt) {
      this.scheduledAt = scheduledAt;
      return this;
    }

    /** Sets the time at which the partition started running. */
    public Builder setRunningAt(@Nullable Timestamp runningAt) {
      this.runningAt = runningAt;
      return this;
    }

    /** Sets the time at which the partition finished running. */
    public Builder setFinishedAt(@Nullable Timestamp finishedAt) {
      this.finishedAt = finishedAt;
      return this;
    }

    /**
     * Builds a {@link PartitionMetadata} from the given fields. Mandatory fields are:
     *
     * <ul>
     *   <li>partition token
     *   <li>parent tokens
     *   <li>start timestamp
     *   <li>heartbeat millis
     *   <li>state
     *   <li>watermark
     * </ul>
     */
    public PartitionMetadata build() {
      Preconditions.checkState(partitionToken != null, "partitionToken");
      Preconditions.checkState(parentTokens != null, "parentTokens");
      Preconditions.checkState(startTimestamp != null, "startTimestamp");
      Preconditions.checkState(heartbeatMillis != null, "heartbeatMillis");
      Preconditions.checkState(state != null, "state");
      Preconditions.checkState(watermark != null, "watermark");
      if (createdAt == null) {
        createdAt = Value.COMMIT_TIMESTAMP;
      }
      return new PartitionMetadata(
          partitionToken,
          parentTokens,
          startTimestamp,
          endTimestamp,
          heartbeatMillis,
          state,
          watermark,
          createdAt,
          scheduledAt,
          runningAt,
          finishedAt);
    }
  }
}
