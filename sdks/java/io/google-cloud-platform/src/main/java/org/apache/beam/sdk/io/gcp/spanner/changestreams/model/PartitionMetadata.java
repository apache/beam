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
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Model for the partition metadata database table used in the Connector. */
@DefaultCoder(AvroCoder.class)
public class PartitionMetadata implements Serializable {

  private static final long serialVersionUID = 995720273301116075L;

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

  // Unique partition token, obtained from the Child Partition record from the Change Streams API
  // call.
  private String partitionToken;
  // Unique partition token of the parents that generated this partition.
  // This needs to be an implementation (HashSet), instead of the Set interface, otherwise
  // we can not encode / decode this with Avro.
  private HashSet<String> parentTokens;
  // Inclusive start timestamp, used to query the partition.
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp startTimestamp;
  // Inclusive end timestamp, used to query the partition
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp endTimestamp;
  // The interval for a heartbeat record to be returned for a partition when there are no changes
  // within the partition.
  private long heartbeatMillis;
  // The current state of the partition in the Connector.
  private State state;
  // When the row was inserted.
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp createdAt;
  // When the partition was scheduled
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp scheduledAt;
  // When the partition started running
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp runningAt;
  // When the partition finished running
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
      Timestamp createdAt,
      Timestamp scheduledAt,
      Timestamp runningAt,
      Timestamp finishedAt) {
    this.partitionToken = partitionToken;
    this.parentTokens = parentTokens;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.heartbeatMillis = heartbeatMillis;
    this.state = state;
    this.createdAt = createdAt;
    this.scheduledAt = scheduledAt;
    this.runningAt = runningAt;
    this.finishedAt = finishedAt;
  }

  public String getPartitionToken() {
    return partitionToken;
  }

  public HashSet<String> getParentTokens() {
    return parentTokens;
  }

  public Timestamp getStartTimestamp() {
    return startTimestamp;
  }

  public Timestamp getEndTimestamp() {
    return endTimestamp;
  }

  public long getHeartbeatMillis() {
    return heartbeatMillis;
  }

  public State getState() {
    return state;
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

  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override
  public boolean equals(Object o) {
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

  public static PartitionMetadata.Builder newBuilder() {
    return new PartitionMetadata.Builder();
  }

  public static class Builder {

    private String partitionToken;
    private HashSet<String> parentTokens;
    private Timestamp startTimestamp;
    private Timestamp endTimestamp;
    private Long heartbeatMillis;
    private State state;
    private Timestamp createdAt;
    private Timestamp scheduledAt;
    private Timestamp runningAt;
    private Timestamp finishedAt;

    public Builder() {}

    private Builder(PartitionMetadata partition) {
      this.partitionToken = partition.partitionToken;
      this.parentTokens = partition.parentTokens;
      this.startTimestamp = partition.startTimestamp;
      this.endTimestamp = partition.endTimestamp;
      this.heartbeatMillis = partition.heartbeatMillis;
      this.state = partition.state;
      this.createdAt = partition.createdAt;
      this.scheduledAt = partition.scheduledAt;
      this.runningAt = partition.runningAt;
      this.finishedAt = partition.finishedAt;
    }

    public Builder setPartitionToken(String partitionToken) {
      this.partitionToken = partitionToken;
      return this;
    }

    public Builder setParentTokens(HashSet<String> parentTokens) {
      this.parentTokens = parentTokens;
      return this;
    }

    public Builder setStartTimestamp(Timestamp startTimestamp) {
      this.startTimestamp = startTimestamp;
      return this;
    }

    public Builder setEndTimestamp(Timestamp endTimestamp) {
      this.endTimestamp = endTimestamp;
      return this;
    }

    public Builder setHeartbeatMillis(long heartbeatMillis) {
      this.heartbeatMillis = heartbeatMillis;
      return this;
    }

    public Builder setState(State state) {
      this.state = state;
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

    public PartitionMetadata build() {
      Preconditions.checkState(partitionToken != null, "partitionToken");
      Preconditions.checkState(parentTokens != null, "parentTokens");
      Preconditions.checkState(startTimestamp != null, "startTimestamp");
      Preconditions.checkState(heartbeatMillis != null, "heartbeatMillis");
      Preconditions.checkState(state != null, "state");
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
          createdAt,
          scheduledAt,
          runningAt,
          finishedAt);
    }
  }
}
