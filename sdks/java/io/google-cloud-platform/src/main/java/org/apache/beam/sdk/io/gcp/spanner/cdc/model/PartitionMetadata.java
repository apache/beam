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
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.HashSet;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Model for the partition metadata database table used in the Connector. */
@DefaultCoder(AvroCoder.class)
public class PartitionMetadata implements Serializable {

  private static final long serialVersionUID = 995720273301116075L;

  @DefaultCoder(AvroCoder.class)
  public enum State {
    // The partition has been discovered and is waiting to be started
    CREATED,
    // The partition has started and is being processed
    SCHEDULED,
    // The partition has ended
    FINISHED
  }

  // Unique partition token, obtained from the Child Partition record from the Change Streams API
  // call.
  private String partitionToken;
  // Unique partition token of the parents that generated this partition.
  // This needs to be an implementation (HashSet), instead of the Set interface, otherwise
  // we can not encode / decode this with Avro.
  private HashSet<String> parentTokens;
  // Start timestamp, used to query the partition.
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp startTimestamp;
  // Whether the start timestamp is inclusive or exclusive.
  private boolean inclusiveStart;
  // The end timestamp, used to query the partition
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp endTimestamp;
  // Whether the end timestamp is inclusive or exclusive.
  private boolean inclusiveEnd;
  // The interval for a heartbeat record to be returned for a partition when there are no changes
  // within the partition.
  private long heartbeatMillis;
  // The current state of the partition in the Connector.
  private State state;
  // When the row was inserted.
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp createdAt;
  // When the row was updated.
  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp updatedAt;

  /** Default constructor for serialization only. */
  private PartitionMetadata() {}

  public PartitionMetadata(
      String partitionToken,
      HashSet<String> parentTokens,
      Timestamp startTimestamp,
      boolean inclusiveStart,
      Timestamp endTimestamp,
      boolean inclusiveEnd,
      long heartbeatMillis,
      State state,
      Timestamp createdAt,
      Timestamp updatedAt) {
    this.partitionToken = partitionToken;
    this.parentTokens = parentTokens;
    this.startTimestamp = startTimestamp;
    this.inclusiveStart = inclusiveStart;
    this.endTimestamp = endTimestamp;
    this.inclusiveEnd = inclusiveEnd;
    this.heartbeatMillis = heartbeatMillis;
    this.state = state;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
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

  public boolean isInclusiveStart() {
    return inclusiveStart;
  }

  public Timestamp getEndTimestamp() {
    return endTimestamp;
  }

  public boolean isInclusiveEnd() {
    return inclusiveEnd;
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

  public Timestamp getUpdatedAt() {
    return updatedAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionMetadata partitionMetadata = (PartitionMetadata) o;
    return isInclusiveStart() == partitionMetadata.isInclusiveStart()
        && isInclusiveEnd() == partitionMetadata.isInclusiveEnd()
        && getHeartbeatMillis() == partitionMetadata.getHeartbeatMillis()
        && Objects.equal(getPartitionToken(), partitionMetadata.getPartitionToken())
        && Objects.equal(getParentTokens(), partitionMetadata.getParentTokens())
        && Objects.equal(getStartTimestamp(), partitionMetadata.getStartTimestamp())
        && Objects.equal(getEndTimestamp(), partitionMetadata.getEndTimestamp())
        && getState() == partitionMetadata.getState()
        && Objects.equal(getCreatedAt(), partitionMetadata.getCreatedAt())
        && Objects.equal(getUpdatedAt(), partitionMetadata.getUpdatedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getPartitionToken(),
        getParentTokens(),
        getStartTimestamp(),
        isInclusiveStart(),
        getEndTimestamp(),
        isInclusiveEnd(),
        getHeartbeatMillis(),
        getState(),
        getCreatedAt(),
        getUpdatedAt());
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
        + ", inclusiveStart="
        + inclusiveStart
        + ", endTimestamp="
        + endTimestamp
        + ", inclusiveEnd="
        + inclusiveEnd
        + ", heartbeatMillis="
        + heartbeatMillis
        + ", state="
        + state
        + ", createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }

  public static PartitionMetadata.Builder newBuilder() {
    return new PartitionMetadata.Builder();
  }

  public static class Builder {

    private String partitionToken;
    private HashSet<String> parentTokens;
    private Timestamp startTimestamp;
    private Boolean inclusiveStart;
    private Timestamp endTimestamp;
    private Boolean inclusiveEnd;
    private Long heartbeatMillis;
    private State state;
    private Timestamp createdAt;
    private Timestamp updatedAt;

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

    public Builder setInclusiveStart(boolean inclusiveStart) {
      this.inclusiveStart = inclusiveStart;
      return this;
    }

    public Builder setEndTimestamp(Timestamp endTimestamp) {
      this.endTimestamp = endTimestamp;
      return this;
    }

    public Builder setInclusiveEnd(Boolean inclusiveEnd) {
      this.inclusiveEnd = inclusiveEnd;
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

    public Builder setUpdatedAt(Timestamp updatedAt) {
      this.updatedAt = updatedAt;
      return this;
    }

    public PartitionMetadata build() {
      Preconditions.checkState(partitionToken != null, "partitionToken");
      Preconditions.checkState(parentTokens != null, "parentTokens");
      Preconditions.checkState(startTimestamp != null, "startTimestamp");
      Preconditions.checkState(heartbeatMillis != null, "heartbeatMillis");
      Preconditions.checkState(state != null, "state");
      // TODO: Add test for default inclusive start
      if (inclusiveStart == null) {
        inclusiveStart = true;
      }
      // TODO: Add test for default inclusive end
      if (inclusiveEnd == null) {
        inclusiveEnd = false;
      }
      // TODO: Add test for default created at
      if (createdAt == null) {
        createdAt = Value.COMMIT_TIMESTAMP;
      }
      // TODO: Add test for default updated at
      if (updatedAt == null) {
        updatedAt = Value.COMMIT_TIMESTAMP;
      }
      return new PartitionMetadata(
          partitionToken,
          parentTokens,
          startTimestamp,
          inclusiveStart,
          endTimestamp,
          inclusiveEnd,
          heartbeatMillis,
          state,
          createdAt,
          updatedAt);
    }
  }
}
