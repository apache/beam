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
package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;

// TODO: Add java docs
public class PartitionRestriction implements Serializable {

  private static final long serialVersionUID = -7009236776208644264L;

  private final Timestamp startTimestamp;
  private final Timestamp endTimestamp;
  private final PartitionMode mode;
  private final PartitionMode stoppedMode;

  public static PartitionRestriction queryChangeStream(
      Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, QUERY_CHANGE_STREAM, null);
  }

  public static PartitionRestriction waitForChildPartitions(
      Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, WAIT_FOR_CHILD_PARTITIONS, null);
  }

  public static PartitionRestriction finishPartition(
      Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, FINISH_PARTITION, null);
  }

  public static PartitionRestriction waitForParentPartitions(
      Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, WAIT_FOR_PARENT_PARTITIONS, null);
  }

  public static PartitionRestriction deletePartition(
      Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, DELETE_PARTITION, null);
  }

  public static PartitionRestriction done(Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, DONE, null);
  }

  public static PartitionRestriction stop(PartitionRestriction restriction) {
    return new PartitionRestriction(
        restriction.getStartTimestamp(),
        restriction.getEndTimestamp(),
        STOP,
        restriction.getMode());
  }

  public PartitionRestriction(
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      PartitionMode mode,
      PartitionMode stoppedMode) {
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.mode = mode;
    this.stoppedMode = stoppedMode;
  }

  public Timestamp getStartTimestamp() {
    return startTimestamp;
  }

  public Timestamp getEndTimestamp() {
    return endTimestamp;
  }

  public PartitionMode getMode() {
    return mode;
  }

  public PartitionMode getStoppedMode() {
    return stoppedMode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionRestriction)) {
      return false;
    }
    PartitionRestriction that = (PartitionRestriction) o;
    return Objects.equals(startTimestamp, that.startTimestamp)
        && Objects.equals(endTimestamp, that.endTimestamp)
        && mode == that.mode
        && stoppedMode == that.stoppedMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTimestamp, endTimestamp, mode, stoppedMode);
  }

  @Override
  public String toString() {
    return "PartitionRestriction{"
        + "startTimestamp="
        + startTimestamp
        + ", endTimestamp="
        + endTimestamp
        + ", mode="
        + mode
        + ", stoppedMode="
        + stoppedMode
        + '}';
  }
}
