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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.UPDATE_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Represents the restriction for PartitionRestrictionTracker. */
@SuppressWarnings("initialization.fields.uninitialized")
public class PartitionRestriction implements Serializable {

  private static final long serialVersionUID = -7009236776208644264L;

  private final Timestamp startTimestamp;
  private final Timestamp endTimestamp;
  private final PartitionMode mode;
  private final @Nullable PartitionMode stoppedMode;
  private PartitionRestrictionMetadata metadata;

  public static PartitionRestriction updateState(Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, UPDATE_STATE, null);
  }

  public static PartitionRestriction queryChangeStream(
      Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, QUERY_CHANGE_STREAM, null);
  }

  public static PartitionRestriction waitForChildPartitions(
      Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, WAIT_FOR_CHILD_PARTITIONS, null);
  }

  public static PartitionRestriction done(Timestamp startTimestamp, Timestamp endTimestamp) {
    return new PartitionRestriction(startTimestamp, endTimestamp, DONE, null);
  }

  public static PartitionRestriction stop(PartitionRestriction restriction) {
    return new PartitionRestriction(
            restriction.getStartTimestamp(),
            restriction.getEndTimestamp(),
            STOP,
            restriction.getMode())
        .withMetadata(restriction.getMetadata());
  }

  public PartitionRestriction(
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      PartitionMode mode,
      @Nullable PartitionMode stoppedMode) {
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.mode = mode;
    this.stoppedMode = stoppedMode;
  }

  public PartitionRestriction withMetadata(PartitionRestrictionMetadata metadata) {
    this.metadata = metadata;
    return this;
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

  public @Nullable PartitionMode getStoppedMode() {
    return stoppedMode;
  }

  public PartitionRestrictionMetadata getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(@Nullable Object o) {
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
        + ", metadata="
        + metadata
        + '}';
  }
}
