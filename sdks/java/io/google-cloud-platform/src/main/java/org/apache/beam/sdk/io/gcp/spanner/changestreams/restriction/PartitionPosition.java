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

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Represents the current position of the running SDF within PartitionRestriction. */
public class PartitionPosition implements Serializable {

  private static final long serialVersionUID = -9088898012221404492L;

  private final Optional<Timestamp> maybeTimestamp;
  private final PartitionMode mode;

  public static PartitionPosition updateState() {
    return new PartitionPosition(Optional.empty(), PartitionMode.UPDATE_STATE);
  }

  public static PartitionPosition queryChangeStream(Timestamp timestamp) {
    return new PartitionPosition(Optional.of(timestamp), PartitionMode.QUERY_CHANGE_STREAM);
  }

  public static PartitionPosition waitForChildPartitions() {
    return new PartitionPosition(Optional.empty(), PartitionMode.WAIT_FOR_CHILD_PARTITIONS);
  }

  public static PartitionPosition done() {
    return new PartitionPosition(Optional.empty(), PartitionMode.DONE);
  }

  public static PartitionPosition stop() {
    return new PartitionPosition(Optional.empty(), PartitionMode.STOP);
  }

  @VisibleForTesting
  public PartitionPosition(Optional<Timestamp> maybeTimestamp, PartitionMode mode) {
    this.maybeTimestamp = maybeTimestamp;
    this.mode = mode;
  }

  public Optional<Timestamp> getTimestamp() {
    return maybeTimestamp;
  }

  public PartitionMode getMode() {
    return mode;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionPosition)) {
      return false;
    }
    PartitionPosition that = (PartitionPosition) o;
    return Objects.equals(maybeTimestamp, that.maybeTimestamp) && mode == that.mode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(maybeTimestamp, mode);
  }

  @Override
  public String toString() {
    return "PartitionPosition{" + "maybeTimestamp=" + maybeTimestamp + ", mode=" + mode + '}';
  }
}
