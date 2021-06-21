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

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.common.annotations.VisibleForTesting;

// TODO: Add java docs
public class PartitionPosition implements Serializable {

  private static final long serialVersionUID = -9088898012221404492L;
  private final Optional<Timestamp> maybeTimestamp;
  private final PartitionMode mode;
  private final Optional<Long> maybeChildPartitionsToWaitFor;

  public static PartitionPosition queryChangeStream(Timestamp timestamp) {
    return new PartitionPosition(
        Optional.of(timestamp), PartitionMode.QUERY_CHANGE_STREAM, Optional.empty());
  }

  public static PartitionPosition waitForChildPartitions(long childPartitionsToWaitFor) {
    return new PartitionPosition(
        Optional.empty(),
        PartitionMode.WAIT_FOR_CHILD_PARTITIONS,
        Optional.of(childPartitionsToWaitFor));
  }

  public static PartitionPosition finishPartition() {
    return new PartitionPosition(
        Optional.empty(), PartitionMode.FINISH_PARTITION, Optional.empty());
  }

  public static PartitionPosition waitForParentPartitions() {
    return new PartitionPosition(
        Optional.empty(), PartitionMode.WAIT_FOR_PARENT_PARTITIONS, Optional.empty());
  }

  public static PartitionPosition deletePartition() {
    return new PartitionPosition(
        Optional.empty(), PartitionMode.DELETE_PARTITION, Optional.empty());
  }

  public static PartitionPosition done() {
    return new PartitionPosition(Optional.empty(), PartitionMode.DONE, Optional.empty());
  }

  @VisibleForTesting
  protected PartitionPosition(
      Optional<Timestamp> maybeTimestamp,
      PartitionMode mode,
      Optional<Long> maybeChildPartitionsToWaitFor) {
    this.maybeTimestamp = maybeTimestamp;
    this.mode = mode;
    this.maybeChildPartitionsToWaitFor = maybeChildPartitionsToWaitFor;
  }

  public Optional<Timestamp> getTimestamp() {
    return maybeTimestamp;
  }

  public PartitionMode getMode() {
    return mode;
  }

  public Optional<Long> getChildPartitionsToWaitFor() {
    return maybeChildPartitionsToWaitFor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionPosition)) {
      return false;
    }
    PartitionPosition that = (PartitionPosition) o;
    return Objects.equals(maybeTimestamp, that.maybeTimestamp)
        && mode == that.mode
        && Objects.equals(maybeChildPartitionsToWaitFor, that.maybeChildPartitionsToWaitFor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maybeTimestamp, mode, maybeChildPartitionsToWaitFor);
  }

  @Override
  public String toString() {
    return "PartitionPosition{"
        + "maybeTimestamp="
        + maybeTimestamp
        + ", mode="
        + mode
        + ", maybeChildPartitionsToWaitFor="
        + maybeChildPartitionsToWaitFor
        + '}';
  }
}
