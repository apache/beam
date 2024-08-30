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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Configuration for write to Bigtable. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class BigtableWriteOptions implements Serializable {

  /** Returns the table id. */
  abstract @Nullable ValueProvider<String> getTableId();

  /** Returns the attempt timeout for writes. */
  abstract @Nullable Duration getAttemptTimeout();

  /** Returns the operation timeout for writes. */
  abstract @Nullable Duration getOperationTimeout();

  /** Returns the max number of elements of a batch. */
  abstract @Nullable Long getMaxElementsPerBatch();

  /** Returns the max number of bytes of a batch. */
  abstract @Nullable Long getMaxBytesPerBatch();

  /** Returns the max number of concurrent elements allowed before enforcing flow control. */
  abstract @Nullable Long getMaxOutstandingElements();

  /** Returns the max number of concurrent bytes allowed before enforcing flow control. */
  abstract @Nullable Long getMaxOutstandingBytes();

  /** Returns the target latency if latency based throttling is enabled. */
  abstract @Nullable Integer getThrottlingTargetMs();

  /** Returns the target latency if latency based throttling report to runner is enabled. */
  abstract @Nullable Integer getThrottlingReportTargetMs();

  /** Returns true if batch write flow control is enabled. Otherwise return false. */
  abstract @Nullable Boolean getFlowControl();

  /** Returns the time to wait when closing the writer. */
  abstract @Nullable Duration getCloseWaitTimeout();

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_BigtableWriteOptions.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTableId(ValueProvider<String> tableId);

    abstract Builder setAttemptTimeout(Duration timeout);

    abstract Builder setOperationTimeout(Duration timeout);

    abstract Builder setMaxElementsPerBatch(long size);

    abstract Builder setMaxBytesPerBatch(long bytes);

    abstract Builder setMaxOutstandingElements(long count);

    abstract Builder setMaxOutstandingBytes(long bytes);

    abstract Builder setThrottlingTargetMs(int targetMs);

    abstract Builder setThrottlingReportTargetMs(int targetMs);

    abstract Builder setFlowControl(boolean enableFlowControl);

    abstract Builder setCloseWaitTimeout(Duration timeout);

    abstract BigtableWriteOptions build();
  }

  boolean isDataAccessible() {
    return getTableId() != null && getTableId().isAccessible();
  }

  void populateDisplayData(DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("tableId", getTableId()).withLabel("Bigtable Table Id"))
        .addIfNotNull(
            DisplayData.item("attemptTimeout", getAttemptTimeout())
                .withLabel("Write Attempt Timeout"))
        .addIfNotNull(
            DisplayData.item("operationTimeout", getOperationTimeout())
                .withLabel("Write Operation Timeout"))
        .addIfNotNull(
            DisplayData.item("maxElementsPerBatch", getMaxElementsPerBatch())
                .withLabel("Write batch element count"))
        .addIfNotNull(
            DisplayData.item("maxBytesPerBatch", getMaxBytesPerBatch())
                .withLabel("Write batch byte size"))
        .addIfNotNull(
            DisplayData.item("maxOutstandingElements", getMaxOutstandingElements())
                .withLabel("Write max outstanding elements"))
        .addIfNotNull(
            DisplayData.item("maxOutstandingBytes", getMaxOutstandingBytes())
                .withLabel("Write max outstanding bytes"))
        .addIfNotNull(
            DisplayData.item("flowControl", getFlowControl())
                .withLabel("Write flow control is enabled"));
  }

  void validate() {
    checkArgument(
        getTableId() != null && (!getTableId().isAccessible() || !getTableId().get().isEmpty()),
        "Could not obtain Bigtable table id");

    if (getAttemptTimeout() != null && getOperationTimeout() != null) {
      checkArgument(
          getAttemptTimeout().isShorterThan(getOperationTimeout()),
          "attempt timeout can't be longer than operation timeout");
    }
  }
}
