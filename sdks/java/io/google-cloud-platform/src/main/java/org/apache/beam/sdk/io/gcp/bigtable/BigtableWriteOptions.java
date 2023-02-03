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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration for write to Bigtable. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class BigtableWriteOptions implements Serializable {

  /** Returns the table id. */
  abstract @Nullable ValueProvider<String> getTableId();

  /** Returns the attempt timeout for writes. */
  abstract @Nullable Long getAttemptTimeout();

  /** Returns the operation timeout for writes. */
  abstract @Nullable Long getOperationTimeout();

  /** Returns the retry delay. */
  abstract @Nullable Long getRetryInitialDelay();

  /** Returns the retry delay multiplier. */
  abstract @Nullable Double getRetryDelayMultiplier();

  /** Returns the number of elements of a batch. */
  abstract @Nullable Long getBatchElements();

  /** Returns the number of bytes of a batch. */
  abstract @Nullable Long getBatchBytes();

  /** Returns the max number of concurrent requests allowed. */
  abstract @Nullable Long getMaxRequests();

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_BigtableWriteOptions.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTableId(ValueProvider<String> tableId);

    abstract Builder setAttemptTimeout(long timeout);

    abstract Builder setOperationTimeout(long timeout);

    abstract Builder setRetryInitialDelay(long delay);

    abstract Builder setRetryDelayMultiplier(double multiplier);

    abstract Builder setBatchElements(long size);

    abstract Builder setBatchBytes(long bytes);

    abstract Builder setMaxRequests(long count);

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
            DisplayData.item("retryInitialDelay", getRetryInitialDelay())
                .withLabel("Write retry initial delay"))
        .addIfNotNull(
            DisplayData.item("retryDelayMultiplier", getRetryDelayMultiplier())
                .withLabel("Write retry delay multiplier"))
        .addIfNotNull(
            DisplayData.item("batchELements", getBatchElements())
                .withLabel("Write batch element count"))
        .addIfNotNull(
            DisplayData.item("batchBytes", getBatchBytes()).withLabel("Write batch byte size"))
        .addIfNotNull(
            DisplayData.item("maxRequests", getMaxRequests())
                .withLabel("Write max concurrent requests"));
  }

  void validate() {
    checkArgument(
        getTableId() != null && (!getTableId().isAccessible() || !getTableId().get().isEmpty()),
        "Could not obtain Bigtable table id");

    if (getAttemptTimeout() != null && getOperationTimeout() != null) {
      checkArgument(
          getAttemptTimeout() <= getOperationTimeout(),
          "attempt timeout can't be greater than operation timeout");
    }
  }
}
