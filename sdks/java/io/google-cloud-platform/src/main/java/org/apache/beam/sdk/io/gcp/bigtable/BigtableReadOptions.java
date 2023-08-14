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
import com.google.bigtable.v2.RowFilter;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Configuration for read from Bigtable. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class BigtableReadOptions implements Serializable {

  /** Returns the table id. */
  abstract ValueProvider<String> getTableId();

  /** Returns the row filter to use. */
  abstract @Nullable ValueProvider<RowFilter> getRowFilter();

  /** Returns the key ranges to read. */
  abstract @Nullable ValueProvider<List<ByteKeyRange>> getKeyRanges();

  /** Returns the size limit for reading segements. */
  abstract @Nullable Integer getMaxBufferElementCount();

  /** Returns the attempt timeout of the reads. */
  abstract @Nullable Duration getAttemptTimeout();

  /** Returns the operation timeout of the reads. */
  abstract @Nullable Duration getOperationTimeout();

  /**
   * Watchdog will kill the stream after waiting this much time for the next response from server.
   */
  abstract @Nullable Duration getWaitTimeout();

  abstract Builder toBuilder();

  static BigtableReadOptions.Builder builder() {
    return new AutoValue_BigtableReadOptions.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTableId(ValueProvider<String> tableId);

    abstract Builder setRowFilter(ValueProvider<RowFilter> rowFilter);

    abstract Builder setMaxBufferElementCount(@Nullable Integer maxBufferElementCount);

    abstract Builder setKeyRanges(ValueProvider<List<ByteKeyRange>> keyRanges);

    abstract Builder setAttemptTimeout(Duration timeout);

    abstract Builder setOperationTimeout(Duration timeout);

    abstract Builder setWaitTimeout(Duration timeout);

    abstract BigtableReadOptions build();
  }

  BigtableReadOptions setMaxBufferElementCount(@Nullable Integer maxBufferElementCount) {
    return toBuilder().setMaxBufferElementCount(maxBufferElementCount).build();
  }

  BigtableReadOptions withRowFilter(RowFilter rowFilter) {
    return toBuilder().setRowFilter(ValueProvider.StaticValueProvider.of(rowFilter)).build();
  }

  BigtableReadOptions withKeyRanges(List<ByteKeyRange> keyRanges) {
    return toBuilder().setKeyRanges(ValueProvider.StaticValueProvider.of(keyRanges)).build();
  }

  BigtableReadOptions withKeyRange(ByteKeyRange keyRange) {
    return withKeyRanges(Collections.singletonList(keyRange));
  }

  boolean isDataAccessible() {
    return getTableId() != null && getTableId().isAccessible();
  }

  void populateDisplayData(DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("tableId", getTableId()).withLabel("Bigtable Table Id"))
        .addIfNotNull(DisplayData.item("rowFilter", getRowFilter()).withLabel("Row Filter"))
        .addIfNotNull(DisplayData.item("keyRanges", getKeyRanges()).withLabel("Key Ranges"))
        .addIfNotNull(
            DisplayData.item("attemptTimeout", getAttemptTimeout())
                .withLabel("Read Attempt Timeout"))
        .addIfNotNull(
            DisplayData.item("operationTimeout", getOperationTimeout())
                .withLabel("Read Operation Timeout"));
  }

  void validate() {
    checkArgument(
        getTableId() != null && (!getTableId().isAccessible() || !getTableId().get().isEmpty()),
        "Could not obtain Bigtable table id");

    if (getRowFilter() != null && getRowFilter().isAccessible()) {
      checkArgument(getRowFilter().get() != null, "rowFilter can not be null");
    }
    if (getMaxBufferElementCount() != null) {
      checkArgument(
          getMaxBufferElementCount() > 0, "maxBufferElementCount can not be zero or negative");
    }

    if (getKeyRanges() != null && getKeyRanges().isAccessible()) {
      checkArgument(getKeyRanges().get() != null, "keyRanges can not be null");
      checkArgument(!getKeyRanges().get().isEmpty(), "keyRanges can not be empty");
      for (ByteKeyRange range : getKeyRanges().get()) {
        checkArgument(range != null, "keyRanges cannot hold null range");
      }
    }

    if (getAttemptTimeout() != null && getOperationTimeout() != null) {
      checkArgument(
          getAttemptTimeout().isShorterThan(getOperationTimeout()),
          "attempt timeout can't be longer than operation timeout");
    }
  }
}
