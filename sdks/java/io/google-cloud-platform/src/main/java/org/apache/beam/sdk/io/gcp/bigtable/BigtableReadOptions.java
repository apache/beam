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
import com.google.bigtable.v2.RowFilter;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration for which values to read from Bigtable. */
@AutoValue
abstract class BigtableReadOptions implements Serializable {

  /** Returns the row filter to use. */
  abstract @Nullable ValueProvider<RowFilter> getRowFilter();

  /** Returns the key ranges to read. */
  abstract @Nullable ValueProvider<List<ByteKeyRange>> getKeyRanges();

  abstract Builder toBuilder();

  static BigtableReadOptions.Builder builder() {
    return new AutoValue_BigtableReadOptions.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setRowFilter(ValueProvider<RowFilter> rowFilter);

    abstract Builder setKeyRanges(ValueProvider<List<ByteKeyRange>> keyRanges);

    abstract BigtableReadOptions build();
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

  void populateDisplayData(DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("rowFilter", getRowFilter()).withLabel("Row Filter"))
        .addIfNotNull(DisplayData.item("keyRanges", getKeyRanges()).withLabel("Key Ranges"));
  }

  void validate() {
    if (getRowFilter() != null && getRowFilter().isAccessible()) {
      checkArgument(getRowFilter().get() != null, "rowFilter can not be null");
    }

    if (getKeyRanges() != null && getKeyRanges().isAccessible()) {
      checkArgument(getKeyRanges().get() != null, "keyRanges can not be null");
      checkArgument(!getKeyRanges().get().isEmpty(), "keyRanges can not be empty");
      for (ByteKeyRange range : getKeyRanges().get()) {
        checkArgument(range != null, "keyRanges cannot hold null range");
      }
    }
  }
}
