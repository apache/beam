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
package org.apache.beam.sdk.io.iceberg;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

@AutoValue
public abstract class IcebergScanConfig implements Serializable {

  private transient @MonotonicNonNull Table cachedTable;

  public enum ScanType {
    TABLE,
    BATCH
  }

  @Pure
  public abstract ScanType getScanType();

  @Pure
  public abstract IcebergCatalogConfig getCatalogConfig();

  @Pure
  public abstract String getTableIdentifier();

  @Pure
  public Table getTable() {
    if (cachedTable == null) {
      cachedTable =
          getCatalogConfig().catalog().loadTable(TableIdentifier.parse(getTableIdentifier()));
    }
    return cachedTable;
  }

  @Pure
  public abstract Schema getSchema();

  @Pure
  public abstract @Nullable Expression getFilter();

  @Pure
  public abstract @Nullable Boolean getCaseSensitive();

  @Pure
  public abstract ImmutableMap<String, String> getOptions();

  @Pure
  public abstract @Nullable Long getSnapshot();

  @Pure
  public abstract @Nullable Long getTimestamp();

  @Pure
  public abstract @Nullable Long getFromSnapshotInclusive();

  @Pure
  public abstract @Nullable String getFromSnapshotRefInclusive();

  @Pure
  public abstract @Nullable Long getFromSnapshotExclusive();

  @Pure
  public abstract @Nullable String getFromSnapshotRefExclusive();

  @Pure
  public abstract @Nullable Long getToSnapshot();

  @Pure
  public abstract @Nullable String getToSnapshotRef();

  @Pure
  public abstract @Nullable String getTag();

  @Pure
  public abstract @Nullable String getBranch();

  @Pure
  public static Builder builder() {
    return new AutoValue_IcebergScanConfig.Builder()
        .setScanType(ScanType.TABLE)
        .setFilter(null)
        .setCaseSensitive(null)
        .setOptions(ImmutableMap.of())
        .setSnapshot(null)
        .setTimestamp(null)
        .setFromSnapshotInclusive(null)
        .setFromSnapshotRefInclusive(null)
        .setFromSnapshotExclusive(null)
        .setFromSnapshotRefExclusive(null)
        .setToSnapshot(null)
        .setToSnapshotRef(null)
        .setTag(null)
        .setBranch(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setScanType(ScanType type);

    public abstract Builder setCatalogConfig(IcebergCatalogConfig catalog);

    public abstract Builder setTableIdentifier(String tableIdentifier);

    public Builder setTableIdentifier(TableIdentifier tableIdentifier) {
      return this.setTableIdentifier(tableIdentifier.toString());
    }

    public Builder setTableIdentifier(String... names) {
      return setTableIdentifier(TableIdentifier.of(names));
    }

    public abstract Builder setSchema(Schema schema);

    public abstract Builder setFilter(@Nullable Expression filter);

    public abstract Builder setCaseSensitive(@Nullable Boolean caseSensitive);

    public abstract Builder setOptions(ImmutableMap<String, String> options);

    public abstract Builder setSnapshot(@Nullable Long snapshot);

    public abstract Builder setTimestamp(@Nullable Long timestamp);

    public abstract Builder setFromSnapshotInclusive(@Nullable Long fromInclusive);

    public abstract Builder setFromSnapshotRefInclusive(@Nullable String ref);

    public abstract Builder setFromSnapshotExclusive(@Nullable Long fromExclusive);

    public abstract Builder setFromSnapshotRefExclusive(@Nullable String ref);

    public abstract Builder setToSnapshot(@Nullable Long snapshot);

    public abstract Builder setToSnapshotRef(@Nullable String ref);

    public abstract Builder setTag(@Nullable String tag);

    public abstract Builder setBranch(@Nullable String branch);

    public abstract IcebergScanConfig build();
  }
}
