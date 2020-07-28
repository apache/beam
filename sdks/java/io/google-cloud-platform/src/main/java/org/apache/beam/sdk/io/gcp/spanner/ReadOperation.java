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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Encapsulates a spanner read operation. */
@AutoValue
public abstract class ReadOperation implements Serializable {

  public static ReadOperation create() {
    return new AutoValue_ReadOperation.Builder()
        .setPartitionOptions(PartitionOptions.getDefaultInstance())
        .setKeySet(KeySet.all())
        .build();
  }

  public abstract @Nullable Statement getQuery();

  public abstract @Nullable String getTable();

  public abstract @Nullable String getIndex();

  public abstract @Nullable List<String> getColumns();

  public abstract @Nullable KeySet getKeySet();

  abstract @Nullable PartitionOptions getPartitionOptions();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setQuery(Statement statement);

    abstract Builder setTable(String table);

    abstract Builder setIndex(String index);

    abstract Builder setColumns(List<String> columns);

    abstract Builder setKeySet(KeySet keySet);

    abstract Builder setPartitionOptions(PartitionOptions partitionOptions);

    abstract ReadOperation build();
  }

  abstract Builder toBuilder();

  public ReadOperation withTable(String table) {
    return toBuilder().setTable(table).build();
  }

  public ReadOperation withColumns(String... columns) {
    return withColumns(Arrays.asList(columns));
  }

  public ReadOperation withColumns(List<String> columns) {
    return toBuilder().setColumns(columns).build();
  }

  public ReadOperation withQuery(Statement statement) {
    return toBuilder().setQuery(statement).build();
  }

  public ReadOperation withQuery(String sql) {
    return withQuery(Statement.of(sql));
  }

  public ReadOperation withKeySet(KeySet keySet) {
    return toBuilder().setKeySet(keySet).build();
  }

  public ReadOperation withIndex(String index) {
    return toBuilder().setIndex(index).build();
  }

  public ReadOperation withPartitionOptions(PartitionOptions partitionOptions) {
    return toBuilder().setPartitionOptions(partitionOptions).build();
  }
}
