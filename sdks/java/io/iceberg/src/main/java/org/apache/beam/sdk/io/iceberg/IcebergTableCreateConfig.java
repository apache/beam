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
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.iceberg.PartitionSpec;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

@AutoValue
public abstract class IcebergTableCreateConfig {

  /** Schema for the destination, in the event that it must be dynamically created. */
  @Pure
  public abstract Schema getSchema();

  /** Partition spec destination, in the event that it must be dynamically created. */
  @Pure
  public PartitionSpec getPartitionSpec() {
    return PartitionUtils.toPartitionSpec(getPartitionFields(), getSchema());
  }

  @Pure
  public abstract @Nullable List<String> getPartitionFields();

  @Pure
  public abstract @Nullable Map<String, String> getTableProperties();

  @Pure
  public static Builder builder() {
    return new AutoValue_IcebergTableCreateConfig.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSchema(Schema schema);

    public abstract Builder setPartitionFields(@Nullable List<String> partitionFields);

    public abstract Builder setTableProperties(@Nullable Map<String, String> tableProperties);

    @Pure
    public abstract IcebergTableCreateConfig build();
  }
}
