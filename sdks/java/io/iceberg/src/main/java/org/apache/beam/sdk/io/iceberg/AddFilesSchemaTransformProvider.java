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

import static org.apache.beam.sdk.io.iceberg.AddFilesSchemaTransformProvider.Configuration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@AutoService(SchemaTransformProvider.class)
public class AddFilesSchemaTransformProvider extends TypedSchemaTransformProvider<Configuration> {
  @Override
  public AddFiles from(Configuration configuration) {
    @Nullable Integer frequency = configuration.getTriggeringFrequencySeconds();

    return new AddFiles(
        configuration.getIcebergCatalog(),
        configuration.getTable(),
        configuration.getLocationPrefix(),
        configuration.getAppendBatchSize(),
        frequency != null ? Duration.standardSeconds(frequency) : null);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:iceberg_add_files:v1";
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    public static Builder builder() {
      return new AutoValue_AddFilesSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("A fully-qualified table identifier.")
    public abstract String getTable();

    @SchemaFieldDescription("Properties used to set up the Iceberg catalog.")
    public abstract @Nullable Map<String, String> getCatalogProperties();

    @SchemaFieldDescription("Properties passed to the Hadoop ")
    public abstract @Nullable Map<String, String> getConfigProperties();

    @SchemaFieldDescription(
        "For a streaming pipeline, sets the frequency at which incoming files are appended. Defaults to 600 (10 minutes). "
            + "A commit is triggered when either this or append batch size is reached.")
    public abstract @Nullable Integer getTriggeringFrequencySeconds();

    @SchemaFieldDescription(
        "For a streaming pipeline, sets the desired number of appended files per commit. Defaults to 100,000 files. "
            + "A commit is triggered when either this or append triggering interval is reached.")
    public abstract @Nullable Integer getAppendBatchSize();

    @SchemaFieldDescription(
        "The prefix shared among all partitions. For example, a data file may have the following"
            + " location:\n"
            + "'file:/Users/user/Documents/tmp/namespace/table_name/data/id=13/name=beam/data_file.parquet'\n\n"
            + "The provided prefix should go up until the partition information:\n"
            + "'file:/Users/user/Documents/tmp/namespace/table_name/data/'.\n"
            + "Required if the table is partitioned. ")
    public abstract @Nullable String getLocationPrefix();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      public abstract Builder setConfigProperties(Map<String, String> confProperties);

      public abstract Builder setTriggeringFrequencySeconds(Integer triggeringFrequencySeconds);

      public abstract Builder setAppendBatchSize(Integer size);

      public abstract Builder setLocationPrefix(String prefix);

      public abstract Configuration build();
    }

    public IcebergCatalogConfig getIcebergCatalog() {
      return IcebergCatalogConfig.builder()
          .setCatalogProperties(getCatalogProperties())
          .setConfigProperties(getConfigProperties())
          .build();
    }
  }
}
