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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Configuration for reading from BigQuery.
 *
 * <p>This class is meant to be used with {@link BigQueryExportReadSchemaTransformProvider}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQueryExportReadSchemaTransformConfiguration {

  /** Instantiates a {@link BigQueryExportReadSchemaTransformConfiguration.Builder}. */
  public static Builder builder() {
    return new AutoValue_BigQueryExportReadSchemaTransformConfiguration.Builder();
  }

  /** Configures the BigQuery read job with the SQL query. */
  @Nullable
  public abstract String getQuery();

  /**
   * Specifies a table for a BigQuery read job. See {@link BigQueryIO.TypedRead#from(String)} for
   * more details on the expected format.
   */
  @Nullable
  public abstract String getTableSpec();

  /** BigQuery geographic location where the query job will be executed. */
  @Nullable
  public abstract String getQueryLocation();

  /** Enables BigQuery's Standard SQL dialect when reading from a query. */
  @Nullable
  public abstract Boolean getUseStandardSql();

  @AutoValue.Builder
  public abstract static class Builder {

    /** Configures the BigQuery read job with the SQL query. */
    public abstract Builder setQuery(String value);

    /**
     * Specifies a table for a BigQuery read job. See {@link BigQueryIO.TypedRead#from(String)} for
     * more details on the expected format.
     */
    public abstract Builder setTableSpec(String value);

    /** BigQuery geographic location where the query job will be executed. */
    public abstract Builder setQueryLocation(String value);

    /** Enables BigQuery's Standard SQL dialect when reading from a query. */
    public abstract Builder setUseStandardSql(Boolean value);

    /** Builds the {@link BigQueryExportReadSchemaTransformConfiguration} configuration. */
    public abstract BigQueryExportReadSchemaTransformConfiguration build();
  }
}
