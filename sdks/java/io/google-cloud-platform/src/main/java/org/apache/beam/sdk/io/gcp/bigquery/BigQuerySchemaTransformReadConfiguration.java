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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;

/**
 * Configuration for reading from BigQuery.
 *
 * <p>This class is meant to be used with {@link BigQuerySchemaTransformReadProvider}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQuerySchemaTransformReadConfiguration {

  private static final boolean DEFAULT_USE_STANDARD_SQL = true;

  /**
   * Instantiates a {@link BigQuerySchemaTransformReadConfiguration.Builder} from the SQL query.
   *
   * <p>The configuration defaults to useStandardSql=true.
   */
  public static Builder createQueryBuilder(String query) {
    return defaultBuilder().setQuery(query).setJobType(JobType.QUERY);
  }

  /**
   * Instantiates a {@link BigQuerySchemaTransformReadConfiguration.Builder} to support BigQuery
   * extract jobs. See the getTableSpec() getter for details.
   */
  public static Builder createExtractBuilder(String tableSpec) {
    return defaultBuilder().setTableSpec(tableSpec).setJobType(JobType.EXTRACT);
  }

  /**
   * Instantiates a {@link BigQuerySchemaTransformReadConfiguration.Builder} to support BigQuery
   * extract jobs.
   */
  public static Builder createExtractBuilder(TableReference tableSpec) {
    if (tableSpec.getProjectId().isEmpty()) {
      return createExtractBuilder(
          String.format("%s.%s", tableSpec.getDatasetId(), tableSpec.getTableId()));
    }
    return createExtractBuilder(
        String.format(
            "%s:%s.%s",
            tableSpec.getProjectId(), tableSpec.getDatasetId(), tableSpec.getTableId()));
  }

  private static Builder defaultBuilder() {
    return new AutoValue_BigQuerySchemaTransformReadConfiguration.Builder()
        .setJobType(JobType.UNSPECIFIED)
        .setQuery("")
        .setQueryLocation("")
        .setTableSpec("")
        .setUseStandardSql(DEFAULT_USE_STANDARD_SQL);
  }

  /** Configures the BigQuery job type. */
  abstract JobType getJobType();

  /** Configures the BigQuery read job with the SQL query. */
  public abstract String getQuery();

  /**
   * Specifies a table for a BigQuery read job as "[project_id]:[dataset_id].[table_id]" or
   * "[dataset_id].[table_id]" for tables within the current project.
   */
  public abstract String getTableSpec();

  /** BigQuery geographic location where the query job will be executed. */
  public abstract String getQueryLocation();

  /** Enables BigQuery's Standard SQL dialect when reading from a query. */
  public abstract Boolean getUseStandardSql();

  /** Instantiates a {@link BigQueryIO.TypedRead} from the configuration. */
  public BigQueryIO.TypedRead<TableRow> toTypedRead() {
    JobType jobType = getJobType();
    switch (jobType) {
      case QUERY:
        return toQueryTypedRead();

      case EXTRACT:
        return toExtractTypedRead();

      default:
        throw new InvalidConfigurationException(
            String.format("invalid job type for BigQueryIO read, got: %s", jobType));
    }
  }

  private BigQueryIO.TypedRead<TableRow> toQueryTypedRead() {
    BigQueryIO.TypedRead<TableRow> read =
        BigQueryIO.readTableRowsWithSchema().fromQuery(getQuery());

    if (!getQueryLocation().isEmpty()) {
      read = read.withQueryLocation(getQueryLocation());
    }

    if (getUseStandardSql()) {
      read = read.usingStandardSql();
    }

    return read;
  }

  private BigQueryIO.TypedRead<TableRow> toExtractTypedRead() {
    return BigQueryIO.readTableRowsWithSchema().from(getTableSpec());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /** Configures the BigQuery job type. */
    abstract Builder setJobType(JobType value);

    /** Configures the BigQuery read job with the SQL query. */
    public abstract Builder setQuery(String value);

    /**
     * Specifies a table for a BigQuery read job as "[project_id]:[dataset_id].[table_id]" or
     * "[dataset_id].[table_id]" for tables within the current project.
     */
    public abstract Builder setTableSpec(String value);

    /** BigQuery geographic location where the query job will be executed. */
    public abstract Builder setQueryLocation(String value);

    /** Enables BigQuery's Standard SQL dialect when reading from a query. */
    public abstract Builder setUseStandardSql(Boolean value);

    /** Builds the {@link BigQuerySchemaTransformReadConfiguration}. */
    public abstract BigQuerySchemaTransformReadConfiguration build();
  }

  /** An enumeration of expected BigQuery read job types. */
  enum JobType {
    UNSPECIFIED,
    QUERY,
    EXTRACT,
  }
}
