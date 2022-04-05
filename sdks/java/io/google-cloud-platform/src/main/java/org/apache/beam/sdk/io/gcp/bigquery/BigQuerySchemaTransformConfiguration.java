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
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Configurations for reading from or writing to BigQuery. */
public class BigQuerySchemaTransformConfiguration {
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();

  /**
   * Instantiates a {@link Read.Builder} from the SQL query.
   *
   * <p>The configuration defaults to useStandardSql=true.
   */
  public static Read.Builder createQueryBuilder(String query) {
    return new AutoValue_BigQuerySchemaTransformConfiguration_Read.Builder()
        .setJobType(JobType.QUERY)
        .setUseStandardSql(Read.DEFAULT_USE_STANDARD_SQL)
        .setQuery(query);
  }

  /**
   * Instantiates a {@link Read.Builder} to support BigQuery extract jobs. See {@link
   * BigQueryIO.TypedRead#from(String)} for the expected format.
   */
  public static Read.Builder createExtractBuilder(String tableSpec) {
    return new AutoValue_BigQuerySchemaTransformConfiguration_Read.Builder()
        .setJobType(JobType.EXTRACT)
        .setTableSpec(tableSpec);
  }

  /** Instantiates a {@link Read.Builder} to support BigQuery extract jobs. */
  public static Read.Builder createExtractBuilder(TableReference tableReference) {
    return createExtractBuilder(BigQueryHelpers.toTableSpec(tableReference));
  }

  /**
   * Instantiates a {@link Write.Builder} to support BigQuery load jobs. See {@link
   * BigQueryIO.Write#to(String)}} for toTableSpec expected format.
   */
  public static Write.Builder createLoadBuilder(
      String tableSpec, CreateDisposition createDisposition, WriteDisposition writeDisposition) {
    return new AutoValue_BigQuerySchemaTransformConfiguration_Write.Builder()
        .setTableSpec(tableSpec)
        .setCreateDisposition(createDisposition.name())
        .setWriteDisposition(writeDisposition.name());
  }

  /** Instantiates a {@link Write.Builder} to support BigQuery load jobs. */
  public static Write.Builder createLoadBuilder(
      TableReference toTable,
      CreateDisposition createDisposition,
      WriteDisposition writeDisposition) {
    return createLoadBuilder(
        BigQueryHelpers.toTableSpec(toTable), createDisposition, writeDisposition);
  }

  /**
   * Configuration for reading from BigQuery.
   *
   * <p>This class is meant to be used with {@link BigQuerySchemaTransformReadProvider}.
   *
   * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
   * provide no backwards compatibility guarantees, and it should not be implemented outside the
   * Beam repository.
   */
  @SuppressWarnings({
    "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
  })
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Read {
    private static final TypeDescriptor<Read> TYPE_DESCRIPTOR = TypeDescriptor.of(Read.class);
    private static final SerializableFunction<Read, Row> ROW_SERIALIZABLE_FUNCTION =
        AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

    private static final boolean DEFAULT_USE_STANDARD_SQL = true;

    /** Configures the BigQuery job type. */
    abstract JobType getJobType();

    /** Serializes configuration to a {@link Row}. */
    Row toBeamRow() {
      return ROW_SERIALIZABLE_FUNCTION.apply(this);
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

      /** Configures the BigQuery job type. */
      abstract Builder setJobType(JobType value);

      /** Configures the BigQuery read job with the SQL query. */
      public abstract Builder setQuery(String value);

      /**
       * Specifies a table for a BigQuery read job. See {@link BigQueryIO.TypedRead#from(String)}
       * for more details on the expected format.
       */
      public abstract Builder setTableSpec(String value);

      /** BigQuery geographic location where the query job will be executed. */
      public abstract Builder setQueryLocation(String value);

      /** Enables BigQuery's Standard SQL dialect when reading from a query. */
      public abstract Builder setUseStandardSql(Boolean value);

      /** Builds the {@link Read} configuration. */
      public abstract Read build();
    }
  }

  /**
   * Configuration for writing to BigQuery.
   *
   * <p>This class is meant to be used with {@link BigQuerySchemaTransformWriteProvider}.
   *
   * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
   * provide no backwards compatibility guarantees, and it should not be implemented outside the
   * Beam repository.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Write {
    private static final TypeDescriptor<BigQuerySchemaTransformConfiguration.Write>
        TYPE_DESCRIPTOR = TypeDescriptor.of(BigQuerySchemaTransformConfiguration.Write.class);
    private static final SerializableFunction<BigQuerySchemaTransformConfiguration.Write, Row>
        ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

    /**
     * Writes to the given table specification. See {@link BigQueryIO.Write#to(String)}} for the
     * expected format.
     */
    public abstract String getTableSpec();

    /** Specifies whether the table should be created if it does not exist. */
    public abstract String getCreateDisposition();

    /** Specifies what to do with existing data in the table, in case the table already exists. */
    public abstract String getWriteDisposition();

    /** Returns the {@link #getTableSpec()} as a {@link TableReference}. */
    TableReference getTableReference() {
      return BigQueryHelpers.parseTableSpec(getTableSpec());
    }

    /** Returns the {@link #getCreateDisposition()} as a {@link CreateDisposition}. */
    CreateDisposition getCreateDispositionEnum() {
      return CreateDisposition.valueOf(getCreateDisposition());
    }

    /** Serializes configuration to a {@link Row}. */
    Row toBeamRow() {
      return ROW_SERIALIZABLE_FUNCTION.apply(this);
    }

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * Writes to the given table specification. See {@link BigQueryIO.Write#to(String)}} for the
       * expected format.
       */
      public abstract Builder setTableSpec(String value);

      /** Specifies whether the table should be created if it does not exist. */
      public abstract Builder setCreateDisposition(String value);

      /** Specifies what to do with existing data in the table, in case the table already exists. */
      public abstract Builder setWriteDisposition(String value);

      /** Builds the {@link Write} configuration. */
      public abstract Write build();
    }
  }

  enum JobType {
    EXTRACT,
    QUERY,
    UNSPECIFIED,
  }
}
