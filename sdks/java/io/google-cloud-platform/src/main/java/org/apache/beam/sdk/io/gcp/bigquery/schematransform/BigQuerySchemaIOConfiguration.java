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
package org.apache.beam.sdk.io.gcp.bigquery.schematransform;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;

/**
 * Configuration for the {@link org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaIOProvider}.
 *
 * <p><b>Internal only:</b> This is actively being worked on and will likely change.
 * We provide no backwards compatibility guarantees, and it should not be implemented outside the
 * Beam repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQuerySchemaIOConfiguration {

  private static final String API = "bigquery";
  private static final String VERSION = "v2";

  /** Unique identifier for this configuration. **/
  public static final String IDENTIFIER =
      String.format("%s:%s", API, VERSION);

  /** Returns a Builder for a BigQuery Query Job type. **/
  public static Builder builderOfQueryType(String query) {
    return builderOf(JobType.QUERY).setQuery(query);
  }

  /** Returns a Builder for a BigQuery Extract Job type. **/
  public static Builder builderOfExtractType(String tableSpec) {
    return builderOf(JobType.EXTRACT).setTableSpec(tableSpec);
  }

  /** Returns a Builder for a BigQuery Extract Job type. **/
  public static Builder builderOfExtractType(TableReference tableReference) {
    return builderOfExtractType(tableReference.toString());
  }

  // TODO: Add required load parameter values.
  /** Returns a Builder for a BigQuery Load Job type. **/
  public static Builder builderOfLoadType() {
    return builderOf(JobType.LOAD);
  }

  private static Builder builderOf(JobType jobType) {
    return new AutoValue_BigQuerySchemaIOConfiguration.Builder()
        .setJobType(jobType.name());
  }

  public BigQueryIO.TypedRead<TableRow> toQueryTypedRead() {
    if (getQuery().isEmpty()) {
      throw new InvalidConfigurationException("query is empty but required for a BigQuery Query Job");
    }
    BigQueryIO.TypedRead<TableRow> read = BigQueryIO.readTableRowsWithSchema().fromQuery(getQuery());
    read = applyCommonTypedReadProperties(read);
    return read;
  }

  public BigQueryIO.TypedRead<TableRow> toExtractTypedRead() {
    if (getTableSpec().isEmpty()) {
      throw new InvalidConfigurationException("tableSpec is empty but required for a BigQuery Extract Job");
    }

    BigQueryIO.TypedRead<TableRow> read = BigQueryIO.readTableRowsWithSchema().from(getTableSpec());
    read = applyCommonTypedReadProperties(read);
    return read;
  }

  private BigQueryIO.TypedRead<TableRow> applyCommonTypedReadProperties(BigQueryIO.TypedRead<TableRow> read) {
    if (getUseAvroLogicalTypes()) {
      read = read.useAvroLogicalTypes();
    }

    if (getUseStandardSql()) {
      read = read.usingStandardSql();
    }

    if (!getFormat().isEmpty()) {
      read = read.withFormat(DataFormat.valueOf(getFormat()));
    }

    if (!getKmsKey().isEmpty()) {
      read = read.withKmsKey(getKmsKey());
    }

    if (!getMethod().isEmpty()) {
      read = read.withMethod(BigQueryIO.TypedRead.Method.valueOf(getMethod()));
    }

    if (getWithoutFlattenResults()) {
      read = read.withoutResultFlattening();
    }

    if (getWithoutValidation()) {
      read = read.withoutValidation();
    }

    if (!getQueryLocation().isEmpty()) {
      read = read.withQueryLocation(getQueryLocation());
    }

    if (!getQueryPriority().isEmpty()) {
      read = read.withQueryPriority(QueryPriority.valueOf(getQueryPriority()));
    }

    if (!getQueryTempDataset().isEmpty()) {
      read = read.withQueryTempDataset(getQueryTempDataset());
    }

    if (!getRowRestriction().isEmpty()) {
      read = read.withRowRestriction(getRowRestriction());
    }

    if (!getSelectedFields().isEmpty()) {
      read = read.withSelectedFields(getSelectedFields());
    }

    if (getWithTemplateCompatibility()) {
      read = read.withTemplateCompatibility();
    }

    return read;
  }

  /** Get the configuration's Job type. **/
  public abstract String getJobType();

  /** Reads results received after executing the given query. **/
  public abstract String getQuery();

  /**
   * Reads a BigQuery table specified as "[project_id]:[dataset_id].[table_id]"
   * or "[dataset_id].[table_id]" for tables within the current project.
   *
   **/
  public abstract String getTableSpec();

  /** Flags whether to use avro logical types. **/
  public abstract Boolean getUseAvroLogicalTypes();

  /** Enables BigQuery's Standard SQL dialect when reading from a query. **/
  public abstract Boolean getUseStandardSql();

  /** DateFormat for output data. See com.google.cloud.bigquery.storage.v1.DataFormat **/
  public abstract String getFormat();

  /** For query sources, use this Cloud KMS key to encrypt any temporary tables created. **/
  public abstract String getKmsKey();

  /**
   * Determines the method used to read data from BigQuery.
   *
   * DEFAULT - The default behavior if no method is explicitly set.
   * DIRECT_READ - Read the contents of a table directly using the BigQuery storage API.
   * EXPORT - Export data to Google Cloud Storage in Avro format and read data files from that location.
   **/
  public abstract String getMethod();

  /**
   * Flatten the queried results.
   *
   * Setting this option when reading from a table will cause an error during validation.
   **/
  public abstract Boolean getWithoutFlattenResults();

  /**
   * Enable/disable validation that the table exists or the query succeeds prior to pipeline submission.
   * Basic validation (such as ensuring that a query or table is specified) still occurs.
   **/
  public abstract Boolean getWithoutValidation();

  /** BigQuery geographic location where the query job will be executed. **/
  public abstract String getQueryLocation();

  /**
   * The priority of a query.
   *
   * BATCH - Specifies that a query should be run with a BATCH priority.
   * INTERACTIVE - Specifies that a query should be run with an INTERACTIVE priority.
   *
   **/
  public abstract String getQueryPriority();

  /** Temporary dataset reference when querying. **/
  public abstract String getQueryTempDataset();

  /**
   * Read only rows which match the specified filter.
   *
   * Must be an SQL expression compatible with Google standard SQL.
   *
   **/
  public abstract String getRowRestriction();

  /** Read only the specified fields (columns) from a BigQuery table. **/
  public abstract List<String> getSelectedFields();

  public abstract Boolean getWithTemplateCompatibility();

  @AutoValue.Builder
  public static abstract class Builder {

    /** Set the configuration's Job type **/
    public abstract Builder setJobType(String value);

    /** Set the SQL query for the BigQuery Job. **/
    public abstract Builder setQuery(String value);
    abstract Optional<String> getQuery();

    /**
     * Reads a BigQuery table specified as "[project_id]:[dataset_id].[table_id]"
     * or "[dataset_id].[table_id]" for tables within the current project.
     *
     */
    public abstract Builder setTableSpec(String value);
    abstract Optional<String> getTableSpec();

    /** Set whether to use Avro logical types **/
    public abstract Builder setUseAvroLogicalTypes(Boolean value);
    abstract Optional<Boolean> getUseAvroLogicalTypes();

    /** Set whether to use standard SQL **/
    public abstract Builder setUseStandardSql(Boolean value);
    abstract Optional<Boolean> getUseStandardSql();

    /** Set format for output data. See com.google.cloud.bigquery.storage.v1.DataFormat **/
    public abstract Builder setFormat(String value);
    abstract Optional<String> getFormat();

    /** For query sources, use this Cloud KMS key to encrypt any temporary tables created. **/
    public abstract Builder setKmsKey(String value);
    abstract Optional<String> getKmsKey();

    /**
     * Determines the method used to read data from BigQuery.
     *
     * DEFAULT - The default behavior if no method is explicitly set.
     * DIRECT_READ - Read the contents of a table directly using the BigQuery storage API.
     * EXPORT - Export data to Google Cloud Storage in Avro format and read data files from that location.
     **/
    public abstract Builder setMethod(String value);
    abstract Optional<String> getMethod();

    /**
     * Flatten the queried results.
     *
     * Setting this option when reading from a table will cause an error during validation.
     **/
    public abstract Builder setWithoutFlattenResults(Boolean value);
    abstract Optional<Boolean> getWithoutFlattenResults();

    /**
     * Enable/disable validation that the table exists or the query succeeds prior to pipeline submission.
     * Basic validation (such as ensuring that a query or table is specified) still occurs.
     **/
    public abstract Builder setWithoutValidation(Boolean value);
    abstract Optional<Boolean> getWithoutValidation();

    /** BigQuery geographic location where the query job will be executed. **/
    public abstract Builder setQueryLocation(String value);
    abstract Optional<String> getQueryLocation();

    /**
     * The priority of a query.
     *
     * BATCH - Specifies that a query should be run with a BATCH priority.
     * INTERACTIVE - Specifies that a query should be run with an INTERACTIVE priority.
     *
     **/
    public abstract Builder setQueryPriority(String value);
    abstract Optional<String> getQueryPriority();

    /** Temporary dataset reference when querying. **/
    public abstract Builder setQueryTempDataset(String value);
    abstract Optional<String> getQueryTempDataset();

    /**
     * Read only rows which match the specified filter.
     *
     * Must be an SQL expression compatible with Google standard SQL.
     *
     **/
    public abstract Builder setRowRestriction(String value);
    abstract Optional<String> getRowRestriction();

    /** Read only the specified fields (columns) from a BigQuery table. **/
    public abstract Builder setSelectedFields(List<String> value);
    abstract Optional<List<String>> getSelectedFields();

    public abstract Builder setWithTemplateCompatibility(Boolean value);
    abstract Optional<Boolean> getWithTemplateCompatibility();

    abstract BigQuerySchemaIOConfiguration autoBuild();

    /**
     * Build the configuration.
     *
     * Except useStandardSql which defaults to true, all properties default to their zero value i.e.
     * boolean = false, String = "", etc.
     *
     **/
    public final BigQuerySchemaIOConfiguration build() {
      if (!getQuery().isPresent()) {
        setQuery("");
      }
      if (!getTableSpec().isPresent()) {
        setTableSpec("");
      }
      if (!getUseAvroLogicalTypes().isPresent()) {
        setUseAvroLogicalTypes(false);
      }
      if (!getUseStandardSql().isPresent()) {
        setUseStandardSql(true);
      }
      if (!getFormat().isPresent()) {
        setFormat("");
      }
      if (!getKmsKey().isPresent()) {
        setKmsKey("");
      }
      if (!getMethod().isPresent()) {
        setMethod("");
      }
      if (!getWithoutFlattenResults().isPresent()) {
        setWithoutFlattenResults(false);
      }
      if (!getQueryLocation().isPresent()) {
        setQueryLocation("");
      }
      if (!getQueryPriority().isPresent()) {
        setQueryPriority("");
      }
      if (!getQueryTempDataset().isPresent()) {
        setQueryTempDataset("");
      }
      if (!getRowRestriction().isPresent()) {
        setRowRestriction("");
      }
      if (!getSelectedFields().isPresent()) {
        setSelectedFields(Collections.emptyList());
      }
      if (!getWithTemplateCompatibility().isPresent()) {
        setWithTemplateCompatibility(false);
      }
      if (!getWithoutValidation().isPresent()) {
        setWithoutValidation(false);
      }

      return autoBuild();
    }
  }

  /** Enumeration for possible BigQuery job types supported by this configuration. **/
  public enum JobType {
    QUERY,
    EXTRACT,
    LOAD,
  }
}
