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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * Configuration for writing to BigQuery with SchemaTransforms. Used by {@link
 * BigQueryStorageWriteApiSchemaTransformProvider} and {@link
 * BigQueryFileLoadsWriteSchemaTransformProvider}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQueryWriteConfiguration {
  protected static final String DYNAMIC_DESTINATIONS = "DYNAMIC_DESTINATIONS";

  @AutoValue
  public abstract static class ErrorHandling {
    @SchemaFieldDescription("The name of the output PCollection containing failed writes.")
    public abstract String getOutput();

    public static Builder builder() {
      return new AutoValue_BigQueryWriteConfiguration_ErrorHandling.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setOutput(String output);

      public abstract ErrorHandling build();
    }
  }

  public void validate() {
    String invalidConfigMessage = "Invalid BigQuery Storage Write configuration: ";

    // validate output table spec
    checkArgument(
        !Strings.isNullOrEmpty(this.getTable()),
        invalidConfigMessage + "Table spec for a BigQuery Write must be specified.");

    // if we have an input table spec, validate it
    if (!this.getTable().equals(DYNAMIC_DESTINATIONS)) {
      checkNotNull(BigQueryHelpers.parseTableSpec(this.getTable()));
    }

    // validate create and write dispositions
    String createDisposition = getCreateDisposition();
    if (createDisposition != null && !createDisposition.isEmpty()) {
      List<String> createDispositions =
          Arrays.stream(BigQueryIO.Write.CreateDisposition.values())
              .map(c -> c.name())
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          createDispositions.contains(createDisposition.toUpperCase()),
          "Invalid create disposition (%s) was specified. Available dispositions are: %s",
          createDisposition,
          createDispositions);
    }
    String writeDisposition = getWriteDisposition();
    if (writeDisposition != null && !writeDisposition.isEmpty()) {
      List<String> writeDispostions =
          Arrays.stream(BigQueryIO.Write.WriteDisposition.values())
              .map(w -> w.name())
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          writeDispostions.contains(writeDisposition.toUpperCase()),
          "Invalid write disposition (%s) was specified. Available dispositions are: %s",
          writeDisposition,
          writeDispostions);
    }

    ErrorHandling errorHandling = getErrorHandling();
    if (errorHandling != null) {
      checkArgument(
          !Strings.isNullOrEmpty(errorHandling.getOutput()),
          invalidConfigMessage + "Output must not be empty if error handling specified.");
    }

    Boolean autoSharding = getAutoSharding();
    Integer numStreams = getNumStreams();
    if (autoSharding != null && autoSharding && numStreams != null) {
      checkArgument(
          numStreams == 0,
          invalidConfigMessage
              + "Cannot set a fixed number of streams when auto-sharding is enabled. Please pick only one of the two options.");
    }
  }

  /** Instantiates a {@link BigQueryWriteConfiguration.Builder} instance. */
  public static Builder builder() {
    return new AutoValue_BigQueryWriteConfiguration.Builder();
  }

  @SchemaFieldDescription(
      "The bigquery table to write to. Format: [${PROJECT}:]${DATASET}.${TABLE}")
  public abstract String getTable();

  @SchemaFieldDescription(
      "Optional field that specifies whether the job is allowed to create new tables. "
          + "The following values are supported: CREATE_IF_NEEDED (the job may create the table), CREATE_NEVER ("
          + "the job must fail if the table does not exist already).")
  @Nullable
  public abstract String getCreateDisposition();

  @SchemaFieldDescription(
      "Specifies the action that occurs if the destination table already exists. "
          + "The following values are supported: "
          + "WRITE_TRUNCATE (overwrites the table data), "
          + "WRITE_APPEND (append the data to the table), "
          + "WRITE_EMPTY (job must fail if the table is not empty).")
  @Nullable
  public abstract String getWriteDisposition();

  @SchemaFieldDescription(
      "Determines how often to 'commit' progress into BigQuery. Default is every 5 seconds.")
  @Nullable
  public abstract Long getTriggeringFrequencySeconds();

  @SchemaFieldDescription(
      "This option enables lower latency for insertions to BigQuery but may ocassionally "
          + "duplicate data elements.")
  @Nullable
  public abstract Boolean getUseAtLeastOnceSemantics();

  @SchemaFieldDescription(
      "This option enables using a dynamically determined number of Storage Write API streams to write to "
          + "BigQuery. Only applicable to unbounded data.")
  @Nullable
  public abstract Boolean getAutoSharding();

  @SchemaFieldDescription(
      "Specifies the number of write streams that the Storage API sink will use. "
          + "This parameter is only applicable when writing unbounded data.")
  @Nullable
  public abstract Integer getNumStreams();

  @SchemaFieldDescription("Use this Cloud KMS key to encrypt your data")
  @Nullable
  public abstract String getKmsKey();

  @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
  @Nullable
  public abstract ErrorHandling getErrorHandling();

  @SchemaFieldDescription(
      "This option enables the use of BigQuery CDC functionality. The expected PCollection"
          + " should contain Beam Rows with a schema wrapping the record to be inserted and"
          + " adding the CDC info similar to: {row_mutation_info: {mutation_type:\"...\", "
          + "change_sequence_number:\"...\"}, record: {...}}")
  @Nullable
  public abstract Boolean getUseCdcWrites();

  @SchemaFieldDescription(
      "If CREATE_IF_NEEDED disposition is set, BigQuery table(s) will be created with this"
          + " columns as primary key. Required when CDC writes are enabled with CREATE_IF_NEEDED.")
  @Nullable
  public abstract List<String> getPrimaryKey();

  /** Builder for {@link BigQueryWriteConfiguration}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTable(String table);

    public abstract Builder setCreateDisposition(String createDisposition);

    public abstract Builder setWriteDisposition(String writeDisposition);

    public abstract Builder setTriggeringFrequencySeconds(Long seconds);

    public abstract Builder setUseAtLeastOnceSemantics(Boolean use);

    public abstract Builder setAutoSharding(Boolean autoSharding);

    public abstract Builder setNumStreams(Integer numStreams);

    public abstract Builder setKmsKey(String kmsKey);

    public abstract Builder setErrorHandling(ErrorHandling errorHandling);

    public abstract Builder setUseCdcWrites(Boolean cdcWrites);

    public abstract Builder setPrimaryKey(List<String> pkColumns);

    /** Builds a {@link BigQueryWriteConfiguration} instance. */
    public abstract BigQueryWriteConfiguration build();
  }
}
