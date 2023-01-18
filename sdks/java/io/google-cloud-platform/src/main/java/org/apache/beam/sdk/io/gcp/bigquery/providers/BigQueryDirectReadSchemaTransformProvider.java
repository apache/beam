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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider.BigQueryDirectReadSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery Storage Read API jobs
 * configured via {@link BigQueryDirectReadSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Experimental(Kind.SCHEMAS)
@AutoService(SchemaTransformProvider.class)
public class BigQueryDirectReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigQueryDirectReadSchemaTransformConfiguration> {

  private static final String OUTPUT_TAG = "OUTPUT_ROWS";

  @Override
  protected Class<BigQueryDirectReadSchemaTransformConfiguration> configurationClass() {
    return BigQueryDirectReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(BigQueryDirectReadSchemaTransformConfiguration configuration) {
    return new BigQueryDirectReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return String.format("beam:transform:org.apache.beam:bigquery_storage_read:v1");
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /** Configuration for reading from BigQuery with Storage Read API. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class BigQueryDirectReadSchemaTransformConfiguration {

    public void validate() {
      String invalidConfigMessage = "Invalid BigQuery Direct Read configuration: ";
      if (!Strings.isNullOrEmpty(this.getTableSpec())) {
        checkNotNull(BigQueryHelpers.parseTableSpec(this.getTableSpec()));
        checkArgument(
            Strings.isNullOrEmpty(this.getQuery()),
            invalidConfigMessage + "Cannot specify both query and table spec.");
      } else {
        checkArgument(
            !Strings.isNullOrEmpty(this.getQuery()),
            invalidConfigMessage + "Either a query or table spec needs to be specified.");
        checkArgument(
            Strings.isNullOrEmpty(this.getRowRestriction()),
            invalidConfigMessage + "Row restriction can only be specified when using table spec.");
        checkArgument(
            this.getSelectedFields() == null,
            invalidConfigMessage + "Selected fields can only be specified when using table spec.");
      }
    }

    /** Instantiates a {@link BigQueryDirectReadSchemaTransformConfiguration.Builder} instance. */
    public static Builder builder() {
      return new AutoValue_BigQueryDirectReadSchemaTransformProvider_BigQueryDirectReadSchemaTransformConfiguration
          .Builder();
    }

    @Nullable
    public abstract String getQuery();

    @Nullable
    public abstract String getTableSpec();

    @Nullable
    public abstract String getRowRestriction();

    @Nullable
    public abstract List<String> getSelectedFields();

    @Nullable
    /** Builder for the {@link BigQueryDirectReadSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setQuery(String query);

      public abstract Builder setTableSpec(String tableSpec);

      public abstract Builder setRowRestriction(String rowRestriction);

      public abstract Builder setSelectedFields(List<String> selectedFields);

      /** Builds a {@link BigQueryDirectReadSchemaTransformConfiguration} instance. */
      public abstract BigQueryDirectReadSchemaTransformConfiguration build();
    }
  }

  /**
   * A {@link SchemaTransform} for BigQuery Storage Read API, configured with {@link
   * BigQueryDirectReadSchemaTransformConfiguration} and instantiated by {@link
   * BigQueryDirectReadSchemaTransformProvider}.
   */
  private static class BigQueryDirectReadSchemaTransform implements SchemaTransform {
    private final BigQueryDirectReadSchemaTransformConfiguration configuration;

    BigQueryDirectReadSchemaTransform(
        BigQueryDirectReadSchemaTransformConfiguration configuration) {
      // Validate configuration parameters before PTransform expansion
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new BigQueryDirectReadPCollectionRowTupleTransform(configuration);
    }
  }

  static class BigQueryDirectReadPCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {
    private final BigQueryDirectReadSchemaTransformConfiguration configuration;
    private BigQueryServices testBigQueryServices = null;

    BigQueryDirectReadPCollectionRowTupleTransform(
        BigQueryDirectReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @VisibleForTesting
    public void setBigQueryServices(BigQueryServices testBigQueryServices) {
      this.testBigQueryServices = testBigQueryServices;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.getAll().isEmpty(),
          String.format(
              "Input to %s is expected to be empty, but is not.", getClass().getSimpleName()));

      BigQueryIO.TypedRead<TableRow> read = createDirectReadTransform();

      PCollection<TableRow> tableRows = input.getPipeline().apply(read);
      Schema schema = tableRows.getSchema();
      PCollection<Row> rows =
          tableRows.apply(
              MapElements.into(TypeDescriptor.of(Row.class))
                  .via((tableRow) -> BigQueryUtils.toBeamRow(schema, tableRow)));

      return PCollectionRowTuple.of(OUTPUT_TAG, rows.setRowSchema(schema));
    }

    BigQueryIO.TypedRead<TableRow> createDirectReadTransform() {
      BigQueryIO.TypedRead<TableRow> read =
          BigQueryIO.readTableRowsWithSchema().withMethod(TypedRead.Method.DIRECT_READ);

      if (!Strings.isNullOrEmpty(configuration.getTableSpec())) {
        read = read.from(configuration.getTableSpec());
        if (!Strings.isNullOrEmpty(configuration.getRowRestriction())) {
          read = read.withRowRestriction(configuration.getRowRestriction());
        }
        if (configuration.getSelectedFields() != null) {
          read = read.withSelectedFields(configuration.getSelectedFields());
        }
      } else {
        read = read.fromQuery(configuration.getQuery());
      }

      if (this.testBigQueryServices != null) {
        read = read.withTestServices(testBigQueryServices);
      }

      return read;
    }
  }
}
