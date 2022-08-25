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

import com.google.api.services.bigquery.model.TableRow;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
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
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery read jobs configured using
 * {@link BigQuerySchemaTransformReadConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
@Experimental(Kind.SCHEMAS)
public class BigQuerySchemaTransformReadProvider
    extends TypedSchemaTransformProvider<BigQuerySchemaTransformReadConfiguration> {

  private static final String API = "bigquery";
  private static final String OUTPUT_TAG = "OUTPUT";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<BigQuerySchemaTransformReadConfiguration> configurationClass() {
    return BigQuerySchemaTransformReadConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(BigQuerySchemaTransformReadConfiguration configuration) {
    return new BigQueryReadSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return String.format("%s:read", API);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. Since
   * no input is expected, this returns an empty list.
   */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. Since
   * a single output is expected, this returns a list with a single name.
   */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /**
   * An implementation of {@link SchemaTransform} for BigQuery read jobs configured using {@link
   * BigQuerySchemaTransformReadConfiguration}.
   */
  private static class BigQueryReadSchemaTransform implements SchemaTransform {
    private final BigQuerySchemaTransformReadConfiguration configuration;

    BigQueryReadSchemaTransform(BigQuerySchemaTransformReadConfiguration configuration) {
      this.configuration = configuration;
    }

    /** Implements {@link SchemaTransform} buildTransform method. */
    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PCollectionRowTupleTransform(configuration);
    }
  }

  /**
   * An implementation of {@link PTransform} for BigQuery read jobs configured using {@link
   * BigQuerySchemaTransformReadConfiguration}.
   */
  static class PCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {

    private final BigQuerySchemaTransformReadConfiguration configuration;

    /** An instance of {@link BigQueryServices} used for testing. */
    private BigQueryServices testBigQueryServices = null;

    PCollectionRowTupleTransform(BigQuerySchemaTransformReadConfiguration configuration) {
      this.configuration = configuration;
    }

    @VisibleForTesting
    void setTestBigQueryServices(BigQueryServices testBigQueryServices) {
      this.testBigQueryServices = testBigQueryServices;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (!input.getAll().isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "%s %s input is expected to be empty",
                input.getClass().getSimpleName(), getClass().getSimpleName()));
      }

      BigQueryIO.TypedRead<TableRow> read = toTypedRead();
      if (testBigQueryServices != null) {
        read = read.withTestServices(testBigQueryServices).withoutValidation();
      }

      PCollection<TableRow> tableRowPCollection = input.getPipeline().apply(read);
      Schema schema = tableRowPCollection.getSchema();
      PCollection<Row> rowPCollection =
          tableRowPCollection.apply(
              MapElements.into(TypeDescriptor.of(Row.class))
                  .via((tableRow) -> BigQueryUtils.toBeamRow(schema, tableRow)));
      return PCollectionRowTuple.of(OUTPUT_TAG, rowPCollection.setRowSchema(schema));
    }

    BigQueryIO.TypedRead<TableRow> toTypedRead() {
      BigQueryIO.TypedRead<TableRow> read = BigQueryIO.readTableRowsWithSchema();

      if (!Strings.isNullOrEmpty(configuration.getQuery())) {
        read = read.fromQuery(configuration.getQuery());
      }

      if (!Strings.isNullOrEmpty(configuration.getTableSpec())) {
        read = read.from(configuration.getTableSpec());
      }

      if (configuration.getUseStandardSql() != null && configuration.getUseStandardSql()) {
        read = read.usingStandardSql();
      }

      if (!Strings.isNullOrEmpty(configuration.getQueryLocation())) {
        read = read.withQueryLocation(configuration.getQueryLocation());
      }

      return read;
    }
  }
}
