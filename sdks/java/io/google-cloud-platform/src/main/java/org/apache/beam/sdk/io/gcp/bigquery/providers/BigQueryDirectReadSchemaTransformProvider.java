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

import com.google.api.client.util.Strings;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
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
    return new DirectReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return String.format("bigquery:direct-read");
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  private static class DirectReadSchemaTransform implements SchemaTransform {
    private final BigQueryDirectReadSchemaTransformConfiguration configuration;

    DirectReadSchemaTransform(BigQueryDirectReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new DirectReadPCollectionRowTupleTransform(configuration);
    }
  }

  static class DirectReadPCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {
    private final BigQueryDirectReadSchemaTransformConfiguration configuration;

    private BigQueryServices testBigQueryServices = null;

    @VisibleForTesting
    void setTestBigQueryServices(BigQueryServices testBigQueryServices) {
      this.testBigQueryServices = testBigQueryServices;
    }

    DirectReadPCollectionRowTupleTransform(
        BigQueryDirectReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
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

      if (!Strings.isNullOrEmpty(configuration.getQuery())) {
        read = read.fromQuery(configuration.getQuery());
      }
      if (!Strings.isNullOrEmpty(configuration.getTableSpec())) {
        read = read.from(configuration.getTableSpec());
      }
      if (!Strings.isNullOrEmpty(configuration.getRowRestriction())) {
        read = read.withRowRestriction(configuration.getRowRestriction());
      }
      if (configuration.getSelectedFields() != null) {
        read = read.withSelectedFields(configuration.getSelectedFields());
      }
      if (testBigQueryServices != null) {
        read = read.withTestServices(testBigQueryServices).withoutValidation();
      }

      return read;
    }
  }
}
