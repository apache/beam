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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryTableSourceDef implements BigQuerySourceDef {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableSourceDef.class);

  private final BigQueryServices bqServices;
  private final ValueProvider<String> jsonTable;

  static BigQueryTableSourceDef create(
      BigQueryServices bqServices, ValueProvider<TableReference> table) {
    ValueProvider<String> jsonTable =
        ValueProvider.NestedValueProvider.of(
            checkNotNull(table, "table"), new BigQueryHelpers.TableRefToJson());
    return new BigQueryTableSourceDef(bqServices, jsonTable);
  }

  private BigQueryTableSourceDef(BigQueryServices bqServices, ValueProvider<String> jsonTable) {
    this.bqServices = bqServices;
    this.jsonTable = jsonTable;
  }

  TableReference getTableReference(BigQueryOptions bqOptions) throws IOException {
    TableReference tableReference =
        BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(), TableReference.class);
    return setDefaultProjectIfAbsent(bqOptions, tableReference);
  }

  /**
   * Sets the {@link TableReference#projectId} of the provided table reference to the id of the
   * default project if the table reference does not have a project ID specified.
   */
  private TableReference setDefaultProjectIfAbsent(
      BigQueryOptions bqOptions, TableReference tableReference) {
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      checkState(
          !Strings.isNullOrEmpty(bqOptions.getProject()),
          "No project ID set in %s or %s, cannot construct a complete %s",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName(),
          TableReference.class.getSimpleName());
      LOG.info(
          "Project ID not set in {}. Using default project from {}.",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName());
      tableReference.setProjectId(bqOptions.getProject());
    }
    return tableReference;
  }

  ValueProvider<String> getJsonTable() {
    return jsonTable;
  }

  /** {@inheritDoc} */
  @Override
  public <T> BigQuerySourceBase<T> toSource(
      String stepUuid,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      boolean useAvroLogicalTypes) {
    return BigQueryTableSource.create(
        stepUuid, this, bqServices, coder, parseFn, useAvroLogicalTypes);
  }

  /** {@inheritDoc} */
  @Experimental(Kind.SCHEMAS)
  @Override
  public Schema getBeamSchema(BigQueryOptions bqOptions) {
    try {
      TableReference tableRef = getTableReference(bqOptions);
      TableSchema tableSchema =
          bqServices.getDatasetService(bqOptions).getTable(tableRef).getSchema();
      return BigQueryUtils.fromTableSchema(tableSchema);
    } catch (IOException | InterruptedException | NullPointerException e) {
      throw new BigQuerySchemaRetrievalException("Exception while trying to retrieve schema", e);
    }
  }
}
