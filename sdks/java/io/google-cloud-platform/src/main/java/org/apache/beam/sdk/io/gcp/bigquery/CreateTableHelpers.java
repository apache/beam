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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;

public class CreateTableHelpers {
  /**
   * The list of tables created so far, so we don't try the creation each time.
   *
   * <p>TODO: We should put a bound on memory usage of this. Use guava cache instead.
   */
  private static Set<String> createdTables =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  static TableDestination possiblyCreateTable(
      DoFn<?, ?>.ProcessContext context,
      TableDestination tableDestination,
      Supplier<TableSchema> schemaSupplier,
      CreateDisposition createDisposition,
      Coder<?> tableDestinationCoder,
      String kmsKey,
      BigQueryServices bqServices) {
    checkArgument(
        tableDestination.getTableSpec() != null,
        "DynamicDestinations.getTable() must return a TableDestination "
            + "with a non-null table spec, but %s returned %s for destination %s,"
            + "which has a null table spec",
        tableDestination);
    boolean destinationCoderSupportsClustering =
        !(tableDestinationCoder instanceof TableDestinationCoderV2);
    checkArgument(
        tableDestination.getClustering() == null || destinationCoderSupportsClustering,
        "DynamicDestinations.getTable() may only return destinations with clustering configured"
            + " if a destination coder is supplied that supports clustering, but %s is configured"
            + " to use TableDestinationCoderV2. Set withClustering() on BigQueryIO.write() and, "
            + " if you provided a custom DynamicDestinations instance, override"
            + " getDestinationCoder() to return TableDestinationCoderV3.");
    TableReference tableReference = tableDestination.getTableReference().clone();
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      tableReference.setProjectId(
          context.getPipelineOptions().as(BigQueryOptions.class).getProject());
      tableDestination = tableDestination.withTableReference(tableReference);
    }
    if (createDisposition == CreateDisposition.CREATE_NEVER) {
      return tableDestination;
    }

    String tableSpec = BigQueryHelpers.stripPartitionDecorator(tableDestination.getTableSpec());
    if (!createdTables.contains(tableSpec)) {
      // Another thread may have succeeded in creating the table in the meanwhile, so
      // check again. This check isn't needed for correctness, but we add it to prevent
      // every thread from attempting a create and overwhelming our BigQuery quota.
      synchronized (createdTables) {
        if (!createdTables.contains(tableSpec)) {
          tryCreateTable(
              context,
              schemaSupplier,
              tableDestination,
              createDisposition,
              tableSpec,
              kmsKey,
              bqServices);
        }
      }
    }
    return tableDestination;
  }

  @SuppressWarnings({"nullness"})
  private static void tryCreateTable(
      DoFn<?, ?>.ProcessContext context,
      Supplier<TableSchema> schemaSupplier,
      TableDestination tableDestination,
      CreateDisposition createDisposition,
      String tableSpec,
      String kmsKey,
      BigQueryServices bqServices) {
    TableReference tableReference = tableDestination.getTableReference().clone();
    tableReference.setTableId(BigQueryHelpers.stripPartitionDecorator(tableReference.getTableId()));
    try (DatasetService datasetService =
        bqServices.getDatasetService(context.getPipelineOptions().as(BigQueryOptions.class))) {
      if (datasetService.getTable(tableReference) == null) {
        TableSchema tableSchema = schemaSupplier.get();
        checkArgument(
            tableSchema != null,
            "Unless create disposition is %s, a schema must be specified, i.e. "
                + "DynamicDestinations.getSchema() may not return null. "
                + "However, create disposition is %s, and "
                + " %s returned null for destination %s",
            CreateDisposition.CREATE_NEVER,
            createDisposition,
            tableDestination);
        Table table = new Table().setTableReference(tableReference).setSchema(tableSchema);
        if (tableDestination.getTableDescription() != null) {
          table = table.setDescription(tableDestination.getTableDescription());
        }
        if (tableDestination.getTimePartitioning() != null) {
          table.setTimePartitioning(tableDestination.getTimePartitioning());
          if (tableDestination.getClustering() != null) {
            table.setClustering(tableDestination.getClustering());
          }
        }
        if (kmsKey != null) {
          table.setEncryptionConfiguration(new EncryptionConfiguration().setKmsKeyName(kmsKey));
        }
        datasetService.createTable(table);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    createdTables.add(tableSpec);
  }

  @VisibleForTesting
  static void clearCreatedTables() {
    synchronized (createdTables) {
      createdTables.clear();
    }
  }
}
