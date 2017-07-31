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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Creates any tables needed before performing streaming writes to the tables. This is a side-effect
 * {@link DoFn}, and returns the original collection unchanged.
 */
public class CreateTables<DestinationT>
    extends PTransform<
        PCollection<KV<DestinationT, TableRow>>, PCollection<KV<TableDestination, TableRow>>> {
  private final CreateDisposition createDisposition;
  private final BigQueryServices bqServices;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;

  /**
   * The list of tables created so far, so we don't try the creation each time.
   *
   * <p>TODO: We should put a bound on memory usage of this. Use guava cache instead.
   */
  private static Set<String> createdTables =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public CreateTables(
      CreateDisposition createDisposition,
      DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this(createDisposition, new BigQueryServicesImpl(), dynamicDestinations);
  }

  private CreateTables(
      CreateDisposition createDisposition,
      BigQueryServices bqServices,
      DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this.createDisposition = createDisposition;
    this.bqServices = bqServices;
    this.dynamicDestinations = dynamicDestinations;
  }

  CreateTables<DestinationT> withTestServices(BigQueryServices bqServices) {
    return new CreateTables<DestinationT>(createDisposition, bqServices, dynamicDestinations);
  }

  @Override
  public PCollection<KV<TableDestination, TableRow>> expand(
      PCollection<KV<DestinationT, TableRow>> input) {
    List<PCollectionView<?>> sideInputs = Lists.newArrayList();
    sideInputs.addAll(dynamicDestinations.getSideInputs());

    return input.apply(
        ParDo.of(
                new DoFn<KV<DestinationT, TableRow>, KV<TableDestination, TableRow>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context)
                      throws InterruptedException, IOException {
                    dynamicDestinations.setSideInputAccessorFromProcessContext(context);
                    TableDestination tableDestination =
                        dynamicDestinations.getTable(context.element().getKey());
                    TableReference tableReference = tableDestination.getTableReference();
                    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
                      tableReference.setProjectId(
                          context.getPipelineOptions().as(BigQueryOptions.class).getProject());
                      tableDestination =
                          new TableDestination(
                              tableReference, tableDestination.getTableDescription());
                    }
                    TableSchema tableSchema =
                        dynamicDestinations.getSchema(context.element().getKey());
                    BigQueryOptions options =
                        context.getPipelineOptions().as(BigQueryOptions.class);
                    possibleCreateTable(options, tableDestination, tableSchema);
                    context.output(KV.of(tableDestination, context.element().getValue()));
                  }
                })
            .withSideInputs(sideInputs));
  }

  private void possibleCreateTable(
      BigQueryOptions options, TableDestination tableDestination, TableSchema tableSchema)
      throws InterruptedException, IOException {
    String tableSpec = tableDestination.getTableSpec();
    TableReference tableReference = tableDestination.getTableReference();
    String tableDescription = tableDestination.getTableDescription();
    if (createDisposition != createDisposition.CREATE_NEVER && !createdTables.contains(tableSpec)) {
      synchronized (createdTables) {
        // Another thread may have succeeded in creating the table in the meanwhile, so
        // check again. This check isn't needed for correctness, but we add it to prevent
        // every thread from attempting a create and overwhelming our BigQuery quota.
        DatasetService datasetService = bqServices.getDatasetService(options);
        if (!createdTables.contains(tableSpec)) {
          if (datasetService.getTable(tableReference) == null) {
            Table table = new Table()
                .setTableReference(tableReference)
                .setSchema(tableSchema)
                .setDescription(tableDescription);
            if (tableDestination.getTimePartitioning() != null) {
              table.setTimePartitioning(tableDestination.getTimePartitioning());
            }
            datasetService.createTable(table);
          }
          createdTables.add(tableSpec);
        }
      }
    }
  }

  /** This method is used by the testing fake to clear static state. */
  @VisibleForTesting
  static void clearCreatedTables() {
    synchronized (createdTables) {
      createdTables.clear();
    }
  }
}
