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
public class CreateTables<T, DestinationT>
    extends PTransform<
        PCollection<KV<DestinationT, T>>, PCollection<KV<String, T>>> {
  private final CreateDisposition createDisposition;
  private final BigQueryServices bqServices;
  private final DynamicDestinations<T, DestinationT> dynamicDestinations;

  /**
   * The list of tables created so far, so we don't try the creation each time.
   *
   * <p>TODO: We should put a bound on memory usage of this. Use guava cache instead.
   */
  private static Set<String> createdTables =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public CreateTables(
      CreateDisposition createDisposition,
      DynamicDestinations<T, DestinationT> dynamicDestinations) {
    this(createDisposition, new BigQueryServicesImpl(), dynamicDestinations);
  }

  private CreateTables(
      CreateDisposition createDisposition,
      BigQueryServices bqServices,
      DynamicDestinations<T, DestinationT> dynamicDestinations) {
    this.createDisposition = createDisposition;
    this.bqServices = bqServices;
    this.dynamicDestinations = dynamicDestinations;
  }

  CreateTables<T, DestinationT> withTestServices(BigQueryServices bqServices) {
    return new CreateTables<>(createDisposition, bqServices, dynamicDestinations);
  }

  @Override
  public PCollection<KV<String, T>> expand(
      PCollection<KV<DestinationT, T>> input) {
    List<PCollectionView<?>> sideInputs = Lists.newArrayList();
    sideInputs.addAll(dynamicDestinations.getSideInputs());

    return input.apply(
        ParDo.of(
                new DoFn<KV<DestinationT, T>, KV<String, T>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context)
                      throws InterruptedException, IOException {
                    dynamicDestinations.setSideInputAccessorFromProcessContext(context);
                    DestinationT destination = context.element().getKey();
                    TableDestination tableDestination = dynamicDestinations.getTable(destination);
                    TableReference tableReference = tableDestination.getTableReference();
                    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
                      tableReference.setProjectId(
                          context.getPipelineOptions().as(BigQueryOptions.class).getProject());
                      tableDestination =
                          new TableDestination(
                              tableReference, tableDestination.getTableDescription());
                    }
                    BigQueryOptions options =
                        context.getPipelineOptions().as(BigQueryOptions.class);
                    possibleCreateTable(destination, options, tableDestination);
                    context.output(
                        KV.of(tableDestination.getTableSpec(), context.element().getValue()));
                  }
                })
            .withSideInputs(sideInputs));
  }

  private void possibleCreateTable(
      DestinationT destination, BigQueryOptions options, TableDestination tableDestination)
      throws InterruptedException, IOException {
    String tableSpec = BigQueryHelpers.stripPartitionDecorator(tableDestination.getTableSpec());
    if (createDisposition != createDisposition.CREATE_NEVER && !createdTables.contains(tableSpec)) {
      synchronized (createdTables) {
        // Another thread may have succeeded in creating the table in the meanwhile, so
        // check again. This check isn't needed for correctness, but we add it to prevent
        // every thread from attempting a create and overwhelming our BigQuery quota.
        DatasetService datasetService = bqServices.getDatasetService(options);
        if (!createdTables.contains(tableSpec)) {
          TableSchema tableSchema = dynamicDestinations.getSchema(destination);
          TableReference tableReference = tableDestination.getTableReference();
          String tableDescription = tableDestination.getTableDescription();
          tableReference.setTableId(
              BigQueryHelpers.stripPartitionDecorator(tableReference.getTableId()));
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
