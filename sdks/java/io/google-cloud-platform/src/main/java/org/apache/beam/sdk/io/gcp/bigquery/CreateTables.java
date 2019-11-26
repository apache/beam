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
import java.util.List;
import java.util.Map;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * Creates any tables needed before performing streaming writes to the tables. This is a side-effect
 * {@link DoFn}, and returns the original collection unchanged.
 */
public class CreateTables<DestinationT, ElementT>
    extends PTransform<
        PCollection<KV<DestinationT, ElementT>>, PCollection<KV<TableDestination, ElementT>>> {
  private final CreateDisposition createDisposition;
  private final BigQueryServices bqServices;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;
  private final String kmsKey;

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
    this(createDisposition, new BigQueryServicesImpl(), dynamicDestinations, null);
  }

  private CreateTables(
      CreateDisposition createDisposition,
      BigQueryServices bqServices,
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      String kmsKey) {
    this.createDisposition = createDisposition;
    this.bqServices = bqServices;
    this.dynamicDestinations = dynamicDestinations;
    this.kmsKey = kmsKey;
  }

  CreateTables<DestinationT, ElementT> withKmsKey(String kmsKey) {
    return new CreateTables<>(createDisposition, bqServices, dynamicDestinations, kmsKey);
  }

  CreateTables<DestinationT, ElementT> withTestServices(BigQueryServices bqServices) {
    return new CreateTables<>(createDisposition, bqServices, dynamicDestinations, kmsKey);
  }

  @Override
  public PCollection<KV<TableDestination, ElementT>> expand(
      PCollection<KV<DestinationT, ElementT>> input) {
    List<PCollectionView<?>> sideInputs = Lists.newArrayList();
    sideInputs.addAll(dynamicDestinations.getSideInputs());

    return input.apply(ParDo.of(new CreateTablesFn()).withSideInputs(sideInputs));
  }

  private class CreateTablesFn
      extends DoFn<KV<DestinationT, ElementT>, KV<TableDestination, ElementT>> {
    private Map<DestinationT, TableDestination> destinations;

    @StartBundle
    public void startBundle() {
      destinations = Maps.newHashMap();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      dynamicDestinations.setSideInputAccessorFromProcessContext(context);
      context.output(
          KV.of(
              destinations.computeIfAbsent(
                  context.element().getKey(), dest -> getTableDestination(context, dest)),
              context.element().getValue()));
    }

    private TableDestination getTableDestination(ProcessContext context, DestinationT destination) {
      TableDestination tableDestination = dynamicDestinations.getTable(destination);
      checkArgument(
          tableDestination != null,
          "DynamicDestinations.getTable() may not return null, "
              + "but %s returned null for destination %s",
          dynamicDestinations,
          destination);
      checkArgument(
          tableDestination.getTableSpec() != null,
          "DynamicDestinations.getTable() must return a TableDestination "
              + "with a non-null table spec, but %s returned %s for destination %s,"
              + "which has a null table spec",
          dynamicDestinations,
          tableDestination,
          destination);
      boolean destinationCoderSupportsClustering =
          !(dynamicDestinations.getDestinationCoder() instanceof TableDestinationCoderV2);
      checkArgument(
          tableDestination.getClustering() == null || destinationCoderSupportsClustering,
          "DynamicDestinations.getTable() may only return destinations with clustering configured"
              + " if a destination coder is supplied that supports clustering, but %s is configured"
              + " to use TableDestinationCoderV2. Set withClustering() on BigQueryIO.write() and, "
              + " if you provided a custom DynamicDestinations instance, override"
              + " getDestinationCoder() to return TableDestinationCoderV3.",
          dynamicDestinations);
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
            tryCreateTable(context, destination, tableDestination, tableSpec, kmsKey);
          }
        }
      }
      return tableDestination;
    }

    private void tryCreateTable(
        ProcessContext context,
        DestinationT destination,
        TableDestination tableDestination,
        String tableSpec,
        String kmsKey) {
      DatasetService datasetService =
          bqServices.getDatasetService(context.getPipelineOptions().as(BigQueryOptions.class));
      TableReference tableReference = tableDestination.getTableReference().clone();
      tableReference.setTableId(
          BigQueryHelpers.stripPartitionDecorator(tableReference.getTableId()));
      try {
        if (datasetService.getTable(tableReference) == null) {
          TableSchema tableSchema = dynamicDestinations.getSchema(destination);
          checkArgument(
              tableSchema != null,
              "Unless create disposition is %s, a schema must be specified, i.e. "
                  + "DynamicDestinations.getSchema() may not return null. "
                  + "However, create disposition is %s, and "
                  + " %s returned null for destination %s",
              CreateDisposition.CREATE_NEVER,
              createDisposition,
              dynamicDestinations,
              destination);
          Table table =
              new Table()
                  .setTableReference(tableReference)
                  .setSchema(tableSchema)
                  .setDescription(tableDestination.getTableDescription());
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
  }

  /** This method is used by the testing fake to clear static state. */
  @VisibleForTesting
  static void clearCreatedTables() {
    synchronized (createdTables) {
      createdTables.clear();
    }
  }
}
