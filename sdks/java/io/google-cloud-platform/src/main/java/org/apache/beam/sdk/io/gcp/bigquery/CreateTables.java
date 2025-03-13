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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

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
  private final @Nullable String kmsKey;

  /**
   * The list of tables created so far, so we don't try the creation each time.
   *
   * <p>TODO: We should put a bound on memory usage of this. Use guava cache instead.
   */
  public CreateTables(
      CreateDisposition createDisposition,
      DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this(createDisposition, new BigQueryServicesImpl(), dynamicDestinations, null);
  }

  private CreateTables(
      CreateDisposition createDisposition,
      BigQueryServices bqServices,
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      @Nullable String kmsKey) {
    this.createDisposition = createDisposition;
    this.bqServices = bqServices;
    this.dynamicDestinations = dynamicDestinations;
    this.kmsKey = kmsKey;
  }

  CreateTables<DestinationT, ElementT> withKmsKey(@Nullable String kmsKey) {
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
    private @Nullable Map<DestinationT, TableDestination> destinations = null;

    @StartBundle
    public void startBundle() {
      destinations = Maps.newHashMap();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      dynamicDestinations.setSideInputAccessorFromProcessContext(context);
      Preconditions.checkStateNotNull(destinations);
      TableDestination tableDestination =
          destinations.computeIfAbsent(
              context.element().getKey(),
              dest -> {
                TableDestination tableDestination1 = dynamicDestinations.getTable(dest);
                checkArgument(
                    tableDestination1 != null,
                    "DynamicDestinations.getTable() may not return null, "
                        + "but %s returned null for destination %s",
                    dynamicDestinations,
                    dest);
                Supplier<@Nullable TableSchema> schemaSupplier =
                    () -> dynamicDestinations.getSchema(dest);
                Supplier<@Nullable TableConstraints> tableConstraintsSupplier =
                    () -> dynamicDestinations.getTableConstraints(dest);

                BigQueryOptions bqOptions = context.getPipelineOptions().as(BigQueryOptions.class);
                Lineage.getSinks()
                    .add(
                        "bigquery",
                        BigQueryHelpers.dataCatalogSegments(
                            tableDestination1.getTableReference(), bqOptions));
                return CreateTableHelpers.possiblyCreateTable(
                    bqOptions,
                    tableDestination1,
                    schemaSupplier,
                    tableConstraintsSupplier,
                    createDisposition,
                    dynamicDestinations.getDestinationCoder(),
                    kmsKey,
                    bqServices,
                    null);
              });

      context.output(KV.of(tableDestination, context.element().getValue()));
    }
  }

  /** This method is used by the testing fake to clear static state. */
  @VisibleForTesting
  static void clearCreatedTables() {
    CreateTableHelpers.clearCreatedTables();
  }
}
