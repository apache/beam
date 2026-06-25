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
package org.apache.beam.sdk.io.iceberg.maintenance;

import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergMaintenance {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenance.class);
  public static final String MAINTENANCE_PREFIX = "[Maintenance]";
  private static final String IMPULSE = "Impulse";
  private final Pipeline pipeline;
  private PCollection<SnapshotInfo> maintenance;
  private final SerializableTable table;
  private final String tableIdentifier;
  private final IcebergCatalogConfig catalogConfig;
  private final String operationId;

  private IcebergMaintenance(
      String tableIdentifier,
      Map<String, String> catalogConfig,
      @Nullable PipelineOptions pipelineOptions,
      @Nullable Pipeline pipeline) {
    this.tableIdentifier = tableIdentifier;
    this.catalogConfig = IcebergCatalogConfig.builder().setCatalogProperties(catalogConfig).build();

    if (pipeline == null) {
      PipelineOptions options =
          pipelineOptions != null ? pipelineOptions : PipelineOptionsFactory.create();
      LOG.info(
          MAINTENANCE_PREFIX + " Building a new {} pipeline to run maintenance for table '{}'.",
          options.getRunner().getSimpleName(),
          tableIdentifier);
      pipeline = Pipeline.create(options);
    }
    this.pipeline = pipeline;

    this.table =
        (SerializableTable)
            SerializableTable.copyOf(
                this.catalogConfig.catalog().loadTable(TableIdentifier.parse(tableIdentifier)));
    Snapshot snapshot =
        Preconditions.checkStateNotNull(
            table.currentSnapshot(),
            "Iceberg maintenance requires a valid snapshot, but table '%s' has none. "
                + "Ensure that a write operation has been successfully committed.",
            tableIdentifier);
    this.maintenance = this.pipeline.apply(IMPULSE, Create.of(SnapshotInfo.fromSnapshot(snapshot)));
    this.operationId = UUID.randomUUID().toString();
  }

  public static IcebergMaintenance create(
      String tableIdentifier, Map<String, String> catalogConfig) {
    return new IcebergMaintenance(tableIdentifier, catalogConfig, null, null);
  }

  public static IcebergMaintenance create(
      String tableIdentifier,
      Map<String, String> catalogConfig,
      @Nullable PipelineOptions pipelineOptions) {
    return new IcebergMaintenance(tableIdentifier, catalogConfig, pipelineOptions, null);
  }

  public static IcebergMaintenance create(
      String tableIdentifier, Map<String, String> catalogConfig, @Nullable Pipeline pipeline) {
    return new IcebergMaintenance(tableIdentifier, catalogConfig, null, pipeline);
  }

  public IcebergMaintenance rewriteDataFiles() {
    return rewriteDataFiles(RewriteDataFiles.Configuration.builder().build());
  }

  public IcebergMaintenance rewriteDataFiles(RewriteDataFiles.Configuration rewriteConfig) {
    checkNotAddedYet(RewriteDataFiles.class);
    LOG.info(
        MAINTENANCE_PREFIX + " Adding {} task with config: {}",
        RewriteDataFiles.class.getSimpleName(),
        rewriteConfig);
    maintenance =
        maintenance.apply(
            RewriteDataFiles.create(
                tableIdentifier, table, catalogConfig, rewriteConfig, operationId));
    return this;
  }

  public PipelineResult run() {
    checkNotEmpty();
    LOG.info(
        MAINTENANCE_PREFIX + " Running maintenance on table {} with operation-id: {}",
        tableIdentifier,
        operationId);
    return pipeline.run();
  }

  private void checkNotAddedYet(Class<?> transform) {
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            @Nullable PTransform<?, ?> nodeT = node.getTransform();
            if (nodeT != null && nodeT.getClass().equals(transform)) {
              throw new IllegalStateException(
                  String.format(
                      "A '%s' task can only be applied once per maintenance operation. Please remove the duplicate task.",
                      transform.getSimpleName()));
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
  }

  public void checkNotEmpty() {
    boolean[] isEmpty = new boolean[] {true};
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (node.getTransform() != null && !node.getFullName().startsWith(IMPULSE)) {
              isEmpty[0] = false;
              return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    if (isEmpty[0]) {
      throw new IllegalStateException(
          String.format(
              "Maintenance operation for Iceberg table '%s' is empty. Please apply at least one task.",
              tableIdentifier));
    }
  }
}
