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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for Iceberg table maintenance as a Beam pipeline: build one with {@link #create}, add
 * tasks (currently only {@link #rewriteDataFiles} compaction, see {@link RewriteDataFiles} for
 * details), then {@link #run}.
 */
public class IcebergMaintenance {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenance.class);
  public static final String MAINTENANCE_PREFIX = "[Maintenance]";
  private static final String IMPULSE = "Impulse";
  private final Pipeline pipeline;
  private final SerializableTable table;
  private final String tableIdentifier;
  private final IcebergCatalogConfig catalogConfig;
  private @Nullable PCollection<RewriteResult> rewriteResult;

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
            SerializableTable.copyOf(TableCache.getRefreshed(this.catalogConfig, tableIdentifier));
  }

  /** Run table maintenance with default options. */
  public static IcebergMaintenance create(
      String tableIdentifier, Map<String, String> catalogConfig) {
    return new IcebergMaintenance(tableIdentifier, catalogConfig, null, null);
  }

  /** Run table maintenance with specified {@link PipelineOptions}. */
  public static IcebergMaintenance create(
      String tableIdentifier,
      Map<String, String> catalogConfig,
      @Nullable PipelineOptions pipelineOptions) {
    return new IcebergMaintenance(tableIdentifier, catalogConfig, pipelineOptions, null);
  }

  /** Run table maintenance on an existing pipeline. */
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

    // Resolve the head to compact: explicit snapshot id, else the branch ref, else main's current
    // snapshot. A null head means nothing to compact -> a graceful empty-impulse no-op.
    @Nullable String branch = rewriteConfig.getBranch();
    @Nullable Long snapshotId = rewriteConfig.getSnapshotId();
    @Nullable Snapshot head;
    if (snapshotId != null) {
      head = table.snapshot(snapshotId);
      if (head == null) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot rewrite data files: snapshot %s not found in table '%s'.",
                snapshotId, tableIdentifier));
      }
    } else if (branch != null) {
      SnapshotRef ref = table.refs().get(branch);
      if (ref == null) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot rewrite data files on branch '%s' of table '%s': the branch does not exist.",
                branch, tableIdentifier));
      }
      head = table.snapshot(ref.snapshotId());
    } else {
      head = table.currentSnapshot();
    }

    PCollection<SnapshotInfo> impulse;
    if (head == null) {
      LOG.warn(
          "{} {} has no snapshot yet; maintenance will be a no-op.",
          MAINTENANCE_PREFIX,
          branch != null ? "Branch '" + branch + "'" : "Table '" + tableIdentifier + "'");
      impulse = pipeline.apply(IMPULSE, Create.empty(RewriteDataFiles.SNAPSHOT_CODER));
    } else {
      impulse = pipeline.apply(IMPULSE, Create.of(SnapshotInfo.fromSnapshot(head)));
    }

    PCollectionTuple output =
        impulse.apply(
            RewriteDataFiles.create(tableIdentifier, table, catalogConfig, rewriteConfig));
    this.rewriteResult = output.get(RewriteDataFiles.RESULT);
    this.rewriteResult.apply("Log Rewrite Result", ParDo.of(new LogResultFn()));
    return this;
  }

  /**
   * The single structured {@link RewriteResult} summary of the added rewrite task, for {@code
   * PAssert}/sinking by embed-in-pipeline users. Throws if no rewrite task has been added yet.
   */
  public PCollection<RewriteResult> rewriteResult() {
    if (rewriteResult == null) {
      throw new IllegalStateException(
          "No rewrite task has been added; call rewriteDataFiles(...) before rewriteResult().");
    }
    return rewriteResult;
  }

  /** Logs the one {@link RewriteResult} row so a run's outcome is visible in the worker logs. */
  private static class LogResultFn extends DoFn<RewriteResult, Void> {
    @ProcessElement
    public void process(@Element RewriteResult result) {
      LOG.info(MAINTENANCE_PREFIX + " Rewrite result: {}", result);
    }
  }

  public PipelineResult run() {
    checkNotEmpty();
    LOG.info(MAINTENANCE_PREFIX + " Running maintenance on table {}.", tableIdentifier);
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

  void checkNotEmpty() {
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
