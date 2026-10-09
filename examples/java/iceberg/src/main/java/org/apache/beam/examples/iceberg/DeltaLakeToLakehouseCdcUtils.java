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
package org.apache.beam.examples.iceberg;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcExample.Options;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for {@link DeltaLakeToLakehouseCdcExample} that parse the target Lakehouse table
 * identifier and validate the pipeline options and the source Delta Lake table.
 */
final class DeltaLakeToLakehouseCdcUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeToLakehouseCdcUtils.class);

  /** Delta Lake table property that enables the Change Data Feed of a table. */
  static final String ENABLE_CHANGE_DATA_FEED_PROPERTY = "delta.enableChangeDataFeed";

  private DeltaLakeToLakehouseCdcUtils() {}

  /** Parsed GCP Lakehouse table coordinates (`project`, `warehouse`, and `namespace.table`). */
  static final class LakehouseTableSpec {
    final String project;
    final String warehouse;
    final String tableId;

    LakehouseTableSpec(String project, String warehouse, String tableId) {
      this.project = project;
      this.warehouse = warehouse;
      this.tableId = tableId;
    }
  }

  /**
   * Resolves the GCP project, GCS warehouse URI, and 2-part Iceberg {@code namespace.table}
   * identifier from {@link Options}.
   */
  static LakehouseTableSpec resolveLakehouseTableSpec(Options options) {
    String rawTable = options.getLakehouseTable();
    if (rawTable == null || rawTable.trim().isEmpty()) {
      throw new IllegalArgumentException("--lakehouseTable must not be empty.");
    }
    List<String> parts = Arrays.asList(rawTable.trim().split("\\."));
    @Nullable String project = options.getProject();
    @Nullable String warehouse = options.getWarehouse();
    String tableId;

    if (parts.size() == 4) {
      project = parts.get(0);
      if (warehouse == null || warehouse.trim().isEmpty()) {
        warehouse = "gs://" + parts.get(1);
      }
      tableId = parts.get(2) + "." + parts.get(3);
    } else if (parts.size() == 3) {
      if (warehouse == null || warehouse.trim().isEmpty()) {
        warehouse = "gs://" + parts.get(0);
      }
      tableId = parts.get(1) + "." + parts.get(2);
    } else if (parts.size() == 2) {
      tableId = rawTable.trim();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Invalid --lakehouseTable '%s'. Expected '<project>.<warehouse_bucket>.<namespace>.<table>', "
                  + "'<warehouse_bucket>.<namespace>.<table>', or '<namespace>.<table>'.",
              rawTable));
    }

    if (project == null || project.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "GCP project must be specified via --project or as the first component of a 4-part "
              + "--lakehouseTable identifier.");
    }
    if (warehouse == null || warehouse.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Lakehouse warehouse bucket must be specified via --warehouse or as part of a 3-part / "
              + "4-part --lakehouseTable identifier.");
    }
    String normalizedWarehouse =
        warehouse.startsWith("gs://") ? warehouse : "gs://" + warehouse.trim();

    return new LakehouseTableSpec(project.trim(), normalizedWarehouse, tableId);
  }

  /**
   * Validates that either a commit version range or a timestamp range (and not a mix of both) is
   * configured on {@link Options}.
   */
  static void validateRangeOptions(Options options) {
    @Nullable Long startVersion = options.getStartVersion();
    @Nullable Long endVersion = options.getEndVersion();
    @Nullable String startTimestamp = options.getStartTimestamp();
    @Nullable String endTimestamp = options.getEndTimestamp();

    boolean hasStartVersion = startVersion != null;
    boolean hasEndVersion = endVersion != null;
    boolean hasStartTimestamp = startTimestamp != null && !startTimestamp.trim().isEmpty();
    boolean hasEndTimestamp = endTimestamp != null && !endTimestamp.trim().isEmpty();

    if (!hasStartVersion && !hasStartTimestamp) {
      throw new IllegalArgumentException(
          "Either --startVersion or --startTimestamp must be provided to read Delta Lake CDC data.");
    }
    if (hasStartVersion && hasStartTimestamp) {
      throw new IllegalArgumentException(
          "Cannot set both --startVersion and --startTimestamp; specify either a version range or "
              + "a timestamp range.");
    }
    if (hasEndVersion && hasEndTimestamp) {
      throw new IllegalArgumentException(
          "Cannot set both --endVersion and --endTimestamp; specify either a version range or "
              + "a timestamp range.");
    }
    if (hasStartVersion && hasEndTimestamp) {
      throw new IllegalArgumentException(
          "Cannot mix --startVersion with --endTimestamp; use --endVersion instead.");
    }
    if (hasStartTimestamp && hasEndVersion) {
      throw new IllegalArgumentException(
          "Cannot mix --startTimestamp with --endVersion; use --endTimestamp instead.");
    }
    if (startVersion != null && endVersion != null && startVersion > endVersion) {
      throw new IllegalArgumentException(
          String.format(
              "--startVersion (%d) must be less than or equal to --endVersion (%d).",
              startVersion, endVersion));
    }
  }

  /**
   * Verifies that Change Data Feed is enabled on the input Delta Lake table, i.e. that the latest
   * snapshot of the table has the table property {@code delta.enableChangeDataFeed = true}.
   *
   * <p>The table is read using the Delta Kernel API with the given Hadoop configuration (for
   * example, the GCS connector settings that the Delta Lake CDC source uses to access the table).
   *
   * @throws IllegalArgumentException if there is no Delta Lake table at {@code deltaTablePath}
   * @throws IllegalStateException if {@code delta.enableChangeDataFeed = true} is not set
   */
  static void verifyChangeDataFeedEnabled(String deltaTablePath, Map<String, String> hadoopConfig) {
    Configuration conf = new Configuration();
    hadoopConfig.forEach(conf::set);
    Engine engine = DefaultEngine.create(conf);

    Snapshot snapshot;
    try {
      snapshot = Table.forPath(engine, deltaTablePath).getLatestSnapshot(engine);
    } catch (TableNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "No Delta Lake table found at '%s'. Verify that it is a valid Delta Lake table.",
              deltaTablePath),
          e);
    }

    @Nullable String changeDataFeed =
        snapshot.getTableProperties().get(ENABLE_CHANGE_DATA_FEED_PROPERTY);
    if (!"true".equalsIgnoreCase(changeDataFeed)) {
      throw new IllegalStateException(
          String.format(
              "Delta Lake table '%s' does not have 'delta.enableChangeDataFeed = true' enabled "
                  + "(found '%s' at version %d). Enable Change Data Feed on the table before "
                  + "running this pipeline.",
              deltaTablePath, changeDataFeed, snapshot.getVersion()));
    }
    LOG.info(
        "Verified 'delta.enableChangeDataFeed = true' on Delta Lake table '{}' (version {}).",
        deltaTablePath,
        snapshot.getVersion());
  }
}
