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
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor.Guard;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An updatable cache for table schemas. */
public class TableSchemaCache {
  @AutoValue
  abstract static class SchemaHolder {
    abstract TableSchema getTableSchema();

    abstract int getVersion();

    static SchemaHolder of(TableSchema tableSchema, int version) {
      return new AutoValue_TableSchemaCache_SchemaHolder(tableSchema, version);
    }
  }

  @AutoValue
  abstract static class Refresh {
    abstract DatasetService getDatasetService();

    abstract int getTargetVersion();

    static Refresh of(DatasetService datasetService, int targetVersion) {
      return new AutoValue_TableSchemaCache_Refresh(datasetService, targetVersion);
    }
  }

  private final Map<String, SchemaHolder> cachedSchemas = Maps.newHashMap();
  private Map<String, Refresh> tablesToRefresh = Maps.newHashMap();
  private final Monitor tableUpdateMonitor;
  private final Monitor.Guard tableUpdateGuard;
  private final Duration minSchemaRefreshFrequency;
  private final ExecutorService refreshExecutor;
  private boolean stopped;
  private boolean clearing;
  private static final Logger LOG = LoggerFactory.getLogger(TableSchemaCache.class);

  TableSchemaCache(Duration minSchemaRefreshFrequency) {
    this.tableUpdateMonitor = new Monitor();
    this.tableUpdateGuard =
        new Guard(tableUpdateMonitor) {
          @Override
          public boolean isSatisfied() {
            return !tablesToRefresh.isEmpty() || stopped || clearing;
          }
        };
    this.refreshExecutor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setThreadFactory(MoreExecutors.platformThreadFactory())
                .setDaemon(true)
                .setNameFormat("BigQuery table schema refresh thread")
                .build());
    this.minSchemaRefreshFrequency = minSchemaRefreshFrequency;
    this.stopped = false;
    this.clearing = false;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  void start() {
    this.refreshExecutor.submit(this::refreshThread);
  }

  void clear() throws ExecutionException, InterruptedException {
    // Wait for the thread to exit.
    runUnderMonitor(
        () -> {
          clearing = true;
          tableUpdateMonitor.waitForUninterruptibly(
              new Guard(tableUpdateMonitor) {
                @Override
                public boolean isSatisfied() {
                  return stopped;
                }
              });
        });
    // Clear and restart thread.
    runUnderMonitor(
        () -> {
          this.cachedSchemas.clear();
          clearing = false;
          stopped = false;
        });
    start();
  }

  private void runUnderMonitor(Runnable runnable) {
    tableUpdateMonitor.enter();
    try {
      runnable.run();
    } finally {
      tableUpdateMonitor.leave();
    }
  }

  private <T> void runUnderMonitor(Consumer<T> consumer, T value) {
    tableUpdateMonitor.enter();
    try {
      consumer.accept(value);
    } finally {
      tableUpdateMonitor.leave();
    }
  }

  private <T> T runUnderMonitor(Supplier<T> supplier) {
    tableUpdateMonitor.enter();
    try {
      return supplier.get();
    } finally {
      tableUpdateMonitor.leave();
    }
  }

  private static String tableKey(TableReference tableReference) {
    return BigQueryHelpers.stripPartitionDecorator(BigQueryHelpers.toTableSpec(tableReference));
  }

  @Nullable
  public TableSchema getSchema(TableReference tableReference, DatasetService datasetService) {
    Optional<SchemaHolder> schemaHolder;
    // We don't use computeIfAbsent here, as we want to avoid calling into datasetService (which can
    // be an RPC
    // with the monitor locked).
    final String key = tableKey(tableReference);
    schemaHolder = runUnderMonitor(() -> Optional.ofNullable(cachedSchemas.get(key)));
    if (!schemaHolder.isPresent()) {
      // Not initialized. Query the new schema with the monitor released and then update the cache.
      try {
        // requesting the BASIC view will prevent BQ backend to run calculations
        // related with storage stats that are not needed here
        @Nullable
        Table table =
            datasetService.getTable(
                tableReference, Collections.emptyList(), DatasetService.TableMetadataView.BASIC);
        schemaHolder =
            Optional.ofNullable((table == null) ? null : SchemaHolder.of(table.getSchema(), 0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (schemaHolder.isPresent()) {
        runUnderMonitor(h -> this.cachedSchemas.put(key, h.get()), schemaHolder);
      }
    }
    return schemaHolder.map(SchemaHolder::getTableSchema).orElse(null);
  }

  /**
   * Registers schema for a table if one is not already present. If a schema is already in the
   * cache, returns the existing schema, otherwise returns null.
   */
  @Nullable
  public TableSchema putSchemaIfAbsent(TableReference tableReference, TableSchema tableSchema) {
    final String key = tableKey(tableReference);
    Optional<SchemaHolder> existing =
        runUnderMonitor(
            () ->
                Optional.ofNullable(
                    this.cachedSchemas.putIfAbsent(key, SchemaHolder.of(tableSchema, 0))));
    return existing.map(SchemaHolder::getTableSchema).orElse(null);
  }

  public void refreshSchema(TableReference tableReference, DatasetService datasetService) {
    int targetVersion =
        runUnderMonitor(
            () -> {
              if (stopped) {
                throw new RuntimeException(
                    "Cannot call refreshSchema after the object has been stopped!");
              }
              String key = tableKey(tableReference);
              @Nullable SchemaHolder schemaHolder = cachedSchemas.get(key);
              int nextVersion = schemaHolder != null ? schemaHolder.getVersion() + 1 : 0;
              tablesToRefresh.put(key, Refresh.of(datasetService, nextVersion));
              // Wait at least until the next version.
              return nextVersion;
            });
    waitForRefresh(tableReference, targetVersion);
  }

  private void waitForRefresh(TableReference tableReference, int version) {
    tableUpdateMonitor.enterWhenUninterruptibly(
        new Guard(tableUpdateMonitor) {
          @Override
          public boolean isSatisfied() {
            if (stopped) {
              return false;
            }
            SchemaHolder schemaHolder = cachedSchemas.get(tableKey(tableReference));
            if (schemaHolder == null) {
              return false;
            }
            return schemaHolder.getVersion() >= version;
          }
        });
    tableUpdateMonitor.leave();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void refreshThread() {
    Instant start = Instant.now();
    try {
      Map<String, Refresh> localTablesToRefresh;
      {
        // Block until the refresh set has something in it.
        tableUpdateMonitor.enterWhen(tableUpdateGuard);
        try {
          if (clearing) {
            stopped = true;
            return;
          }
          localTablesToRefresh = this.tablesToRefresh;
          this.tablesToRefresh = Maps.newHashMap();

          // Filter out tables that have already been refreshed.
          localTablesToRefresh
              .entrySet()
              .removeIf(
                  entry -> {
                    @Nullable SchemaHolder schemaHolder = cachedSchemas.get(entry.getKey());
                    return schemaHolder != null
                        && schemaHolder.getVersion() >= entry.getValue().getTargetVersion();
                  });
        } finally {
          tableUpdateMonitor.leave();
        }
      }

      // Query all the tables for their schema.
      final Map<String, TableSchema> schemas = refreshAll(localTablesToRefresh);

      runUnderMonitor(
          () -> {
            // Update the cache schemas.
            for (Map.Entry<String, TableSchema> entry : schemas.entrySet()) {
              SchemaHolder schemaHolder = cachedSchemas.get(entry.getKey());
              if (schemaHolder == null) {
                throw new RuntimeException("Unexpected null schema for " + entry.getKey());
              }
              SchemaHolder newSchema =
                  SchemaHolder.of(entry.getValue(), schemaHolder.getVersion() + 1);
              cachedSchemas.put(entry.getKey(), newSchema);
            }
          });

      // Rate limit this thread so that we query tables for their schema at most once every
      // minSchemaRefreshFrequency.
      Duration timeElapsed = new Duration(start, Instant.now());
      Duration timeRemaining = minSchemaRefreshFrequency.minus(timeElapsed);
      if (timeRemaining.getMillis() > 0) {
        Thread.sleep(timeRemaining.getMillis());
      }
    } catch (Exception e) {
      // Since this is a daemon thread, don't exit until it is explicitly shut down. Exiting early
      // can cause the
      // pipeline to stall.
      LOG.error("Caught exception in BigQuery's table schema cache refresh thread: " + e);
    }
    this.refreshExecutor.submit(this::refreshThread);
  }

  private Map<String, TableSchema> refreshAll(Map<String, Refresh> tables)
      throws IOException, InterruptedException {
    Map<String, TableSchema> schemas = Maps.newHashMapWithExpectedSize(tables.size());
    for (Map.Entry<String, Refresh> entry : tables.entrySet()) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(entry.getKey());
      Table table =
          entry
              .getValue()
              .getDatasetService()
              .getTable(
                  tableReference, Collections.emptyList(), DatasetService.TableMetadataView.BASIC);
      if (table == null) {
        throw new RuntimeException("Did not get value for table " + tableReference);
      }
      LOG.info("Refreshed BigQuery schema for " + entry.getKey());
      schemas.put(entry.getKey(), table.getSchema());
    }
    return schemas;
  }
}
