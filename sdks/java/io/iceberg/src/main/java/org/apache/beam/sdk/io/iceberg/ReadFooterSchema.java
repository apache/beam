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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.metrics.Metrics.counter;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.FileFormat;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits one {@link CollectDistinctSchemas.SchemaGroup} of one file per readable Parquet file: its
 * declared schema and its null-free columns. Unreadable or non-Parquet files contribute nothing.
 */
class ReadFooterSchema
    extends DoFn<KV<String, String>, KV<String, CollectDistinctSchemas.SchemaGroup>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadFooterSchema.class);

  static final int DEFAULT_THREAD_POOL_SIZE = 10;
  static final int DEFAULT_MAX_IN_FLIGHT_TASKS = 100;
  static final String FILES_READ_COUNTER = "numFilesRead";
  static final String SCHEMAS_EMITTED_COUNTER = "numSchemasEmitted";
  static final String FOOTER_READ_ERRORS_COUNTER = "numFooterReadErrors";
  private static final Counter numFilesRead = counter(ReadFooterSchema.class, FILES_READ_COUNTER);
  private static final Counter numSchemasEmitted =
      counter(ReadFooterSchema.class, SCHEMAS_EMITTED_COUNTER);
  private static final Counter numFooterReadErrors =
      counter(ReadFooterSchema.class, FOOTER_READ_ERRORS_COUNTER);

  /**
   * Emitted in a dry run for files that contribute no schema. The key travels in the group's schema
   * JSON field, which the combine groups by, so the files of one kind add up without a second path.
   */
  static final String UNREADABLE_KEY = "unread";

  static final String UNCHECKED_FORMAT_KEY = "unchecked";

  private final SchemaEvolutionConfig config;
  private final @Nullable String locationPrefix;
  private final int threadPoolSize;
  private final int maxInFlightTasks;
  private transient @MonotonicNonNull BoundedAsyncTasks<ReadResult> tasks;

  ReadFooterSchema(SchemaEvolutionConfig config) {
    this(config, null);
  }

  /** {@code locationPrefix}: where the {@code key=value} folders start, e.g. gs://lake/orders/. */
  ReadFooterSchema(SchemaEvolutionConfig config, @Nullable String locationPrefix) {
    this(config, locationPrefix, DEFAULT_THREAD_POOL_SIZE, DEFAULT_MAX_IN_FLIGHT_TASKS);
  }

  ReadFooterSchema(SchemaEvolutionConfig config, int threadPoolSize, int maxInFlightTasks) {
    this(config, null, threadPoolSize, maxInFlightTasks);
  }

  ReadFooterSchema(
      SchemaEvolutionConfig config,
      @Nullable String locationPrefix,
      int threadPoolSize,
      int maxInFlightTasks) {
    this.config = config;
    this.locationPrefix = locationPrefix;
    this.threadPoolSize = threadPoolSize;
    this.maxInFlightTasks = maxInFlightTasks;
  }

  /** Counters are updated on the processing thread: metrics touched from the executor are lost. */
  private static class ReadResult {
    final String table;
    final CollectDistinctSchemas.@Nullable SchemaGroup schema;
    final boolean footerError;
    final Instant timestamp;
    final BoundedWindow window;
    final PaneInfo paneInfo;

    ReadResult(
        String table,
        CollectDistinctSchemas.@Nullable SchemaGroup schema,
        boolean footerError,
        Instant timestamp,
        BoundedWindow window,
        PaneInfo paneInfo) {
      this.table = table;
      this.schema = schema;
      this.footerError = footerError;
      this.timestamp = timestamp;
      this.window = window;
      this.paneInfo = paneInfo;
    }
  }

  @Setup
  public void setup() {
    tasks = new BoundedAsyncTasks<>(threadPoolSize, maxInFlightTasks);
  }

  /** Clears anything left behind if the runner reuses this instance after a failed bundle. */
  @StartBundle
  public void startBundle() {
    checkStateNotNull(tasks).cancelAll();
  }

  @Teardown
  public void teardown() {
    if (tasks != null) {
      tasks.shutdown();
    }
  }

  @ProcessElement
  public void process(
      @Element KV<String, String> element,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo,
      OutputReceiver<KV<String, CollectDistinctSchemas.SchemaGroup>> output)
      throws Exception {
    numFilesRead.inc();
    Callable<ReadResult> task =
        createReadTask(element.getKey(), element.getValue(), timestamp, window, paneInfo);
    checkStateNotNull(tasks).submit(task, result -> outputResult(result, output));
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    checkStateNotNull(tasks).awaitAll(result -> outputAtFinish(result, context));
  }

  private static void outputAtFinish(ReadResult result, FinishBundleContext context) {
    count(result);
    if (result.schema != null) {
      context.output(KV.of(result.table, result.schema), result.timestamp, result.window);
    }
  }

  private static void outputResult(
      ReadResult result, OutputReceiver<KV<String, CollectDistinctSchemas.SchemaGroup>> output) {
    count(result);
    if (result.schema != null) {
      output.outputWindowedValue(
          KV.of(result.table, result.schema),
          result.timestamp,
          Collections.singleton(result.window),
          result.paneInfo);
    }
  }

  private static void count(ReadResult result) {
    if (result.schema != null) {
      numSchemasEmitted.inc();
    }
    if (result.footerError) {
      numFooterReadErrors.inc();
    }
  }

  private Callable<ReadResult> createReadTask(
      String table, String filePath, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return () -> {
      ParquetMetadata footer;
      if (AddFiles.hasNoExtension(filePath)) {
        try {
          footer = ParquetFooters.read(filePath);
        } catch (Exception e) {
          return unknownFormat(table, timestamp, window, paneInfo);
        }
      } else {
        FileFormat format;
        try {
          format = AddFiles.inferFormat(filePath);
        } catch (AddFiles.UnknownFormatException e) {
          return unknownFormat(table, timestamp, window, paneInfo);
        }
        if (!format.equals(FileFormat.PARQUET)) {
          return new ReadResult(
              table, dryRunMarker(UNCHECKED_FORMAT_KEY), false, timestamp, window, paneInfo);
        }
        try {
          footer = ParquetFooters.read(filePath);
        } catch (Exception e) {
          return unreadableFooter(table, filePath, e, timestamp, window, paneInfo);
        }
      }
      try {
        List<String> partitionKeysInPath =
            new ArrayList<>(PartitionPaths.partitionKeysInPath(filePath, locationPrefix));
        CollectDistinctSchemas.SchemaGroup group =
            FileSchemas.schemaGroup(footer).withPartitionKeysInPath(partitionKeysInPath);
        return new ReadResult(table, group, false, timestamp, window, paneInfo);
      } catch (Exception e) {
        return unconvertibleSchema(table, filePath, e, timestamp, window, paneInfo);
      }
    };
  }

  private ReadResult unknownFormat(
      String table, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return new ReadResult(table, dryRunMarker(UNREADABLE_KEY), false, timestamp, window, paneInfo);
  }

  private ReadResult unreadableFooter(
      String table,
      String filePath,
      Exception e,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo) {
    LOG.warn(
        "Could not read the footer of {}; the file will not contribute to schema inference: {}",
        filePath,
        AddFiles.errorMessage(e));
    return new ReadResult(table, dryRunMarker(UNREADABLE_KEY), true, timestamp, window, paneInfo);
  }

  private ReadResult unconvertibleSchema(
      String table,
      String filePath,
      Exception e,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo) {
    LOG.warn(
        "Could not convert the schema of {}; the file will not contribute to schema inference: {}",
        filePath,
        AddFiles.errorMessage(e));
    return new ReadResult(table, dryRunMarker(UNREADABLE_KEY), false, timestamp, window, paneInfo);
  }

  /** A dry run counts files that contribute no schema; a real run handles them at registration. */
  private CollectDistinctSchemas.@Nullable SchemaGroup dryRunMarker(String key) {
    if (!config.getDryRun()) {
      return null;
    }
    return CollectDistinctSchemas.SchemaGroup.of(key, 1, Collections.emptyList());
  }
}
