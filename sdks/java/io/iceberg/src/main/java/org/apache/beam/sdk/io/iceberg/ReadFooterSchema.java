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

import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
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
class ReadFooterSchema extends DoFn<String, CollectDistinctSchemas.SchemaGroup> {
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

  private final int threadPoolSize;
  private final int maxInFlightTasks;
  private transient @MonotonicNonNull BoundedAsyncTasks<ReadResult> tasks;

  ReadFooterSchema() {
    this(DEFAULT_THREAD_POOL_SIZE, DEFAULT_MAX_IN_FLIGHT_TASKS);
  }

  ReadFooterSchema(int threadPoolSize, int maxInFlightTasks) {
    this.threadPoolSize = threadPoolSize;
    this.maxInFlightTasks = maxInFlightTasks;
  }

  /** Counters are updated on the processing thread: metrics touched from the executor are lost. */
  private static class ReadResult {
    final CollectDistinctSchemas.@Nullable SchemaGroup schema;
    final boolean footerError;
    final Instant timestamp;
    final BoundedWindow window;
    final PaneInfo paneInfo;

    ReadResult(
        CollectDistinctSchemas.@Nullable SchemaGroup schema,
        boolean footerError,
        Instant timestamp,
        BoundedWindow window,
        PaneInfo paneInfo) {
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
      @Element String filePath,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo paneInfo,
      OutputReceiver<CollectDistinctSchemas.SchemaGroup> output)
      throws Exception {
    numFilesRead.inc();
    Callable<ReadResult> task = createReadTask(filePath, timestamp, window, paneInfo);
    checkStateNotNull(tasks).submit(task, result -> outputResult(result, output));
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    checkStateNotNull(tasks).awaitAll(result -> outputAtFinish(result, context));
  }

  private static void outputAtFinish(ReadResult result, FinishBundleContext context) {
    count(result);
    if (result.schema != null) {
      context.output(result.schema, result.timestamp, result.window);
    }
  }

  private static void outputResult(
      ReadResult result, OutputReceiver<CollectDistinctSchemas.SchemaGroup> output) {
    count(result);
    if (result.schema != null) {
      output.outputWindowedValue(
          result.schema, result.timestamp, Collections.singleton(result.window), result.paneInfo);
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

  private static Callable<ReadResult> createReadTask(
      String filePath, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return () -> {
      FileFormat format;
      try {
        format = AddFiles.inferFormat(filePath);
      } catch (AddFiles.UnknownFormatException e) {
        return new ReadResult(null, false, timestamp, window, paneInfo);
      }
      if (!format.equals(FileFormat.PARQUET)) {
        return new ReadResult(null, false, timestamp, window, paneInfo);
      }
      try {
        ParquetMetadata footer = ParquetFooters.read(filePath);
        return new ReadResult(FileSchemas.schemaGroup(footer), false, timestamp, window, paneInfo);
      } catch (Exception e) {
        LOG.warn(
            "Could not read the footer of {}; the file will not contribute to schema inference: {}",
            filePath,
            AddFiles.errorMessage(e));
        return new ReadResult(null, true, timestamp, window, paneInfo);
      }
    };
  }
}
