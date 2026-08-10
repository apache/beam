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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A keyed {@link DoFn} that rewrites a single {@link RewriteSubGroup} (one subgroup) into ~one
 * target-sized output file and emits a single {@link ExecutedGroup} under the same commit key.
 *
 * <p>The upstream {@link PlanRewriteGroups} creates planned parent groups. To increase parallelism,
 * we split each parent group into subgroups (represented as {@link RewriteSubGroup}s) and spread
 * them across workers.
 *
 * <p>A subgroup is read sequentially here into ~one target-sized output file (via {@link
 * RewriteSubGroup#getWriteMaxFileSize()}). An oversized group can roll into multiple target-sized
 * outputs.
 */
class RewriteSubGroupDoFn extends DoFn<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteSubGroupDoFn.class);

  /** Main output: groups that were rewritten successfully and are ready to commit. */
  static final TupleTag<KV<Integer, ExecutedGroup>> REWRITTEN = new TupleTag<>() {};

  /**
   * If a subgroup fails, keep track of its parent group index. Downstream stages dedup to get the
   * total count of distinct failed parent groups. This count feeds (a) the atomic gate's
   * all-or-nothing abort decision and (b) the {@link RewriteResult}'s {@code failedRewriteParents}.
   */
  static final TupleTag<Integer> FAILED_PARENTS = new TupleTag<>() {};

  private final SerializableTable table;
  private final FileFormat format;
  // User-supplied write properties that override the table's write properties for the rewrite
  private final Map<String, String> writeProperties;
  // for v3 row-lineage tables: preserve _row_id and _last_updated_sequence_number
  private final boolean preserveRowLineage;

  private static final int MAX_REWRITE_ATTEMPTS = 3;

  private static final Counter activeRewriters =
      Metrics.counter(RewriteSubGroupDoFn.class, "activeRewriters");
  private static final Counter rewriteRetries =
      Metrics.counter(RewriteSubGroupDoFn.class, "rewriteRetries");
  private static final Distribution outputFileByteSize =
      Metrics.distribution(RewriteSubGroupDoFn.class, "outputFileByteSize");

  @VisibleForTesting
  RewriteSubGroupDoFn(SerializableTable table) {
    this(table, Collections.emptyMap());
  }

  RewriteSubGroupDoFn(SerializableTable table, Map<String, String> writeProperties) {
    this.table = table;
    this.writeProperties = writeProperties;
    this.preserveRowLineage = TableUtil.supportsRowLineage(table);

    String fmt =
        PropertyUtil.propertyAsString(
            table.properties(),
            TableProperties.DEFAULT_FILE_FORMAT,
            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.format = FileFormat.fromString(fmt);

    if (format != FileFormat.PARQUET) {
      throw new UnsupportedOperationException(
          "Beam RewriteDataFiles currently supports only Parquet tables, but the table's "
              + "write.format.default is '"
              + fmt
              + "'. Please set the table property write.format.default=parquet.");
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, RewriteSubGroup> element, MultiOutputReceiver out) {
    int commitKey = element.getKey();
    RewriteSubGroup group = element.getValue();

    // We only support Parquet. Other file formats should fail the subgroup. This can
    // happen if the Table's currently specified file format is Parquet but happens
    // to still have older files of other formats.
    Map<Integer, PartitionSpec> specs = table.specs();
    List<TaskDescriptor> descriptors = group.getTaskDescriptors();
    List<FileScanTask> tasks = new ArrayList<>(descriptors.size());
    for (TaskDescriptor descriptor : descriptors) {
      FileScanTask t = descriptor.toScanTask(specs);
      if (t.file().format() != FileFormat.PARQUET) {
        throw new UnsupportedOperationException(
            "Beam RewriteDataFiles currently supports only Parquet, but input file "
                + t.file().location()
                + " has format "
                + t.file().format());
      }
      tasks.add(t);
    }

    activeRewriters.inc();
    ExecutedGroup result;
    try {
      result = rewriteWithRetry(group, tasks);
    } catch (Exception e) {
      if (isInterruption(e)) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Rewrite interrupted while the worker was shutting down; failing the bundle so the "
                + "runner can retry.",
            e);
      }
      // retries are exhausted. mark the upper parent group as failed.
      // original files are not deleted here.
      LOG.warn(
          RewriteDataFiles.REWRITE_PREFIX + "Rewrite failed for sub-group {}; routing aside.",
          group.getGlobalIndex(),
          e);
      out.get(FAILED_PARENTS).output(group.getParentGroupIndex());
      return;
    } finally {
      activeRewriters.dec();
    }

    // rewrite was successful. output to commit stage
    out.get(REWRITTEN).output(KV.of(commitKey, result));
  }

  /**
   * Rewrites the group with a bounded retry. Each attempt uses a FRESH writer and aborts its
   * partial output on failure; the group is marked failed if all retries are exhausted.
   */
  private ExecutedGroup rewriteWithRetry(RewriteSubGroup group, List<FileScanTask> tasks)
      throws Exception {
    BackOff backoff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(100))
            .withMaxBackoff(Duration.standardSeconds(2))
            .withMaxRetries(MAX_REWRITE_ATTEMPTS)
            .backoff();
    for (int attempt = 1; ; attempt++) {
      try {
        return rewriteOnce(group, tasks);
      } catch (Exception e) {
        // Interruptions indicate the worker shutting down. We should propagate them so the
        // bundle fails and the runner retries
        if (isInterruption(e) || attempt >= MAX_REWRITE_ATTEMPTS) {
          throw e;
        }
        rewriteRetries.inc();
        LOG.warn(
            RewriteDataFiles.REWRITE_PREFIX
                + "Rewrite attempt {}/{} failed for sub-group {}; retrying.",
            attempt,
            MAX_REWRITE_ATTEMPTS,
            group.getGlobalIndex(),
            e);
        BackOffUtils.next(Sleeper.DEFAULT, backoff);
      }
    }
  }

  /**
   * Attempts a rewrite by reading the group's inputs (applying the delete filter and row lineage
   * where supported) and writes the compacted output. Returns the equivalent {@link ExecutedGroup}.
   * On failure, throws and deletes the partially written output.
   */
  @VisibleForTesting
  ExecutedGroup rewriteOnce(RewriteSubGroup group, List<FileScanTask> tasks) throws Exception {
    @Nullable TaskWriter<Record> writer = null;
    try {
      PartitionSpec outputSpec =
          checkStateNotNull(
              table.specs().get(group.getOutputSpecId()),
              "Output partition spec id %s not found in table specs %s",
              group.getOutputSpecId(),
              table.specs().keySet());

      // Fresh per-attempt id to keep files unique between retries.
      long attemptId = ThreadLocalRandom.current().nextLong();
      WriterFactory wf =
          new WriterFactory(
              format,
              group.getWriteMaxFileSize(),
              attemptId,
              group.getGlobalIndex(),
              group.getOperationId(),
              outputSpec,
              writeProperties,
              preserveRowLineage);
      wf.init(table);

      writer = wf.create();

      for (int i = 0; i < tasks.size(); i++) {
        FileScanTask task = tasks.get(i);
        Schema requestedSchema =
            preserveRowLineage
                ? MetadataColumns.schemaWithRowLineage(table.schema())
                : table.schema();
        long dataSequenceNumber = group.getTaskDescriptors().get(i).getDataSequenceNumber();

        GenericDeleteFilter deleteFilter =
            new GenericDeleteFilter(table.io(), task, table.schema(), requestedSchema);
        boolean hasDeletes = !task.deletes().isEmpty();
        // When appropriate, read using the delete filter's requiredSchema, which includes its
        // own metadata columns used to identify deleted records.
        // The writer copies fields by position, so the additional metadata columns
        // (added after the original schema) are ignored on write.
        Schema requiredSchema = hasDeletes ? deleteFilter.requiredSchema() : requestedSchema;
        // Pass the original data sequence number captured at planning to preserve the
        // '_last_updated_sequence_number' lineage metadata
        try (CloseableIterable<Record> iterable =
            ReadUtils.createReader(task, table, requiredSchema, dataSequenceNumber)) {
          CloseableIterable<Record> reader = hasDeletes ? deleteFilter.filter(iterable) : iterable;
          for (Record record : reader) {
            writer.write(record);
          }
        }
      }

      // Finished writing. Gather data files and serialize them.
      DataFile[] dataFiles = writer.dataFiles();
      List<SerializableDataFile> newFiles = new ArrayList<>(dataFiles.length);
      for (DataFile df : dataFiles) {
        newFiles.add(SerializableDataFile.from(df, table.specs()));
        outputFileByteSize.update(df.fileSizeInBytes());
      }
      writer.close();

      // Build small commit descriptors
      List<SerializableDataFile> rewrittenDataFiles = new ArrayList<>(tasks.size());
      List<String> danglingDeleteFileJsons = new ArrayList<>();
      for (FileScanTask t : tasks) {
        // Column stats don't matter on files marked for deletion, so we drop them to keep
        // the payload small.
        rewrittenDataFiles.add(
            SerializableDataFile.from(t.file().copyWithoutStats(), table.specs()));
        // include dangling deletion vectors (compact per-file JSONs)
        for (DeleteFile delete : t.deletes()) {
          if (ContentFileUtil.isDV(delete)) {
            PartitionSpec deleteSpec =
                checkStateNotNull(
                    table.specs().get(delete.specId()),
                    "Delete file spec id %s not found in table specs",
                    delete.specId());
            danglingDeleteFileJsons.add(ContentFileParser.toJson(delete, deleteSpec));
          }
        }
      }

      return ExecutedGroup.builder()
          .setStartingSnapshotId(group.getStartingSnapshotId())
          .setStartingSequenceNumber(group.getStartingSequenceNumber())
          .setOperationId(group.getOperationId())
          .setParentGroupIndex(group.getParentGroupIndex())
          .setParentSubgroupCount(group.getParentSubgroupCount())
          .setTotalInputByteSize(group.getTotalInputFileByteSize())
          .setNewFiles(newFiles)
          .setRewrittenDataFiles(rewrittenDataFiles)
          .setDanglingDeleteFileJsons(danglingDeleteFileJsons)
          .build();
    } catch (Exception e) {
      // Abort so this attempt's partial output is deleted (the writer may be null if creation/open
      // itself failed, in which case there is nothing to clean up), then rethrow for the retry
      // loop.
      if (writer != null) {
        try {
          writer.abort();
        } catch (Exception abortEx) {
          LOG.warn(
              RewriteDataFiles.REWRITE_PREFIX + "Failed to abort writer for sub-group {}",
              group.getGlobalIndex(),
              abortEx);
        }
      }
      throw e;
    }
  }

  /** Whether {@code t}, or any cause, is an interruption (the worker is shutting down). */
  private static boolean isInterruption(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof InterruptedException || c instanceof ClosedByInterruptException) {
        return true;
      }
    }
    return Thread.currentThread().isInterrupted();
  }

  /**
   * Row-lineage read constants for input task {@code i}. Starts from {@link
   * PartitionUtil#constantsMap} (which derives {@code _row_id} from the file's {@code first_row_id}
   * and supplies identity partition values), then restores {@code _last_updated_sequence_number}
   * from the DATA sequence number captured on the {@link TaskDescriptor} at planning time (the v3
   * spec derives {@code _lus} from the data sequence number). The {@code ContentFileParser} JSON
   * round-trip drops the file's sequence number, so without this every rewritten row's update
   * sequence would be written as null.
   */
}
