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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes files in batches and emits, for each batch, a {@link FileCategoryCounts} tallying the
 * files that were deleted.
 *
 * <p>This is an internal building block of the table maintenance operations, and is not intended to
 * be applied directly by users. Its input is the set of files that an operation has determined are
 * no longer reachable from any table state that must be preserved. Each table's files are deleted
 * with that table's {@link FileIO}. Callers combine the per-batch counts, for example with {@link
 * FileCategoryCounts#sum()}, into the operation's result.
 *
 * <p>Files are grouped into batches of at most {@link #withBatchSize(int) batchSize} files per
 * table. Batches are keyed by a sharded table key, so the batches of one large table are spread
 * across workers.
 *
 * <p>Each batch is deleted with a single bulk call when the table's {@link FileIO} supports {@link
 * SupportsBulkOperations}. If the bulk call fails, or bulk deletion is not supported, files are
 * deleted one at a time, sequentially within the batch. A file that no longer exists counts as
 * deleted, so retried bundles do not report spurious failures.
 *
 * <p>Failed deletions do not fail the pipeline. Failures reported by the {@link FileIO} are logged,
 * counted in the {@code failedFileDeletions} metric, and excluded from the emitted counts. Some
 * {@link FileIO} implementations do not report every failure (for example, {@code GCSFileIO} bulk
 * deletes and {@code ADLSFileIO} log failures instead of throwing), so the counts are the files
 * whose deletion was not reported as failed, which may include files that still exist. An
 * interrupted delete is not treated as a failed deletion: it fails the bundle so the work is
 * retried or cancelled.
 *
 * <p>Every input file must be a deletion candidate ({@link FileInfo#getReachable()} is false); a
 * batch containing a reachable file fails before anything in it is deleted. A batch whose table
 * cannot be loaded also fails. Input paths should be distinct: a path that appears more than once
 * is counted once for every appearance.
 *
 * <p>The input may be bounded or unbounded. With unbounded input, a batch smaller than {@code
 * batchSize} is only deleted when its window closes. The maintenance operations that use this
 * transform restrict their own input to bounded collections.
 */
@Internal
public class BatchDeleteFiles
    extends PTransform<PCollection<FileInfo>, PCollection<FileCategoryCounts>> {

  static final int DEFAULT_BATCH_SIZE = 10_000;

  static final String DELETED_FILES = "deletedFiles";
  static final String FAILED_FILE_DELETIONS = "failedFileDeletions";

  /** The number of failed deletions per batch that are logged individually, with stack traces. */
  static final int MAX_LOGGED_FAILURES = 10;

  private static final Logger LOG = LoggerFactory.getLogger(BatchDeleteFiles.class);

  private final IcebergCatalogConfig catalogConfig;
  private final int batchSize;
  private final boolean deleteEnabled;

  private BatchDeleteFiles(
      IcebergCatalogConfig catalogConfig, int batchSize, boolean deleteEnabled) {
    this.catalogConfig = catalogConfig;
    this.batchSize = batchSize;
    this.deleteEnabled = deleteEnabled;
  }

  /** Creates a transform that deletes files using the tables of {@code catalogConfig}. */
  public static BatchDeleteFiles of(IcebergCatalogConfig catalogConfig) {
    return new BatchDeleteFiles(catalogConfig, DEFAULT_BATCH_SIZE, true);
  }

  /**
   * Returns a copy of this transform that deletes at most {@code batchSize} files per batch.
   * Defaults to 10,000.
   *
   * @throws IllegalArgumentException if {@code batchSize} is not positive
   */
  public BatchDeleteFiles withBatchSize(int batchSize) {
    checkArgument(batchSize > 0, "Batch size must be positive, but was %s", batchSize);
    return new BatchDeleteFiles(catalogConfig, batchSize, deleteEnabled);
  }

  /**
   * Returns a copy of this transform that deletes files only if {@code deleteEnabled} is true.
   * Defaults to true.
   *
   * <p>When disabled, no files are deleted and the catalog is not contacted. The emitted counts
   * report the files that would have been deleted, and the {@code deletedFiles} and {@code
   * failedFileDeletions} metrics are not updated.
   */
  public BatchDeleteFiles withDeleteEnabled(boolean deleteEnabled) {
    return new BatchDeleteFiles(catalogConfig, batchSize, deleteEnabled);
  }

  @Override
  public PCollection<FileCategoryCounts> expand(PCollection<FileInfo> input) {
    Coder<FileInfo> fileInfoCoder = input.getCoder();
    return input
        .apply(
            "KeyByTable",
            WithKeys.of((SerializableFunction<FileInfo, String>) FileInfo::getTableIdentifier)
                .withKeyType(TypeDescriptors.strings()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), fileInfoCoder))
        .apply("Batch", GroupIntoBatches.<String, FileInfo>ofSize(batchSize).withShardedKey())
        .setCoder(
            KvCoder.of(ShardedKey.Coder.of(StringUtf8Coder.of()), IterableCoder.of(fileInfoCoder)))
        .apply("Delete", ParDo.of(new DeleteFilesDoFn(catalogConfig, deleteEnabled)));
  }

  static class DeleteFilesDoFn
      extends DoFn<KV<ShardedKey<String>, Iterable<FileInfo>>, FileCategoryCounts> {
    private final IcebergCatalogConfig catalogConfig;
    private final boolean deleteEnabled;
    private final Counter deletedFiles = Metrics.counter(BatchDeleteFiles.class, DELETED_FILES);
    private final Counter failedFileDeletions =
        Metrics.counter(BatchDeleteFiles.class, FAILED_FILE_DELETIONS);

    DeleteFilesDoFn(IcebergCatalogConfig catalogConfig, boolean deleteEnabled) {
      this.catalogConfig = catalogConfig;
      this.deleteEnabled = deleteEnabled;
    }

    @ProcessElement
    public void processElement(
        @Element KV<ShardedKey<String>, Iterable<FileInfo>> batch,
        OutputReceiver<FileCategoryCounts> output) {
      List<FileInfo> files = new ArrayList<>();
      List<String> paths = new ArrayList<>();
      for (FileInfo file : batch.getValue()) {
        // Deleting a reachable file would corrupt the table, so reject the whole batch up front.
        checkArgument(
            !file.getReachable(),
            "Refusing to delete reachable file %s of table %s",
            file.getPath(),
            file.getTableIdentifier());
        files.add(file);
        paths.add(file.getPath());
      }

      if (!deleteEnabled) {
        output.output(tally(files, Collections.emptySet()));
        return;
      }

      FileIO io = TableCache.get(catalogConfig, batch.getKey().getKey()).io();
      Set<String> failed = deletePaths(io, paths);
      FileCategoryCounts counts = tally(files, failed);
      deletedFiles.inc(counts.total());
      failedFileDeletions.inc(files.size() - counts.total());
      output.output(counts);
    }

    /** Counts {@code files} by category, excluding files whose path is in {@code failed}. */
    private static FileCategoryCounts tally(List<FileInfo> files, Set<String> failed) {
      Map<FileCategory, Long> tally = new EnumMap<>(FileCategory.class);
      for (FileInfo file : files) {
        if (!failed.contains(file.getPath())) {
          tally.merge(file.getCategory(), 1L, Long::sum);
        }
      }
      FileCategoryCounts counts = FileCategoryCounts.zeros();
      for (Map.Entry<FileCategory, Long> entry : tally.entrySet()) {
        counts = counts.plus(FileCategoryCounts.of(entry.getKey(), entry.getValue()));
      }
      return counts;
    }
  }

  /**
   * Deletes {@code paths} with {@code io} and returns the paths that could not be deleted.
   *
   * <p>Uses a single bulk call if {@code io} supports {@link SupportsBulkOperations}. A failed bulk
   * call does not report which files failed, so every file is then retried individually. Retrying
   * files that the bulk call already deleted is safe: {@link FileIO} implementations either succeed
   * silently on a missing file or throw {@link NotFoundException}, which counts as deleted.
   *
   * <p>Only the first {@code MAX_LOGGED_FAILURES} failures are logged individually, followed by a
   * summary.
   *
   * @throws RuntimeException if the thread is interrupted, rather than reporting the remaining
   *     files as failed
   */
  static Set<String> deletePaths(FileIO io, List<String> paths) {
    if (paths.isEmpty()) {
      return Collections.emptySet();
    }

    if (io instanceof SupportsBulkOperations) {
      try {
        ((SupportsBulkOperations) io).deleteFiles(paths);
        return Collections.emptySet();
      } catch (BulkDeletionFailureException e) {
        rethrowIfInterrupted(e);
        LOG.warn(
            "Bulk deletion failed for {} of {} files. Retrying each file individually.",
            e.numberFailedObjects(),
            paths.size());
      } catch (RuntimeException e) {
        rethrowIfInterrupted(e);
        LOG.warn(
            "Bulk deletion of {} files failed. Retrying each file individually.", paths.size(), e);
      }
    }

    Set<String> failed = new HashSet<>();
    for (String path : paths) {
      // Some FileIOs swallow errors, so an interrupt may not surface as an exception.
      if (Thread.currentThread().isInterrupted()) {
        throw new RuntimeException(
            "Interrupted while deleting files individually", new InterruptedException());
      }
      try {
        io.deleteFile(path);
      } catch (NotFoundException e) {
        // Already deleted, for example by an earlier attempt at this batch.
      } catch (RuntimeException e) {
        rethrowIfInterrupted(e);
        if (failed.add(path) && failed.size() <= MAX_LOGGED_FAILURES) {
          LOG.warn("Failed to delete file {}", path, e);
        }
      }
    }
    if (failed.size() > MAX_LOGGED_FAILURES) {
      LOG.warn(
          "Failed to delete {} of {} files; only the first {} failures were logged.",
          failed.size(),
          paths.size(),
          MAX_LOGGED_FAILURES);
    }
    return failed;
  }

  /**
   * Rethrows {@code e} if the current thread was interrupted or {@code e} was caused by an
   * interruption, restoring the thread's interrupt status. Iceberg's {@link FileIO} implementations
   * report interruptions as unchecked exceptions, which must not be mistaken for failed deletions.
   */
  private static void rethrowIfInterrupted(RuntimeException e) {
    boolean interrupted = Thread.currentThread().isInterrupted();
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (@Nullable Throwable cause = e;
        !interrupted && cause != null && seen.add(cause);
        cause = cause.getCause()) {
      interrupted =
          cause instanceof InterruptedException || cause instanceof java.io.InterruptedIOException;
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw e;
    }
  }
}
